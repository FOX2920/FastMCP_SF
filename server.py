import os
from dotenv import load_dotenv
from typing import List, Optional, Any, Dict
from simple_salesforce import Salesforce
from fastmcp import FastMCP

load_dotenv()

mcp = FastMCP(
    name="salesforce-assistant",
    # description="MCP server providing Salesforce query and metadata capabilities for A+ Assistant"
)

def get_salesforce_client():
    """Get authenticated Salesforce client"""
    username = os.getenv("SALESFORCE_USERNAME")
    password = os.getenv("SALESFORCE_PASSWORD")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN")

    if not all([username, password, security_token]):
        raise Exception("Salesforce credentials are not configured in the environment.")
    
    try:
        sf = Salesforce(username=username, password=password, security_token=security_token)
        return sf
    except Exception as e:
        raise Exception(f"Failed to connect to Salesforce: {e}")

@mcp.tool()
def get_next_best_action(account_id: str) -> Dict[str, Any]:
    """
    Get Next Best Action recommendation for a Salesforce account.
    
    Args:
        account_id: The Salesforce Account ID
        
    Returns:
        A dictionary containing the next best action recommendation
    """
    sf = get_salesforce_client()
    
    try:
        # Check for recently lost opportunities
        lost_opp_query = (
            "SELECT Name, CloseDate FROM Opportunity WHERE AccountId = "
            f"'{account_id}' AND StageName = 'Closed Lost' AND CloseDate = LAST_N_DAYS:30 "
            "ORDER BY CloseDate DESC LIMIT 1"
        )
        lost_opp_result = sf.query(lost_opp_query)

        if lost_opp_result.get('totalSize', 0) > 0:
            lost_opp_name = lost_opp_result['records'][0]['Name']
            return {
                "action_needed": True,
                "recommendation_name": "Follow Up on Lost Opportunity & Propose Alternative",
                "description": f"This account recently lost the '{lost_opp_name}' opportunity. Contact them to understand the reasons and propose a pilot for our premium Quartzite products as an alternative.",
                "acceptance_label": "Create Follow-up Task",
                "reason": f"A key opportunity ('{lost_opp_name}') was lost within the last 30 days."
            }

        # Check for at-risk accounts with multiple high-priority cases
        at_risk_case_query = (
            "SELECT count(Id) FROM Case WHERE AccountId = "
            f"'{account_id}' AND IsClosed = false AND Priority = 'High'"
        )
        at_risk_result = sf.query(at_risk_case_query)
        case_count = at_risk_result.get('records', [{}])[0].get('expr0', 0)

        if case_count > 3:
            return {
                "action_needed": True,
                "recommendation_name": "Schedule Proactive Health Check",
                "description": f"This account has {case_count} open, high-priority cases. Schedule a health check call to address their concerns and offer assistance.",
                "acceptance_label": "Create Health Check Task",
                "reason": f"Account has {case_count} open, high-priority support cases, indicating potential churn risk."
            }

        # Default recommendation for stable accounts
        return {
            "action_needed": True,
            "recommendation_name": "Schedule Q4 Business Review",
            "description": "The account is stable. Schedule a strategic meeting to review Q3 performance and plan for Q4 goals.",
            "acceptance_label": "Schedule Review Meeting",
            "reason": "No immediate risks detected. Good time for strategic planning."
        }

    except Exception as e:
        raise Exception(f"Failed to process Next Best Action: {e}")

@mcp.tool()
def list_salesforce_objects() -> List[Dict[str, Any]]:
    """
    Get a list of all available Salesforce objects with their metadata.
    
    Returns:
        A list of all Salesforce objects with their properties
    """
    sf = get_salesforce_client()
    
    try:
        global_describe = sf.describe()
        all_objects = global_describe.get('sobjects', [])
        
        # Return simplified version with key fields
        return [{
            "name": obj.get("name"),
            "label": obj.get("label"),
            "labelPlural": obj.get("labelPlural"),
            "custom": obj.get("custom"),
            "createable": obj.get("createable"),
            "updateable": obj.get("updateable"),
            "deletable": obj.get("deletable"),
            "queryable": obj.get("queryable"),
            "searchable": obj.get("searchable"),
            "retrieveable": obj.get("retrieveable")
        } for obj in all_objects]
        
    except Exception as e:
        raise Exception(f"Failed to describe Salesforce objects: {e}")

@mcp.tool()
def describe_salesforce_object(object_name: str) -> Dict[str, Any]:
    """
    Get detailed metadata description for a specific Salesforce object.

    IMPORTANT: Call this function ONLY ONCE per object type when you need to understand its structure.
    Cache/remember the results for subsequent operations on the same object. DO NOT call this repeatedly
    for the same object - it's expensive and unnecessary.
    
    Use this when you need to, including but not limited to:
    - Find required fields before creating records
    - Discover available fields for querying
    - Understand field data types and validation rules
    - Find relationship names for SOQL joins
    - Check if fields are createable/updateable
    - Get detailed metadata, which is essential for building complex 
    SOQL queries. This helps you find exact field names, child relationship names, line items, and 
    related objects for better analysis and accurate queries.
    
    Args:
        object_name: The API name of the Salesforce object (e.g., 'Account', 'Contact', 'Lead')
        
    Returns:
        Complete metadata description of the Salesforce object including fields, relationships, etc.
    """
    sf = get_salesforce_client()
    
    try:
        sf_object = getattr(sf, object_name)
        description = sf_object.describe()
        return description
    except AttributeError:
        raise Exception(f"Salesforce object '{object_name}' not found.")
    except Exception as e:
        raise Exception(f"Failed to describe Salesforce object '{object_name}': {e}")

@mcp.tool()
def execute_soql_query(query: str) -> Dict[str, Any]:
    """
    Execute a custom SOQL query against Salesforce.
    
    Args:
        query: The SOQL query to execute. Must be a SELECT statement.
        
    Returns:
        Query results including records and metadata
        
    Example:
        # First explore and describe objects
        list_salesforce_objects()  # Find available objects
        describe_salesforce_object('Opportunity')  # Get field names and relationships
        # Then execute your query
        execute_soql_query("SELECT Id, Name, Amount FROM Opportunity WHERE StageName = 'Closed Won'")
    """
    query_string = query.strip()
    
    # Validate query is a SELECT statement for safety
    if not query_string.lower().startswith('select'):
        raise ValueError("Invalid query. Only SELECT statements are allowed.")
    
    sf = get_salesforce_client()
    
    try:
        results = sf.query(query_string)
        return {
            "totalSize": results.get("totalSize", 0),
            "done": results.get("done", True),
            "records": results.get("records", []),
            "nextRecordsUrl": results.get("nextRecordsUrl", None)
        }
    except Exception as e:
        raise Exception(f"Salesforce query failed: {e}")

@mcp.tool()
def create_salesforce_record(object_name: str, record_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new record in Salesforce.
    
    Args:
        object_name: The API name of the Salesforce object (e.g., 'Account', 'Contact', 'Opportunity')
        record_data: Dictionary containing field names and values for the new record
        
    Returns:
        Dictionary with the created record's ID and success status
        
    Example:
        # First describe the object to understand its fields
        describe_salesforce_object('Account')
        # Then create the record
        create_salesforce_record('Account', {'Name': 'Acme Corp', 'Industry': 'Technology'})
        create_salesforce_record('Contact', {'FirstName': 'John', 'LastName': 'Doe', 'Email': 'john@example.com'})
    """
    sf = get_salesforce_client()
    
    if not record_data:
        raise ValueError("record_data cannot be empty")
    
    try:
        sf_object = getattr(sf, object_name)
        result = sf_object.create(record_data)
        
        return {
            "success": result.get("success", False),
            "id": result.get("id"),
            "errors": result.get("errors", []),
            "object_name": object_name,
            "created_fields": list(record_data.keys())
        }
    except AttributeError:
        raise Exception(f"Salesforce object '{object_name}' not found.")
    except Exception as e:
        raise Exception(f"Failed to create {object_name} record: {e}")

@mcp.tool()
def update_salesforce_record(object_name: str, record_id: str, record_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing record in Salesforce.

    Args:
        object_name: The API name of the Salesforce object
        record_id: The Salesforce record ID to update
        record_data: Dictionary containing field names and values to update
        
    Returns:
        Dictionary with success status
        
    Example:
        update_salesforce_record('Account', '001...', {'Phone': '555-1234', 'Industry': 'Finance'})
    """
    sf = get_salesforce_client()
    
    if not record_data:
        raise ValueError("record_data cannot be empty")
    
    try:
        sf_object = getattr(sf, object_name)
        result = sf_object.update(record_id, record_data)
        
        return {
            "success": True if result == 204 else False,
            "id": record_id,
            "object_name": object_name,
            "updated_fields": list(record_data.keys())
        }
    except AttributeError:
        raise Exception(f"Salesforce object '{object_name}' not found.")
    except Exception as e:
        raise Exception(f"Failed to update {object_name} record: {e}")

# @mcp.tool()
# def delete_salesforce_record(object_name: str, record_id: str) -> Dict[str, Any]:
#     """
#     Delete a record from Salesforce.
    
#     Args:
#         object_name: The API name of the Salesforce object
#         record_id: The Salesforce record ID to delete
        
#     Returns:
#         Dictionary with success status
        
#     Example:
#         delete_salesforce_record('Contact', '003...')
#     """
#     sf = get_salesforce_client()
    
#     try:
#         sf_object = getattr(sf, object_name)
#         result = sf_object.delete(record_id)
        
#         return {
#             "success": True if result == 204 else False,
#             "id": record_id,
#             "object_name": object_name,
#             "message": f"Record {record_id} deleted successfully"
#         }
#     except AttributeError:
#         raise Exception(f"Salesforce object '{object_name}' not found.")
#     except Exception as e:
#         raise Exception(f"Failed to delete {object_name} record: {e}")

@mcp.tool()
def get_server_status() -> Dict[str, str]:
    """
    Check the status of the Salesforce MCP server.
    
    Returns:
        Server status information
    """
    return {
        "status": "running",
        "server": "Salesforce Assistant MCP Server",
        "version": "1.0.0"
    }

# Optional: Add resources for commonly used SOQL queries
@mcp.resource("soql://templates/common-queries")
def get_common_soql_templates() -> str:
    """Common SOQL query templates"""
    return """
    Common SOQL Query Templates:
    
    1. Get Account with Contacts:
    SELECT Id, Name, (SELECT Id, FirstName, LastName, Email FROM Contacts) FROM Account WHERE Id = 'ACCOUNT_ID'
    
    2. Get Open Opportunities:
    SELECT Id, Name, Amount, CloseDate, StageName FROM Opportunity WHERE IsClosed = false
    
    3. Get Recent Cases:
    SELECT Id, CaseNumber, Subject, Status, Priority FROM Case WHERE CreatedDate = LAST_N_DAYS:7
    
    4. Get Leads by Status:
    SELECT Id, Name, Company, Status, LeadSource FROM Lead WHERE Status = 'Open'
    
    5. Get Account with Recent Activities:
    SELECT Id, Name, (SELECT Id, Subject, ActivityDate FROM Tasks WHERE ActivityDate = LAST_N_DAYS:30) FROM Account
    """

@mcp.resource("soql://schema/field-types")
def get_salesforce_field_types() -> str:
    """Information about Salesforce field types"""
    return """
    Salesforce Field Types Reference:
    
    - Id: 18-character unique identifier
    - Text: String field (max 255 chars)
    - TextArea: Long text field
    - Picklist: Single selection from list
    - MultiPicklist: Multiple selections from list
    - Number: Numeric field
    - Currency: Money field with currency
    - Percent: Percentage value
    - Date: Date without time
    - DateTime: Date with time
    - Email: Email address field
    - Phone: Phone number field
    - URL: Web address field
    - Checkbox: Boolean true/false
    - Lookup: Reference to another record
    - MasterDetail: Parent-child relationship
    """

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()
