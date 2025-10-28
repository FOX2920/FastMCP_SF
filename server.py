import os
from dotenv import load_dotenv
from typing import List, Optional, Any, Dict
from simple_salesforce import Salesforce
from fastmcp import FastMCP

load_dotenv()

mcp = FastMCP(
    name="salesforce-assistant",
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
def get_next_best_action(account_name: str) -> Dict[str, Any]:
    """
    Use this ONLY when user explicitly asks for "next best action" analysis.
    Get comprehensive account data for Next Best Action recommendations.
    Returns account info, recent opportunities, orders, and activities.
    
    Args:
        account_name: Exact Salesforce Account Name
    """
    sf = get_salesforce_client()
    
    try:
        query = f"""
        SELECT
            Id,
            Account_Code__c,
            Name,
            RecordTypeId,
            Customer_Status__c,
            Nhom_Khu_vuc_KH__c,
            Received_Conts__c,
            quarter_target__c,
            Muc_tieu_nam__c,
            Nh_m_Kh_ch_h_ng__c,
            (SELECT Name, StageName, Amount, CloseDate 
             FROM Opportunities 
             ORDER BY CloseDate DESC 
             LIMIT 5),
            (SELECT OrderNumber, PI_No__c, EffectiveDate, Expected_ETD__c, Order_Processing__c 
             FROM Orders 
             ORDER BY EffectiveDate DESC NULLS LAST 
             LIMIT 5),
            (SELECT ActivityDate, Subject, Description 
             FROM ActivityHistories 
             ORDER BY ActivityDate DESC 
             LIMIT 5)
        FROM
            Account
        WHERE
            Name = '{account_name}'
        """
        
        result = sf.query(query)
        
        if result.get('totalSize', 0) == 0:
            raise Exception(f"No account found with name: {account_name}")
        
        account_data = result['records'][0]
        
        return {
            "account_info": {
                "id": account_data.get('Id'),
                "account_code": account_data.get('Account_Code__c'),
                "name": account_data.get('Name'),
                "record_type_id": account_data.get('RecordTypeId'),
                "customer_status": account_data.get('Customer_Status__c'),
                "region_group": account_data.get('Nhom_Khu_vuc_KH__c'),
                "received_conts": account_data.get('Received_Conts__c'),
                "quarter_target": account_data.get('quarter_target__c'),
                "annual_target": account_data.get('Muc_tieu_nam__c'),
                "customer_group": account_data.get('Nh_m_Kh_ch_h_ng__c')
            },
            "recent_opportunities": [
                {
                    "name": opp.get('Name'),
                    "stage": opp.get('StageName'),
                    "amount": opp.get('Amount'),
                    "close_date": opp.get('CloseDate')
                }
                for opp in account_data.get('Opportunities', {}).get('records', [])
            ],
            "recent_orders": [
                {
                    "order_number": order.get('OrderNumber'),
                    "pi_no": order.get('PI_No__c'),
                    "effective_date": order.get('EffectiveDate'),
                    "expected_etd": order.get('Expected_ETD__c'),
                    "order_processing": order.get('Order_Processing__c')
                }
                for order in account_data.get('Orders', {}).get('records', [])
            ],
            "recent_activities": [
                {
                    "date": activity.get('ActivityDate'),
                    "subject": activity.get('Subject'),
                    "description": activity.get('Description')
                }
                for activity in account_data.get('ActivityHistories', {}).get('records', [])
            ]
        }
        
    except Exception as e:
        raise Exception(f"Failed to retrieve account data for Next Best Action: {e}")

@mcp.tool()
def list_salesforce_objects() -> List[Dict[str, Any]]:
    """
    List all available Salesforce objects with their metadata.
    Use when user wants to explore available objects.
    """
    sf = get_salesforce_client()
    
    try:
        global_describe = sf.describe()
        all_objects = global_describe.get('sobjects', [])
        
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
    Get detailed field metadata for a Salesforce object.
    Call ONCE per object when you need field names, types, or relationships.
    Required before creating/updating records to know field requirements.
    
    Args:
        object_name: API name (e.g., 'Account', 'Contact', 'Lead')
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
    Execute SOQL SELECT query to retrieve Salesforce data.
    Use for custom data queries, reports, and aggregations.
    
    Args:
        query: SOQL SELECT statement
        
    Example:
        "SELECT Id, Name FROM Account WHERE Industry = 'Technology'"
    """
    query_string = query.strip()
    
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
    Create new Salesforce record.
    MUST call describe_salesforce_object first to identify required fields.
    
    Args:
        object_name: API name (e.g., 'Account', 'Contact')
        record_data: Field names and values (must include required fields)
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
    Update existing Salesforce record.
    
    Args:
        object_name: API name (e.g., 'Account')
        record_id: 18-character Salesforce ID
        record_data: Fields to update with new values
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

@mcp.tool()
def get_server_status() -> Dict[str, str]:
    """Check Salesforce MCP server status."""
    return {
        "status": "running",
        "server": "Salesforce Assistant MCP Server",
        "version": "1.0.0"
    }

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
    mcp.run()
