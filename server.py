from fastmcp import FastMCP
import json
import os
from typing import Dict, Any, List, Optional
from simple_salesforce import Salesforce, SalesforceError
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env (nếu có)
load_dotenv()

# Khởi tạo server FastMCP
mcp = FastMCP(name="SalesforceMCP")

# Cơ sở dữ liệu demo trong bộ nhớ
_DB = {
    "1": {
        "id": "1", 
        "title": "Salesforce MCP Overview", 
        "body": "What Salesforce MCP is and why it matters for CRM automation.",
        "type": "documentation"
    },
    "2": {
        "id": "2", 
        "title": "SOQL Query Examples", 
        "body": "Common SOQL queries for Account, Contact, and Opportunity objects.",
        "type": "examples"
    },
    "3": {
        "id": "3", 
        "title": "Custom Object Creation", 
        "body": "How to create custom objects with fields using the Metadata API.",
        "type": "tutorial"
    },
    "4": {
        "id": "4", 
        "title": "Einstein Studio Models", 
        "body": "Creating predictive models using Einstein Studio and AppFrameworkTemplateBundle.",
        "type": "advanced"
    },
    "5": {
        "id": "5", 
        "title": "Lightning App Builder", 
        "body": "Building custom Lightning applications and pages for Salesforce.",
        "type": "tutorial"
    }
}

# --- Các hàm Tool cho MCP ---

@mcp.tool
def greet(name: str) -> str:
    """Return a friendly greeting for Salesforce MCP users."""
    return f"Hello {name}! Welcome to the Salesforce MCP server. I can help you with Salesforce operations, queries, and metadata management."

@mcp.tool
def search(query: str) -> List[Dict[str, Any]]:
    """Search Salesforce MCP documentation and examples by substring in title/body."""
    q = query.lower().strip()
    results = []
    for item in _DB.values():
        if q in item["title"].lower() or q in item["body"].lower():
            results.append({
                "id": item["id"],
                "title": item["title"],
                "type": item["type"],
                "snippet": item["body"][:200] + "..." if len(item["body"]) > 200 else item["body"]
            })
    return results

@mcp.tool
def fetch(id: str) -> Dict[str, Any]:
    """Fetch a specific Salesforce MCP documentation item by ID."""
    if id not in _DB:
        return {"error": f"Document with id {id} not found"}
    return _DB[id]

# --- Hàm Helper cho Kết nối Salesforce ---

def get_salesforce_client() -> Salesforce:
    """
    Tạo và trả về một instance Salesforce client đã được xác thực.
    Đọc credentials an toàn từ biến môi trường.
    """
    SF_USERNAME = os.getenv("SF_USERNAME")
    SF_PASSWORD = os.getenv("SF_PASSWORD")
    SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
    
    if not all([SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN]):
        raise ValueError("Thiếu biến môi trường Salesforce: SF_USERNAME, SF_PASSWORD, hoặc SF_SECURITY_TOKEN. Vui lòng kiểm tra file .env")
        
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN
    )
    return sf

# --- Các hàm Tool tương tác với Salesforce ---

@mcp.tool
def run_soql_query(query: str) -> Dict[str, Any]:
    """Execute a SOQL query against Salesforce and return results."""
    try:
        sf = get_salesforce_client()
        results = sf.query_all(query)
        return {
            "success": True,
            "total_size": results.get("totalSize", 0),
            "done": results.get("done", True),
            "records": results.get("records", [])
        }
    except SalesforceError as e:
        return {"success": False, "error": f"SOQL Error: {e.status} {e.resource_name} {e.content}"}
    except Exception as e:
        return {"success": False, "error": f"Error executing SOQL: {str(e)}"}

@mcp.tool
def run_sosl_search(search_string: str) -> Dict[str, Any]:
    """Execute a SOSL search against Salesforce and return results."""
    try:
        sf = get_salesforce_client()
        results = sf.search(search_string)
        return {
            "success": True,
            "search_records": results.get("searchRecords", [])
        }
    except SalesforceError as e:
        return {"success": False, "error": f"SOSL Error: {e.status} {e.resource_name} {e.content}"}
    except Exception as e:
        return {"success": False, "error": f"Error executing SOSL: {str(e)}"}

@mcp.tool
def describe_object(object_name: str) -> Dict[str, Any]:
    """Get detailed schema information for a Salesforce object."""
    try:
        sf = get_salesforce_client()
        sf_object = getattr(sf, object_name)
        describe = sf_object.describe()
        
        # Trích xuất thông tin chính
        result = {
            "success": True,
            "object_info": {
                "name": describe["name"],
                "label": describe["label"],
                "label_plural": describe.get("labelPlural", ""),
                "key_prefix": describe.get("keyPrefix", ""),
                "custom": describe.get("custom", False),
                "createable": describe.get("createable", False),
                "updateable": describe.get("updateable", False),
                "deletable": describe.get("deletable", False)
            },
            "fields": []
        }
        
        # Thêm thông tin trường
        for field in describe.get("fields", []):
            field_info = {
                "name": field["name"],
                "label": field["label"],
                "type": field["type"],
                "required": not field.get("nillable", True),
                "unique": field.get("unique", False),
                "external_id": field.get("externalId", False),
                "updateable": field.get("updateable", False),
                "createable": field.get("createable", False)
            }
            result["fields"].append(field_info)
        
        return result
        
    except AttributeError:
        return {"success": False, "error": f"Object '{object_name}' not found or not accessible"}
    except Exception as e:
        return {"success": False, "error": f"Error describing object {object_name}: {str(e)}"}

@mcp.tool
def create_record(object_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new record in Salesforce."""
    try:
        sf = get_salesforce_client()
        sf_object = getattr(sf, object_name)
        result = sf_object.create(data)
        
        return {
            "success": result.get("success", False),
            "id": result.get("id"),
            "errors": result.get("errors", [])
        }
    except AttributeError:
        return {"success": False, "error": f"Object type '{object_name}' not found or accessible via API"}
    except SalesforceError as e:
        return {"success": False, "error": f"Create Record Error: {e.status} {e.resource_name} {e.content}"}
    except Exception as e:
        return {"success": False, "error": f"Error creating {object_name} record: {str(e)}"}

@mcp.tool
def update_record(object_name: str, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing record in Salesforce."""
    try:
        sf = get_salesforce_client()
        sf_object = getattr(sf, object_name)
        status_code = sf_object.update(record_id, data)
        
        success = 200 <= status_code < 300
        return {
            "success": success,
            "status_code": status_code,
            "message": f"Update {object_name} record {record_id}: {'Success' if success else 'Failed'}"
        }
    except AttributeError:
        return {"success": False, "error": f"Object type '{object_name}' not found or accessible via API"}
    except SalesforceError as e:
        return {"success": False, "error": f"Update Record Error: {e.status} {e.resource_name} {e.content}"}
    except Exception as e:
        return {"success": False, "error": f"Error updating {object_name} record {record_id}: {str(e)}"}

@mcp.tool
def delete_record(object_name: str, record_id: str) -> Dict[str, Any]:
    """Delete a record from Salesforce."""
    try:
        sf = get_salesforce_client()
        sf_object = getattr(sf, object_name)
        status_code = sf_object.delete(record_id)
        
        success = 200 <= status_code < 300
        return {
            "success": success,
            "status_code": status_code,
            "message": f"Delete {object_name} record {record_id}: {'Success' if success else 'Failed'}"
        }
    except AttributeError:
        return {"success": False, "error": f"Object type '{object_name}' not found or accessible via API"}
    except SalesforceError as e:
        return {"success": False, "error": f"Delete Record Error: {e.status} {e.resource_name} {e.content}"}
    except Exception as e:
        return {"success": False, "error": f"Error deleting {object_name} record {record_id}: {str(e)}"}

# --- Chạy Server ---

if __name__ == "__main__":
    print("Starting Salesforce MCP server on http://localhost:8000 ...")
    mcp.run(transport="http", port=8000)
