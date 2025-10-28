from fastmcp import FastMCP
import os
from typing import Dict, Any, List, Optional
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from datetime import datetime

# Tải các biến môi trường từ file .env
load_dotenv()

# Khởi tạo server FastMCP
mcp = FastMCP(name="salesforce-contract-assistant")

# --- Hàm Helper cho Kết nối Salesforce ---

def get_salesforce_client() -> Salesforce:
    """
    Tạo và trả về một instance Salesforce client đã được xác thực.
    Đọc credentials an toàn từ biến môi trường (SALESFORCE_USERNAME, v.v.)
    """
    SALESFORCE_USERNAME = os.getenv("SALESFORCE_USERNAME")
    SALESFORCE_PASSWORD = os.getenv("SALESFORCE_PASSWORD")
    SALESFORCE_SECURITY_TOKEN = os.getenv("SALESFORCE_SECURITY_TOKEN")

    if not all([SALESFORCE_USERNAME, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN]):
        raise ValueError(
            "Thiếu biến môi trường Salesforce: SALESFORCE_USERNAME, "
            "SALESFORCE_PASSWORD, hoặc SALESFORCE_SECURITY_TOKEN. "
            "Vui lòng kiểm tra file .env"
        )
    
    try:
        sf = Salesforce(
            username=SALESFORCE_USERNAME,
            password=SALESFORCE_PASSWORD,
            security_token=SALESFORCE_SECURITY_TOKEN
        )
        return sf
    except Exception as e:
        raise Exception(f"Failed to connect to Salesforce: {e}")

# --- Hàm Helper: Logic xử lý dữ liệu ---

def _parse_date(date_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Parse date string và trả về dict với year và formatted date.
    Returns None nếu date_str là None hoặc không parse được.
    """
    if not date_str:
        return None
    
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return {
            'year': dt.year,
            'formatted': dt.strftime('%d/%m/%Y')
        }
    except:
        return None

def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float, return None if not possible"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def _safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int, return None if not possible"""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def _process_contract_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Xử lý một record từ Salesforce và chuyển đổi sang format output.
    """
    # Parse date
    created_date_str = record.get('Contract__r', {}).get('Created_Date__c')
    date_info = _parse_date(created_date_str)
    
    # Extract nested fields
    account_code = record.get('Contract__r', {}).get('Account__r', {}).get('Account_Code__c')
    stone_color = record.get('Product__r', {}).get('STONE_Color_Type__c')
    product_sku = record.get('Product__r', {}).get('StockKeepingUnit')
    product_family = record.get('Product__r', {}).get('Family')
    contract_name = record.get('Contract__r', {}).get('Name')
    
    # Build processed record
    processed = {
        'Account Name: Account Code': account_code,
        'Product: STONE Color Type': stone_color,
        'Product: Product SKU': product_sku,
        'Contract Product Name': record.get('Name'),
        'YEAR': date_info['year'] if date_info else None,
        'Product Discription': record.get('Product_Discription__c'),
        'Product: Mô tả sản phẩm': record.get('Product_Discription__c'),
        'Length': _safe_float(record.get('Length__c')),
        'Width': _safe_float(record.get('Width__c')),
        'Height': _safe_float(record.get('Height__c')),
        'Quantity': _safe_int(record.get('Quantity__c')),
        'Crates': _safe_float(record.get('Crates__c')),
        'm2': _safe_float(record.get('m2__c')),
        'm3': _safe_float(record.get('m3__c')),
        'Tons': _safe_float(record.get('Tons__c')),
        'Cont': _safe_float(record.get('Cont__c')),
        'Sales Price': _safe_float(record.get('Sales_Price__c')),
        'Charge Unit (PI)': record.get('Charge_Unit_PI__c'),
        'Total Price (USD)': _safe_float(record.get('Total_Price_USD__c')),
        'Product: Product Family': product_family,
        'Segment': record.get('Segment__c'),
        'Contract Name': contract_name,
        'Created Date (C)': date_info['formatted'] if date_info else None
    }
    
    return processed

def _filter_records(records: List[Dict[str, Any]], account_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Lọc records theo các tiêu chí:
    - Year phải từ 2015 đến năm hiện tại
    - Account code không được None
    - Nếu account_code được cung cấp, lọc theo account code đó
    """
    current_year = datetime.now().year
    filtered = []
    
    for record in records:
        year = record.get('YEAR')
        acc_code = record.get('Account Name: Account Code')
        
        # Skip if no account code
        if not acc_code:
            continue
        
        # Skip if year is invalid
        if year is None or year < 2015 or year > current_year:
            continue
        
        # Filter by account code if provided
        if account_code:
            if acc_code.strip().lower() != account_code.strip().lower():
                continue
        
        filtered.append(record)
    
    return filtered

def _sort_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sắp xếp records theo Account Code (tăng dần) và Year (giảm dần)
    """
    return sorted(
        records,
        key=lambda x: (
            x.get('Account Name: Account Code', '').lower(),
            -(x.get('YEAR') or 0)
        )
    )

# --- CÁC TOOL MCP ---

@mcp.tool()
def get_all_contract_product_details() -> Dict[str, Any]:
    """
    Lấy toàn bộ dữ liệu chi tiết sản phẩm hợp đồng đã được xử lý.
    Fetch, transform, and return all contract product details as JSON.
    
    Returns:
        Dict with 'success', 'count', and 'data' keys
    """
    sf = get_salesforce_client()
    
    try:
        # Query dữ liệu từ Salesforce
        query = """
            SELECT Name, 
                Contract__r.Account__r.Account_Code__c, 
                Product__r.STONE_Color_Type__c,
                Product__r.StockKeepingUnit,
                Product__r.Family,
                Segment__c,
                Contract__r.Created_Date__c,
                Contract__r.Name,
                Product_Discription__c,
                Length__c,
                Width__c,
                Height__c,
                Quantity__c,
                Crates__c,
                m2__c,
                m3__c,
                Tons__c, 
                Cont__c,
                Sales_Price__c,
                Charge_Unit_PI__c,
                Total_Price_USD__c
            FROM Contract_Product__c 
            ORDER BY Contract__r.Created_Date__c DESC
        """
        
        result = sf.query_all(query)
        records = result.get('records', [])
        
        if not records:
            return {
                "success": True, 
                "count": 0, 
                "data": [],
                "message": "No contract products found"
            }
        
        # Xử lý từng record
        processed_records = [_process_contract_record(record) for record in records]
        
        # Lọc và sắp xếp
        filtered_records = _filter_records(processed_records)
        sorted_records = _sort_records(filtered_records)
        
        return {
            "success": True, 
            "count": len(sorted_records), 
            "data": sorted_records
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error processing data: {str(e)}"
        }

@mcp.tool()
def get_contract_details_by_account(account_code: str) -> Dict[str, Any]:
    """
    Lấy dữ liệu chi tiết sản phẩm hợp đồng đã xử lý, lọc theo Account Code.
    Fetch processed contract product details, filtered by a specific Account Code.
    
    Args:
        account_code: The account code to filter by (case-insensitive)
        
    Returns:
        Dict with 'success', 'count', 'data' keys, and optional 'message'
    """
    # Validate input
    if not account_code or not account_code.strip():
        return {
            "success": False, 
            "error": "Account code cannot be empty"
        }
    
    sf = get_salesforce_client()
    
    try:
        # Query dữ liệu từ Salesforce với filter trong SOQL để tối ưu
        # Note: So sánh exact match trong SOQL, case-insensitive xử lý ở Python
        query = f"""
            SELECT Name, 
                Contract__r.Account__r.Account_Code__c, 
                Product__r.STONE_Color_Type__c,
                Product__r.StockKeepingUnit,
                Product__r.Family,
                Segment__c,
                Contract__r.Created_Date__c,
                Contract__r.Name,
                Product_Discription__c,
                Length__c,
                Width__c,
                Height__c,
                Quantity__c,
                Crates__c,
                m2__c,
                m3__c,
                Tons__c, 
                Cont__c,
                Sales_Price__c,
                Charge_Unit_PI__c,
                Total_Price_USD__c
            FROM Contract_Product__c 
            WHERE Contract__r.Account__r.Account_Code__c != null
            ORDER BY Contract__r.Created_Date__c DESC
        """
        
        result = sf.query_all(query)
        records = result.get('records', [])
        
        if not records:
            return {
                "success": True, 
                "count": 0, 
                "data": [], 
                "message": f"No data found (empty source) for account code {account_code}"
            }
        
        # Xử lý từng record
        processed_records = [_process_contract_record(record) for record in records]
        
        # Lọc theo account code (case-insensitive)
        filtered_records = _filter_records(processed_records, account_code=account_code)
        
        if not filtered_records:
            return {
                "success": True, 
                "count": 0, 
                "data": [], 
                "message": f"No data found for account code '{account_code}'"
            }
        
        # Sắp xếp
        sorted_records = _sort_records(filtered_records)
        
        return {
            "success": True, 
            "count": len(sorted_records), 
            "data": sorted_records,
            "account_code": account_code
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error processing data for account {account_code}: {str(e)}"
        }

@mcp.tool()
def get_server_status() -> Dict[str, str]:
    """Check Salesforce Contract MCP server status."""
    return {
        "status": "running",
        "server": "Salesforce Contract Assistant MCP Server",
        "version": "1.0.0"
    }

# --- Resources cho documentation ---

@mcp.resource("contract://info/data-fields")
def get_contract_data_fields() -> str:
    """Information about available contract product fields"""
    return """
    Contract Product Data Fields:
    
    Account Information:
    - Account Name: Account Code
    
    Product Information:
    - Product: STONE Color Type
    - Product: Product SKU
    - Product: Product Family
    - Product: Mô tả sản phẩm
    - Product Discription
    
    Contract Information:
    - Contract Product Name
    - Contract Name
    - Created Date (C)
    - YEAR (2015 - current year)
    - Segment
    
    Measurements:
    - Length (mm)
    - Width (mm)
    - Height (mm)
    - Quantity (pieces)
    - Crates
    - m2 (square meters)
    - m3 (cubic meters)
    - Tons
    - Cont (containers)
    
    Pricing:
    - Sales Price (per unit)
    - Charge Unit (PI)
    - Total Price (USD)
    """


# --- Chạy Server ---

if __name__ == "__main__":
    # ✅ SỬA: Bỏ transport="http" và port=8000
    # FastMCP cần chạy với stdio transport (mặc định) để deploy
    mcp.run()
