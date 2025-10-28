from fastmcp import FastMCP
import os
from typing import Dict, Any
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import warnings
import numpy as np

# Tải các biến môi trường từ file .env
load_dotenv()
warnings.filterwarnings('ignore')

# Khởi tạo server FastMCP
mcp = FastMCP(name="salesforce-contract-assistant")

# --- Hàm Helper cho Kết nối Salesforce ---

def get_salesforce_client() -> Salesforce:
    """
    Tạo và trả về một instance Salesforce client đã được xác thực.
    Đọc credentials an toàn từ biến môi trường (SALESFORCE_USERNAME, v.v.)
    """
    SF_USERNAME = os.getenv("SALESFORCE_USERNAME")
    SF_PASSWORD = os.getenv("SALESFORCE_PASSWORD")
    SF_SECURITY_TOKEN = os.getenv("SALESFORCE_SECURITY_TOKEN")

    if not all([SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN]):
        raise ValueError(
            "Thiếu biến môi trường Salesforce: SALESFORCE_USERNAME, "
            "SALESFORCE_PASSWORD, hoặc SALESFORCE_SECURITY_TOKEN. "
            "Vui lòng kiểm tra file .env"
        )
        
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN
    )
    return sf

# --- Hàm Helper: Logic xử lý dữ liệu ---

def _get_processed_data() -> pd.DataFrame:
    """
    Hàm helper nội bộ để lấy và xử lý dữ liệu từ Salesforce.
    Kết hợp logic từ fetch_data() và transform_data().
    """
    
    # 1. Kết nối và Lấy dữ liệu
    sf = get_salesforce_client()
    soql = """
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
    
    query_result = sf.query_all(soql)
    records = query_result.get('records', [])
    
    if not records:
        return pd.DataFrame()  # Trả về DataFrame rỗng nếu không có dữ liệu
    
    # Normalize JSON data
    df = pd.json_normalize(records, sep='.')
    
    # Drop attributes columns if they exist
    attribute_cols = [col for col in df.columns if 'attributes' in col]
    if attribute_cols:
        df = df.drop(attribute_cols, axis=1)
    
    # Rename columns
    df.columns = (df.columns
                  .str.replace('Contract__r.', 'Contract_', regex=False)
                  .str.replace('Product__r.', 'Product_', regex=False)
                  .str.replace('Account__r.', 'Account_', regex=False))

    # 2. Chuyển đổi dữ liệu
    df_export = pd.DataFrame()
    
    df_export['Account Name: Account Code'] = df.get('Contract_Account_Account_Code__c')
    df_export['Product: STONE Color Type'] = df.get('Product_STONE_Color_Type__c')
    df_export['Product: Product SKU'] = df.get('Product_StockKeepingUnit')
    df_export['Contract Product Name'] = df.get('Name')
    
    # Convert and extract year
    df['Created_Date'] = pd.to_datetime(
        df.get('Contract_Created_Date__c'), 
        errors='coerce'
    )
    df_export['YEAR'] = df['Created_Date'].dt.year
    
    df_export['Product Discription'] = df.get('Product_Discription__c')
    df_export['Product: Mô tả sản phẩm'] = df.get('Product_Discription__c')
    
    # Convert numeric columns
    numeric_cols = {
        'Length': 'Length__c',
        'Width': 'Width__c',
        'Height': 'Height__c',
        'Quantity': 'Quantity__c',
        'Crates': 'Crates__c',
        'm2': 'm2__c',
        'm3': 'm3__c',
        'Tons': 'Tons__c',
        'Cont': 'Cont__c',
        'Sales Price': 'Sales_Price__c',
        'Total Price (USD)': 'Total_Price_USD__c'
    }
    
    for export_col, source_col in numeric_cols.items():
        df_export[export_col] = pd.to_numeric(df.get(source_col), errors='coerce')
    
    # Special handling for Quantity (convert to int)
    df_export['Quantity'] = df_export['Quantity'].fillna(0).astype(int)
    
    df_export['Charge Unit (PI)'] = df.get('Charge_Unit_PI__c')
    df_export['Product: Product Family'] = df.get('Product_Family')
    df_export['Segment'] = df.get('Segment__c')
    df_export['Contract Name'] = df.get('Contract_Name')
    df_export['Created Date (C)'] = df['Created_Date'].dt.strftime('%d/%m/%Y')
    
    # 3. Lọc và Sắp xếp
    current_year = datetime.now().year
    df_export = df_export[
        (df_export['YEAR'] >= 2015) & 
        (df_export['YEAR'] <= current_year)
    ]
    df_export = df_export.dropna(subset=['Account Name: Account Code'])
    
    df_export = df_export.sort_values(
        by=['Account Name: Account Code', 'YEAR'],
        ascending=[True, False]
    )
    
    # Reset index
    df_export = df_export.reset_index(drop=True)
    
    return df_export

# --- CÁC TOOL MCP ---

@mcp.tool()
def get_all_contract_product_details() -> Dict[str, Any]:
    """
    Lấy toàn bộ dữ liệu chi tiết sản phẩm hợp đồng đã được xử lý.
    Fetch, transform, and return all contract product details as JSON.
    
    Returns:
        Dict with 'success', 'count', and 'data' keys
    """
    try:
        df = _get_processed_data()
        
        if df.empty:
            return {
                "success": True, 
                "count": 0, 
                "data": [],
                "message": "No contract products found"
            }
        
        # Thay thế NaN bằng None để tương thích JSON
        df_json = df.replace({np.nan: None})
        records = df_json.to_dict(orient='records')
        
        return {
            "success": True, 
            "count": len(records), 
            "data": records
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
    try:
        # Validate input
        if not account_code or not account_code.strip():
            return {
                "success": False, 
                "error": "Account code cannot be empty"
            }
        
        df = _get_processed_data()
        
        if df.empty:
            return {
                "success": True, 
                "count": 0, 
                "data": [], 
                "message": f"No data found (empty source) for account code {account_code}"
            }
        
        # Lọc DataFrame (so sánh không phân biệt chữ hoa/thường và khoảng trắng)
        account_code_clean = account_code.strip().lower()
        filtered_df = df[
            df['Account Name: Account Code'].astype(str).str.strip().str.lower() == account_code_clean
        ]
        
        if filtered_df.empty:
            return {
                "success": True, 
                "count": 0, 
                "data": [], 
                "message": f"No data found for account code '{account_code}'"
            }
            
        # Thay thế NaN bằng None để tương thích JSON
        df_json = filtered_df.replace({np.nan: None})
        records = df_json.to_dict(orient='records')
        
        return {
            "success": True, 
            "count": len(records), 
            "data": records,
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
