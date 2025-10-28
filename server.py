from fastmcp import FastMCP
import os
from typing import Dict, Any, List
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
mcp = FastMCP(name="SalesforceContractMCP")

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
        raise ValueError("Thiếu biến môi trường Salesforce: SALESFORCE_USERNAME, SALESFORCE_PASSWORD, hoặc SALESFORCE_SECURITY_TOKEN. Vui lòng kiểm tra file .env")
        
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN
    )
    return sf

# --- Hàm Helper: Logic xử lý dữ liệu (từ code FastAPI của bạn) ---

def _get_processed_data() -> pd.DataFrame:
    """
    Hàm helper nội bộ để lấy và xử lý dữ liệu từ Salesforce.
    Kết hợp logic từ fetch_data() và transform_data() từ code FastAPI.
    """
    
    # 1. Kết nối và Lấy dữ liệu (từ fetch_data)
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
    records = query_result['records']
    
    if not records:
        return pd.DataFrame() # Trả về DataFrame rỗng nếu không có dữ liệu
    
    df = pd.json_normalize(records, sep='.')
    df = df.drop([col for col in df.columns if 'attributes' in col], axis=1)
    
    df.columns = df.columns.str.replace('Contract__r.', 'Contract_', regex=False)
    df.columns = df.columns.str.replace('Product__r.', 'Product_', regex=False)
    df.columns = df.columns.str.replace('Account__r.', 'Account_', regex=False)

    # 2. Chuyển đổi dữ liệu (từ transform_data)
    df_export = pd.DataFrame()
    
    df_export['Account Name: Account Code'] = df.get('Contract_Account_Account_Code__c')
    df_export['Product: STONE Color Type'] = df.get('Product_STONE_Color_Type__c')
    df_export['Product: Product SKU'] = df.get('Product_StockKeepingUnit')
    df_export['Contract Product Name'] = df.get('Name')
    
    df['Created_Date'] = pd.to_datetime(df.get('Contract_Created_Date__c'), errors='coerce')
    df_export['YEAR'] = df['Created_Date'].dt.year
    
    df_export['Product Discription'] = df.get('Product_Discription__c')
    df_export['Product: Mô tả sản phẩm'] = df.get('Product_Discription__c')
    
    df_export['Length'] = pd.to_numeric(df.get('Length__c'), errors='coerce')
    df_export['Width'] = pd.to_numeric(df.get('Width__c'), errors='coerce')
    df_export['Height'] = pd.to_numeric(df.get('Height__c'), errors='coerce')
    df_export['Quantity'] = pd.to_numeric(df.get('Quantity__c'), errors='coerce').fillna(0).astype(int)
    df_export['Crates'] = pd.to_numeric(df.get('Crates__c'), errors='coerce')
    df_export['m2'] = pd.to_numeric(df.get('m2__c'), errors='coerce')
    df_export['m3'] = pd.to_numeric(df.get('m3__c'), errors='coerce')
    df_export['Tons'] = pd.to_numeric(df.get('Tons__c'), errors='coerce')
    df_export['Cont'] = pd.to_numeric(df.get('Cont__c'), errors='coerce')
    df_export['Sales Price'] = pd.to_numeric(df.get('Sales_Price__c'), errors='coerce')
    
    df_export['Charge Unit (PI)'] = df.get('Charge_Unit_PI__c')
    df_export['Total Price (USD)'] = pd.to_numeric(df.get('Total_Price_USD__c'), errors='coerce')
    
    df_export['Product: Product Family'] = df.get('Product_Family')
    df_export['Segment'] = df.get('Segment__c')
    df_export['Contract Name'] = df.get('Contract_Name')
    df_export['Created Date (C)'] = df['Created_Date'].dt.strftime('%d/%m/%Y')
    
    # 3. Lọc và Sắp xếp
    df_export = df_export[
        (df_export['YEAR'] >= 2015) & 
        (df_export['YEAR'] <= datetime.now().year)
    ]
    df_export = df_export.dropna(subset=['Account Name: Account Code'])
    
    df_export = df_export.sort_values(
        by=['Account Name: Account Code', 'YEAR'],
        ascending=[True, False]
    )
    
    return df_export

# --- CÁC TOOL MCP DUY NHẤT THEO YÊU CẦU ---

@mcp.tool
def get_all_contract_product_details() -> Dict[str, Any]:
    """
    Tool 1: Lấy toàn bộ dữ liệu chi tiết sản phẩm hợp đồng đã được xử lý.
    Fetch, transform, and return all contract product details as JSON.
    """
    try:
        df = _get_processed_data()
        
        if df.empty:
            return {"success": True, "count": 0, "data": []}
        
        # Thay thế NaN bằng None để tương thích JSON
        df_json = df.replace({np.nan: None})
        records = df_json.to_dict(orient='records')
        
        return {"success": True, "count": len(records), "data": records}
        
    except Exception as e:
        return {"success": False, "error": f"Error processing data: {str(e)}"}

@mcp.tool
def get_contract_details_by_account(account_code: str) -> Dict[str, Any]:
    """
    Tool 2: Lấy dữ liệu chi tiết sản phẩm hợp đồng đã xử lý, lọc theo Account Code.
    Fetch processed contract product details, filtered by a specific Account Code.
    """
    try:
        df = _get_processed_data()
        
        if df.empty:
            return {"success": True, "count": 0, "data": [], "message": f"No data found (empty source) for account code {account_code}"}
        
        # Lọc DataFrame (so sánh không phân biệt chữ hoa/thường và khoảng trắng)
        filtered_df = df[df['Account Name: Account Code'].str.strip().lower() == account_code.strip().lower()]
        
        if filtered_df.empty:
            return {"success": True, "count": 0, "data": [], "message": f"No data found for account code {account_code}"}
            
        # Thay thế NaN bằng None để tương thích JSON
        df_json = filtered_df.replace({np.nan: None})
        records = df_json.to_dict(orient='records')
        
        return {"success": True, "count": len(records), "data": records}
        
    except Exception as e:
        return {"success": False, "error": f"Error processing data for account {account_code}: {str(e)}"}


# --- Chạy Server ---

if __name__ == "__main__":
    print("Starting Salesforce Contract MCP server on http://localhost:8000 ...")
    mcp.run(transport="http", port=8000)
