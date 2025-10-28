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
        return pd.DataFrame() # Trả về DataFrame rỗng nếu không có dữ liệu
    
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
    
    # === CẢI TIẾN: Điền 0 cho các cột số để tính tổng hợp (aggregation) an toàn ===
    # Lấy danh sách các cột số vừa tạo
    numeric_export_cols = list(numeric_cols.keys())
    for col in numeric_export_cols:
        if col in df_export.columns:
            # Fill NaNs with 0 for reliable aggregations
            df_export[col] = df_export[col].fillna(0)
    # =========================================================================

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

# --- CÁC TOOL MCP HIỆN CÓ ---

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

# --- CÁC TOOL MỚI CHO VISUALIZATION & INSIGHT ---

@mcp.tool()
def get_sales_summary_by_year() -> Dict[str, Any]:
    """
    Tổng hợp tổng doanh thu (USD) và tổng số lượng bán ra, nhóm theo năm.
    Rất hữu ích để tạo biểu đồ đường (line chart) thể hiện xu hướng bán hàng tổng quan theo thời gian.
    
    Returns:
        Dict with 'success' and 'data' (list of {year, total_sales_usd, total_quantity}).
    """
    try:
        df = _get_processed_data()
        if df.empty:
            return {"success": True, "count": 0, "data": [], "message": "No data to summarize."}
        
        # Group by YEAR and aggregate sum for Sales and Quantity
        summary_df = df.groupby('YEAR').agg(
            total_sales_usd=('Total Price (USD)', 'sum'),
            total_quantity=('Quantity', 'sum')
        ).reset_index()
        
        # Sắp xếp theo năm
        summary_df = summary_df.sort_values(by='YEAR', ascending=True)
        
        records = summary_df.to_dict(orient='records')
        
        return {
            "success": True,
            "count": len(records),
            "data": records
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error summarizing sales by year: {str(e)}"
        }

@mcp.tool()
def get_top_items_summary(group_by_field: str) -> Dict[str, Any]:
    """
    Lấy tất cả các mặt hàng, nhóm theo một trường cụ thể,
    sắp xếp theo tổng doanh thu (USD) giảm dần.
    Rất hữu ích để tạo biểu đồ cột (bar chart) cho "Top Khách hàng" hoặc "Top Dòng sản phẩm".

    Valid group_by_field values:
    - 'Account Name: Account Code' (cho top khách hàng)
    - 'Product: Product Family' (cho top dòng sản phẩm)
    - 'Product: Product SKU' (cho top sản phẩm SKU)
    - 'Product: STONE Color Type' (cho top loại màu)
    
    Args:
        group_by_field: Tên cột chính xác để nhóm (xem danh sách hợp lệ ở trên).
        
    Returns:
        Dict with 'success' and 'data' (list of {group_by_field, total_sales_usd, total_quantity}).
    """
    valid_fields = [
        'Account Name: Account Code',
        'Product: Product Family',
        'Product: Product SKU',
        'Product: STONE Color Type'
    ]
    
    if group_by_field not in valid_fields:
        return {
            "success": False,
            "error": f"Invalid group_by_field. Must be one of: {', '.join(valid_fields)}"
        }
        
    try:
        df = _get_processed_data()
        if df.empty:
            return {"success": True, "count": 0, "data": [], "message": "No data to summarize."}
        
        summary_df = df.groupby(group_by_field).agg(
            total_sales_usd=('Total Price (USD)', 'sum'),
            total_quantity=('Quantity', 'sum')
        ).reset_index()
        
        # Sắp xếp theo total_sales_usd giảm dần
        summary_df = summary_df.sort_values(by='total_sales_usd', ascending=False)
        
        records = summary_df.to_dict(orient='records')
        
        return {
            "success": True,
            "count": len(records),
            "group_by_field": group_by_field,
            "data": records
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error getting top items for {group_by_field}: {str(e)}"
        }

@mcp.tool()
def get_sales_trend_for_item(field_name: str, field_value: str) -> Dict[str, Any]:
    """
    Lấy xu hướng bán hàng hàng năm (doanh thu, số lượng, giá bán trung bình) cho một mặt hàng cụ thể.
    Dùng để phân tích sâu hoặc "dự đoán" cho một sản phẩm, khách hàng hoặc phân khúc cụ thể.

    Valid field_name values:
    - 'Account Name: Account Code'
    - 'Product: Product Family'
    - 'Product: Product SKU'
    
    Args:
        field_name: Tên cột để lọc.
        field_value: Giá trị cụ thể để lọc (không phân biệt chữ hoa/thường).
        
    Returns:
        Dict with 'success' and 'data' (list of {YEAR, total_sales_usd, total_quantity, avg_sales_price}).
    """
    valid_fields = [
        'Account Name: Account Code',
        'Product: Product Family',
        'Product: Product SKU'
    ]
    
    if field_name not in valid_fields:
        return {
            "success": False,
            "error": f"Invalid field_name. Must be one of: {', '.join(valid_fields)}"
        }
        
    try:
        df = _get_processed_data()
        if df.empty:
            return {"success": True, "count": 0, "data": [], "message": "No data to analyze."}

        # Filter for the specific item (case-insensitive)
        field_value_clean = field_value.strip().lower()
        filtered_df = df[
            df[field_name].astype(str).str.strip().str.lower() == field_value_clean
        ]
        
        if filtered_df.empty:
            return {
                "success": True, 
                "count": 0, 
                "data": [], 
                "message": f"No data found for {field_name} = '{field_value}'"
            }

        # Group by YEAR and aggregate
        summary_df = filtered_df.groupby('YEAR').agg(
            total_sales_usd=('Total Price (USD)', 'sum'),
            total_quantity=('Quantity', 'sum')
        ).reset_index()
        
        # Calculate average sales price, handle division by zero
        summary_df['avg_sales_price'] = summary_df.apply(
            lambda row: row['total_sales_usd'] / row['total_quantity'] if row['total_quantity'] > 0 else 0,
            axis=1
        )
        
        summary_df = summary_df.sort_values(by='YEAR', ascending=True)
        records = summary_df.to_dict(orient='records')
        
        return {
            "success": True,
            "count": len(records),
            "filter": f"{field_name} = '{field_value}'",
            "data": records
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error getting sales trend for {field_name} = '{field_value}': {str(e)}"
        }

@mcp.tool()
def get_account_purchase_summary(account_code: str) -> Dict[str, Any]:
    """
    Cung cấp một bản tóm tắt toàn diện cho một khách hàng (account_code) cụ thể.
    Bao gồm tổng doanh thu, năm mua hàng, và các dòng sản phẩm hàng đầu của họ.
    Rất hữu ích để có cái nhìn 360 độ nhanh về một khách hàng.
    
    Args:
        account_code: Account code để tóm tắt.
        
    Returns:
        Dict with 'success' and 'data' (một object tóm tắt duy nhất).
    """
    try:
        df = _get_processed_data()
        if df.empty:
            return {"success": True, "data": None, "message": "No data available."}

        # Filter for the specific account (case-insensitive)
        account_code_clean = account_code.strip().lower()
        account_df = df[
            df['Account Name: Account Code'].astype(str).str.strip().str.lower() == account_code_clean
        ]
        
        if account_df.empty:
            return {
                "success": True, 
                "data": None, 
                "message": f"No data found for account code '{account_code}'"
            }

        # 1. Calculate main KPIs
        total_sales_usd = account_df['Total Price (USD)'].sum()
        total_quantity = account_df['Quantity'].sum()
        first_purchase_year = int(account_df['YEAR'].min())
        last_purchase_year = int(account_df['YEAR'].max())
        unique_contracts = account_df['Contract Name'].nunique()
        unique_skus = account_df['Product: Product SKU'].nunique()

        # 2. Get top 5 product families
        top_families_df = account_df.groupby('Product: Product Family').agg(
            total_sales_usd=('Total Price (USD)', 'sum')
        ).sort_values(by='total_sales_usd', ascending=False).head(5)
        
        top_families = top_families_df.reset_index().to_dict(orient='records')

        # 3. Build the summary object
        summary_data = {
            "account_code": account_code,
            "total_sales_usd": total_sales_usd,
            "total_quantity": total_quantity,
            "first_purchase_year": first_purchase_year,
            "last_purchase_year": last_purchase_year,
            "total_contracts": unique_contracts,
            "total_unique_skus_purchased": unique_skus,
            "top_5_product_families": top_families
        }
        
        return {
            "success": True,
            "data": summary_data
        }

    except Exception as e:
        return {
            "success": False, 
            "error": f"Error summarizing account {account_code}: {str(e)}"
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
