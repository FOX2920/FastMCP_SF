from fastmcp import FastMCP
import os
from typing import Dict, Any, List, Optional
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import warnings
import numpy as np
from collections import defaultdict

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
    
    # Convert and extract year, month, quarter
    df['Created_Date'] = pd.to_datetime(
        df.get('Contract_Created_Date__c'), 
        errors='coerce'
    )
    df_export['YEAR'] = df['Created_Date'].dt.year
    df_export['MONTH'] = df['Created_Date'].dt.month
    df_export['QUARTER'] = df['Created_Date'].dt.quarter
    df_export['MONTH_NAME'] = df['Created_Date'].dt.strftime('%B')
    df_export['WEEK_OF_YEAR'] = df['Created_Date'].dt.isocalendar().week
    
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
    df_export['Created_Date_Raw'] = df['Created_Date']
    
    # 3. Lọc và Sắp xếp
    current_year = datetime.now().year
    df_export = df_export[
        (df_export['YEAR'] >= 2015) & 
        (df_export['YEAR'] <= current_year)
    ]
    df_export = df_export.dropna(subset=['Account Name: Account Code'])
    
    df_export = df_export.sort_values(
        by=['Account Name: Account Code', 'YEAR', 'MONTH'],
        ascending=[True, False, False]
    )
    
    # Reset index
    df_export = df_export.reset_index(drop=True)
    
    return df_export

def _filter_by_account(df: pd.DataFrame, account_code: str) -> pd.DataFrame:
    """Helper function to filter DataFrame by account code"""
    if df.empty:
        return df
    
    account_code_clean = account_code.strip().lower()
    return df[
        df['Account Name: Account Code'].astype(str).str.strip().str.lower() == account_code_clean
    ]

# --- TOOL 1: DANH SÁCH KHÁCH HÀNG ---

@mcp.tool()
def get_all_customers_list() -> Dict[str, Any]:
    """
    Lấy danh sách tất cả khách hàng (Account Code) có trong hệ thống.
    Get list of all customer account codes in the system.
    
    Returns:
        Dict with 'success', 'count', 'customers' keys
    """
    try:
        df = _get_processed_data()
        
        if df.empty:
            return {
                "success": True, 
                "count": 0, 
                "customers": [],
                "message": "No customers found"
            }
        
        # Lấy danh sách unique account codes
        customers = df['Account Name: Account Code'].unique().tolist()
        customers = sorted([c for c in customers if pd.notna(c)])
        
        return {
            "success": True, 
            "count": len(customers), 
            "customers": customers
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error getting customers list: {str(e)}"
        }

# --- TOOL 2: LIỆT KÊ SẢN PHẨM CỦA KHÁCH HÀNG ---

@mcp.tool()
def get_customer_products(account_code: str, year: Optional[int] = None) -> Dict[str, Any]:
    """
    Liệt kê tất cả sản phẩm của một khách hàng, có thể lọc theo năm.
    List all products of a specific customer, optionally filtered by year.
    
    Args:
        account_code: Customer account code
        year: Optional year filter (e.g., 2024)
        
    Returns:
        Dict with product details grouped by product type
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True, 
                "count": 0, 
                "products": [], 
                "message": f"No products found for account {account_code}"
            }
        
        # Lọc theo năm nếu có
        if year:
            df_filtered = df_filtered[df_filtered['YEAR'] == year]
            if df_filtered.empty:
                return {
                    "success": True, 
                    "count": 0, 
                    "products": [], 
                    "message": f"No products found for account {account_code} in year {year}"
                }
        
        # Tạo danh sách sản phẩm
        products = []
        for _, row in df_filtered.iterrows():
            product = {
                "product_sku": row.get('Product: Product SKU'),
                "product_family": row.get('Product: Product Family'),
                "stone_color_type": row.get('Product: STONE Color Type'),
                "product_description": row.get('Product Discription'),
                "dimensions": {
                    "length": row.get('Length'),
                    "width": row.get('Width'),
                    "height": row.get('Height')
                },
                "segment": row.get('Segment'),
                "quantity": row.get('Quantity'),
                "m2": row.get('m2'),
                "m3": row.get('m3'),
                "tons": row.get('Tons'),
                "containers": row.get('Cont'),
                "sales_price": row.get('Sales Price'),
                "total_price_usd": row.get('Total Price (USD)'),
                "charge_unit": row.get('Charge Unit (PI)'),
                "order_date": row.get('Created Date (C)'),
                "year": row.get('YEAR'),
                "month": row.get('MONTH'),
                "quarter": row.get('QUARTER'),
                "contract_name": row.get('Contract Name')
            }
            products.append(product)
        
        # Tạo summary
        unique_skus = df_filtered['Product: Product SKU'].nunique()
        unique_families = df_filtered['Product: Product Family'].nunique()
        total_quantity = df_filtered['Quantity'].sum()
        total_value = df_filtered['Total Price (USD)'].sum()
        
        return {
            "success": True,
            "account_code": account_code,
            "year": year,
            "count": len(products),
            "summary": {
                "unique_product_skus": int(unique_skus),
                "unique_product_families": int(unique_families),
                "total_quantity": int(total_quantity),
                "total_value_usd": float(total_value) if pd.notna(total_value) else 0
            },
            "products": products
        }
        
    except Exception as e:
        return {
            "success": False, 
            "error": f"Error getting customer products: {str(e)}"
        }

# --- TOOL 3: LỊCH SỬ MUA HÀNG 5 NĂM ---

@mcp.tool()
def get_customer_purchase_history(
    account_code: str, 
    years_back: int = 5
) -> Dict[str, Any]:
    """
    Lấy lịch sử mua hàng chi tiết của khách hàng trong N năm gần nhất.
    Get detailed purchase history of a customer for the last N years.
    Dữ liệu này dùng để ChatGPT phân tích và dự đoán xu hướng.
    
    Args:
        account_code: Customer account code
        years_back: Number of years to look back (default: 5)
        
    Returns:
        Dict with detailed purchase history organized by year, quarter, month
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "data": {},
                "message": f"No purchase history found for account {account_code}"
            }
        
        # Lọc theo số năm
        current_year = datetime.now().year
        start_year = current_year - years_back
        df_filtered = df_filtered[df_filtered['YEAR'] >= start_year]
        
        # Organize data by year -> quarter -> month
        history_by_year = defaultdict(lambda: {
            "total_orders": 0,
            "total_quantity": 0,
            "total_value_usd": 0,
            "quarters": defaultdict(lambda: {
                "total_orders": 0,
                "total_quantity": 0,
                "total_value_usd": 0,
                "months": defaultdict(lambda: {
                    "orders": [],
                    "total_quantity": 0,
                    "total_value_usd": 0
                })
            })
        })
        
        for _, row in df_filtered.iterrows():
            year = int(row['YEAR'])
            quarter = int(row['QUARTER'])
            month = int(row['MONTH'])
            
            order_data = {
                "date": row['Created Date (C)'],
                "product_sku": row['Product: Product SKU'],
                "product_family": row['Product: Product Family'],
                "stone_color": row['Product: STONE Color Type'],
                "segment": row['Segment'],
                "dimensions": {
                    "length": float(row['Length']) if pd.notna(row['Length']) else None,
                    "width": float(row['Width']) if pd.notna(row['Width']) else None,
                    "height": float(row['Height']) if pd.notna(row['Height']) else None
                },
                "quantity": int(row['Quantity']),
                "m2": float(row['m2']) if pd.notna(row['m2']) else None,
                "m3": float(row['m3']) if pd.notna(row['m3']) else None,
                "tons": float(row['Tons']) if pd.notna(row['Tons']) else None,
                "containers": float(row['Cont']) if pd.notna(row['Cont']) else None,
                "sales_price": float(row['Sales Price']) if pd.notna(row['Sales Price']) else None,
                "total_price_usd": float(row['Total Price (USD)']) if pd.notna(row['Total Price (USD)']) else None
            }
            
            # Add to month
            history_by_year[year]["quarters"][quarter]["months"][month]["orders"].append(order_data)
            history_by_year[year]["quarters"][quarter]["months"][month]["total_quantity"] += int(row['Quantity'])
            history_by_year[year]["quarters"][quarter]["months"][month]["total_value_usd"] += float(row['Total Price (USD)']) if pd.notna(row['Total Price (USD)']) else 0
            
            # Update quarter totals
            history_by_year[year]["quarters"][quarter]["total_orders"] += 1
            history_by_year[year]["quarters"][quarter]["total_quantity"] += int(row['Quantity'])
            history_by_year[year]["quarters"][quarter]["total_value_usd"] += float(row['Total Price (USD)']) if pd.notna(row['Total Price (USD)']) else 0
            
            # Update year totals
            history_by_year[year]["total_orders"] += 1
            history_by_year[year]["total_quantity"] += int(row['Quantity'])
            history_by_year[year]["total_value_usd"] += float(row['Total Price (USD)']) if pd.notna(row['Total Price (USD)']) else 0
        
        # Convert defaultdict to regular dict for JSON serialization
        history_dict = {}
        for year, year_data in history_by_year.items():
            quarters_dict = {}
            for quarter, quarter_data in year_data["quarters"].items():
                months_dict = {}
                for month, month_data in quarter_data["months"].items():
                    months_dict[str(month)] = dict(month_data)
                quarters_dict[f"Q{quarter}"] = {
                    "total_orders": quarter_data["total_orders"],
                    "total_quantity": quarter_data["total_quantity"],
                    "total_value_usd": quarter_data["total_value_usd"],
                    "months": months_dict
                }
            history_dict[str(year)] = {
                "total_orders": year_data["total_orders"],
                "total_quantity": year_data["total_quantity"],
                "total_value_usd": year_data["total_value_usd"],
                "quarters": quarters_dict
            }
        
        return {
            "success": True,
            "account_code": account_code,
            "years_analyzed": years_back,
            "period": f"{start_year}-{current_year}",
            "data": history_dict
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error getting purchase history: {str(e)}"
        }

# --- TOOL 4: XU HƯỚNG SẢN PHẨM THEO THỜI GIAN ---

@mcp.tool()
def get_customer_product_trends(account_code: str) -> Dict[str, Any]:
    """
    Phân tích xu hướng sản phẩm của khách hàng theo thời gian.
    Analyze customer product trends over time for prediction purposes.
    Dữ liệu này giúp ChatGPT dự đoán sản phẩm khách hàng sẽ đặt tiếp.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with trend analysis by product family, SKU, color, dimensions
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "trends": {},
                "message": f"No data found for account {account_code}"
            }
        
        # 1. Xu hướng theo Product Family
        family_trends = []
        for family in df_filtered['Product: Product Family'].dropna().unique():
            family_df = df_filtered[df_filtered['Product: Product Family'] == family]
            
            yearly_data = []
            for year in sorted(family_df['YEAR'].unique()):
                year_df = family_df[family_df['YEAR'] == year]
                yearly_data.append({
                    "year": int(year),
                    "orders": len(year_df),
                    "total_quantity": int(year_df['Quantity'].sum()),
                    "total_value_usd": float(year_df['Total Price (USD)'].sum()),
                    "avg_quantity_per_order": float(year_df['Quantity'].mean())
                })
            
            family_trends.append({
                "product_family": family,
                "total_orders": len(family_df),
                "total_quantity": int(family_df['Quantity'].sum()),
                "yearly_breakdown": yearly_data
            })
        
        # 2. Xu hướng theo màu đá
        color_trends = []
        for color in df_filtered['Product: STONE Color Type'].dropna().unique():
            color_df = df_filtered[df_filtered['Product: STONE Color Type'] == color]
            
            yearly_data = []
            for year in sorted(color_df['YEAR'].unique()):
                year_df = color_df[color_df['YEAR'] == year]
                yearly_data.append({
                    "year": int(year),
                    "orders": len(year_df),
                    "total_quantity": int(year_df['Quantity'].sum())
                })
            
            color_trends.append({
                "stone_color": color,
                "total_orders": len(color_df),
                "total_quantity": int(color_df['Quantity'].sum()),
                "yearly_breakdown": yearly_data
            })
        
        # 3. Xu hướng kích thước (dimensions)
        dimension_trends = []
        dimension_groups = df_filtered.groupby(['Length', 'Width', 'Height'])
        for dims, group in dimension_groups:
            if all(pd.notna(d) for d in dims):
                yearly_data = []
                for year in sorted(group['YEAR'].unique()):
                    year_df = group[group['YEAR'] == year]
                    yearly_data.append({
                        "year": int(year),
                        "orders": len(year_df),
                        "total_quantity": int(year_df['Quantity'].sum())
                    })
                
                dimension_trends.append({
                    "dimensions": {
                        "length": float(dims[0]),
                        "width": float(dims[1]),
                        "height": float(dims[2])
                    },
                    "total_orders": len(group),
                    "total_quantity": int(group['Quantity'].sum()),
                    "yearly_breakdown": yearly_data
                })
        
        # Sort by total quantity
        dimension_trends.sort(key=lambda x: x['total_quantity'], reverse=True)
        
        # 4. Xu hướng theo Segment
        segment_trends = []
        for segment in df_filtered['Segment'].dropna().unique():
            segment_df = df_filtered[df_filtered['Segment'] == segment]
            
            yearly_data = []
            for year in sorted(segment_df['YEAR'].unique()):
                year_df = segment_df[segment_df['YEAR'] == year]
                yearly_data.append({
                    "year": int(year),
                    "orders": len(year_df),
                    "total_quantity": int(year_df['Quantity'].sum())
                })
            
            segment_trends.append({
                "segment": segment,
                "total_orders": len(segment_df),
                "total_quantity": int(segment_df['Quantity'].sum()),
                "yearly_breakdown": yearly_data
            })
        
        return {
            "success": True,
            "account_code": account_code,
            "trends": {
                "by_product_family": family_trends,
                "by_stone_color": color_trends,
                "by_dimensions": dimension_trends[:10],  # Top 10
                "by_segment": segment_trends
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing product trends: {str(e)}"
        }

# --- TOOL 5: PATTERN ĐẶT HÀNG (THEO QUÝ, THÁNG) ---

@mcp.tool()
def get_customer_order_patterns(account_code: str) -> Dict[str, Any]:
    """
    Phân tích pattern đặt hàng của khách hàng theo thời gian.
    Analyze customer ordering patterns by season, quarter, month.
    Giúp dự đoán khách hàng thường đặt hàng vào thời điểm nào trong năm.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with ordering patterns analysis
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "patterns": {},
                "message": f"No data found for account {account_code}"
            }
        
        # 1. Pattern theo Quý
        quarter_pattern = []
        for quarter in range(1, 5):
            quarter_df = df_filtered[df_filtered['QUARTER'] == quarter]
            if not quarter_df.empty:
                quarter_pattern.append({
                    "quarter": f"Q{quarter}",
                    "total_orders": len(quarter_df),
                    "total_quantity": int(quarter_df['Quantity'].sum()),
                    "total_value_usd": float(quarter_df['Total Price (USD)'].sum()),
                    "avg_order_value": float(quarter_df['Total Price (USD)'].mean()),
                    "percentage_of_annual_orders": 0  # Will calculate later
                })
        
        # Calculate percentage
        total_orders = len(df_filtered)
        for q in quarter_pattern:
            q["percentage_of_annual_orders"] = round((q["total_orders"] / total_orders) * 100, 2)
        
        # 2. Pattern theo Tháng
        month_pattern = []
        for month in range(1, 13):
            month_df = df_filtered[df_filtered['MONTH'] == month]
            if not month_df.empty:
                month_name = pd.Timestamp(year=2024, month=month, day=1).strftime('%B')
                month_pattern.append({
                    "month": month,
                    "month_name": month_name,
                    "total_orders": len(month_df),
                    "total_quantity": int(month_df['Quantity'].sum()),
                    "total_value_usd": float(month_df['Total Price (USD)'].sum()),
                    "percentage_of_annual_orders": round((len(month_df) / total_orders) * 100, 2)
                })
        
        # 3. Tần suất đặt hàng
        years_active = df_filtered['YEAR'].nunique()
        avg_orders_per_year = total_orders / years_active if years_active > 0 else 0
        
        # 4. Các tháng có đơn hàng nhiều nhất
        top_months = sorted(month_pattern, key=lambda x: x['total_orders'], reverse=True)[:3]
        
        # 5. Các quý có đơn hàng nhiều nhất
        top_quarters = sorted(quarter_pattern, key=lambda x: x['total_orders'], reverse=True)[:2]
        
        return {
            "success": True,
            "account_code": account_code,
            "summary": {
                "total_years_active": int(years_active),
                "total_orders": int(total_orders),
                "avg_orders_per_year": round(avg_orders_per_year, 2),
                "top_ordering_months": [m["month_name"] for m in top_months],
                "top_ordering_quarters": [q["quarter"] for q in top_quarters]
            },
            "patterns": {
                "by_quarter": quarter_pattern,
                "by_month": month_pattern
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing order patterns: {str(e)}"
        }

# --- TOOL 6: PHÂN TÍCH THEO SEGMENT ---

@mcp.tool()
def get_customer_product_by_segment(account_code: str) -> Dict[str, Any]:
    """
    Phân tích sản phẩm của khách hàng theo Segment (ứng dụng).
    Analyze customer products by segment/application.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with segment analysis
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "segments": [],
                "message": f"No data found for account {account_code}"
            }
        
        segments_analysis = []
        for segment in df_filtered['Segment'].dropna().unique():
            segment_df = df_filtered[df_filtered['Segment'] == segment]
            
            # Top products trong segment này
            top_products = []
            product_groups = segment_df.groupby('Product: Product SKU')
            for sku, group in product_groups:
                if pd.notna(sku):
                    top_products.append({
                        "product_sku": sku,
                        "orders": len(group),
                        "total_quantity": int(group['Quantity'].sum()),
                        "total_value_usd": float(group['Total Price (USD)'].sum())
                    })
            
            top_products.sort(key=lambda x: x['total_quantity'], reverse=True)
            
            segments_analysis.append({
                "segment": segment,
                "total_orders": len(segment_df),
                "total_quantity": int(segment_df['Quantity'].sum()),
                "total_value_usd": float(segment_df['Total Price (USD)'].sum()),
                "unique_products": int(segment_df['Product: Product SKU'].nunique()),
                "top_products": top_products[:5]  # Top 5
            })
        
        # Sort by total value
        segments_analysis.sort(key=lambda x: x['total_value_usd'], reverse=True)
        
        return {
            "success": True,
            "account_code": account_code,
            "total_segments": len(segments_analysis),
            "segments": segments_analysis
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing segments: {str(e)}"
        }

# --- TOOL 7: PHÂN TÍCH THEO MÀU ĐÁ ---

@mcp.tool()
def get_customer_product_by_color(account_code: str) -> Dict[str, Any]:
    """
    Phân tích sản phẩm của khách hàng theo màu đá.
    Analyze customer products by stone color type.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with color analysis
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "colors": [],
                "message": f"No data found for account {account_code}"
            }
        
        colors_analysis = []
        for color in df_filtered['Product: STONE Color Type'].dropna().unique():
            color_df = df_filtered[df_filtered['Product: STONE Color Type'] == color]
            
            # Products with this color
            products_in_color = []
            product_groups = color_df.groupby('Product: Product SKU')
            for sku, group in product_groups:
                if pd.notna(sku):
                    products_in_color.append({
                        "product_sku": sku,
                        "product_family": group['Product: Product Family'].iloc[0] if not group['Product: Product Family'].empty else None,
                        "orders": len(group),
                        "total_quantity": int(group['Quantity'].sum())
                    })
            
            products_in_color.sort(key=lambda x: x['total_quantity'], reverse=True)
            
            colors_analysis.append({
                "stone_color": color,
                "total_orders": len(color_df),
                "total_quantity": int(color_df['Quantity'].sum()),
                "total_value_usd": float(color_df['Total Price (USD)'].sum()),
                "unique_products": int(color_df['Product: Product SKU'].nunique()),
                "products": products_in_color[:10]  # Top 10
            })
        
        # Sort by total quantity
        colors_analysis.sort(key=lambda x: x['total_quantity'], reverse=True)
        
        return {
            "success": True,
            "account_code": account_code,
            "total_colors": len(colors_analysis),
            "colors": colors_analysis
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing colors: {str(e)}"
        }

# --- TOOL 8: PHÂN TÍCH THEO PRODUCT FAMILY (ỨNG DỤNG) ---

@mcp.tool()
def get_customer_product_by_family(account_code: str) -> Dict[str, Any]:
    """
    Phân tích sản phẩm của khách hàng theo Product Family (ứng dụng).
    Analyze customer products by product family/application.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with product family analysis
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "families": [],
                "message": f"No data found for account {account_code}"
            }
        
        families_analysis = []
        for family in df_filtered['Product: Product Family'].dropna().unique():
            family_df = df_filtered[df_filtered['Product: Product Family'] == family]
            
            # Yearly trend
            yearly_trend = []
            for year in sorted(family_df['YEAR'].unique()):
                year_df = family_df[family_df['YEAR'] == year]
                yearly_trend.append({
                    "year": int(year),
                    "orders": len(year_df),
                    "quantity": int(year_df['Quantity'].sum()),
                    "value_usd": float(year_df['Total Price (USD)'].sum())
                })
            
            families_analysis.append({
                "product_family": family,
                "total_orders": len(family_df),
                "total_quantity": int(family_df['Quantity'].sum()),
                "total_value_usd": float(family_df['Total Price (USD)'].sum()),
                "unique_products": int(family_df['Product: Product SKU'].nunique()),
                "yearly_trend": yearly_trend
            })
        
        # Sort by total value
        families_analysis.sort(key=lambda x: x['total_value_usd'], reverse=True)
        
        return {
            "success": True,
            "account_code": account_code,
            "total_families": len(families_analysis),
            "families": families_analysis
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing product families: {str(e)}"
        }

# --- TOOL 9: THỐNG KÊ TỔNG QUAN KHÁCH HÀNG ---

@mcp.tool()
def get_customer_summary_stats(account_code: str) -> Dict[str, Any]:
    """
    Thống kê tổng quan về khách hàng.
    Get comprehensive summary statistics for a customer.
    Bao gồm tổng đơn hàng, tổng giá trị, sản phẩm yêu thích, v.v.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with comprehensive customer statistics
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "stats": {},
                "message": f"No data found for account {account_code}"
            }
        
        # Basic stats
        total_orders = len(df_filtered)
        total_quantity = int(df_filtered['Quantity'].sum())
        total_value = float(df_filtered['Total Price (USD)'].sum())
        avg_order_value = total_value / total_orders if total_orders > 0 else 0
        
        # Time range
        first_order = df_filtered['Created_Date_Raw'].min()
        last_order = df_filtered['Created_Date_Raw'].max()
        years_active = df_filtered['YEAR'].nunique()
        
        # Product diversity
        unique_skus = int(df_filtered['Product: Product SKU'].nunique())
        unique_families = int(df_filtered['Product: Product Family'].nunique())
        unique_colors = int(df_filtered['Product: STONE Color Type'].nunique())
        unique_segments = int(df_filtered['Segment'].nunique())
        
        # Top products
        top_products_by_quantity = (
            df_filtered.groupby('Product: Product SKU')['Quantity']
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )
        
        # Most frequent dimensions
        dimension_counts = df_filtered.groupby(['Length', 'Width', 'Height']).size()
        most_common_dims = dimension_counts.sort_values(ascending=False).head(3)
        
        common_dimensions = []
        for dims, count in most_common_dims.items():
            if all(pd.notna(d) for d in dims):
                common_dimensions.append({
                    "dimensions": {
                        "length": float(dims[0]),
                        "width": float(dims[1]),
                        "height": float(dims[2])
                    },
                    "order_count": int(count)
                })
        
        # Yearly breakdown
        yearly_stats = []
        for year in sorted(df_filtered['YEAR'].unique()):
            year_df = df_filtered[df_filtered['YEAR'] == year]
            yearly_stats.append({
                "year": int(year),
                "orders": len(year_df),
                "quantity": int(year_df['Quantity'].sum()),
                "value_usd": float(year_df['Total Price (USD)'].sum())
            })
        
        return {
            "success": True,
            "account_code": account_code,
            "summary": {
                "total_orders": total_orders,
                "total_quantity": total_quantity,
                "total_value_usd": round(total_value, 2),
                "avg_order_value_usd": round(avg_order_value, 2),
                "first_order_date": first_order.strftime('%Y-%m-%d') if pd.notna(first_order) else None,
                "last_order_date": last_order.strftime('%Y-%m-%d') if pd.notna(last_order) else None,
                "years_active": int(years_active),
                "avg_orders_per_year": round(total_orders / years_active, 2) if years_active > 0 else 0
            },
            "product_diversity": {
                "unique_product_skus": unique_skus,
                "unique_product_families": unique_families,
                "unique_stone_colors": unique_colors,
                "unique_segments": unique_segments
            },
            "top_products": {
                "by_quantity": [
                    {"product_sku": k, "total_quantity": int(v)} 
                    for k, v in top_products_by_quantity.items()
                ]
            },
            "most_common_dimensions": common_dimensions,
            "yearly_breakdown": yearly_stats
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error getting customer summary: {str(e)}"
        }

# --- TOOL 10: PHÂN TÍCH THEO MÙA VỤ ---

@mcp.tool()
def get_customer_seasonal_analysis(account_code: str) -> Dict[str, Any]:
    """
    Phân tích theo mùa vụ để tìm pattern đặt hàng theo thời vụ.
    Seasonal analysis to find ordering patterns by season.
    Giúp dự đoán khách hàng sẽ đặt hàng vào mùa nào.
    
    Args:
        account_code: Customer account code
        
    Returns:
        Dict with seasonal analysis
    """
    try:
        if not account_code or not account_code.strip():
            return {"success": False, "error": "Account code cannot be empty"}
        
        df = _get_processed_data()
        df_filtered = _filter_by_account(df, account_code)
        
        if df_filtered.empty:
            return {
                "success": True,
                "seasonal_patterns": {},
                "message": f"No data found for account {account_code}"
            }
        
        # Define seasons
        def get_season(month):
            if month in [12, 1, 2]:
                return "Winter"
            elif month in [3, 4, 5]:
                return "Spring"
            elif month in [6, 7, 8]:
                return "Summer"
            else:
                return "Fall"
        
        df_filtered['Season'] = df_filtered['MONTH'].apply(get_season)
        
        # Seasonal analysis
        seasons = ["Spring", "Summer", "Fall", "Winter"]
        seasonal_data = []
        
        for season in seasons:
            season_df = df_filtered[df_filtered['Season'] == season]
            if not season_df.empty:
                # Top products in this season
                top_products_season = (
                    season_df.groupby('Product: Product SKU')['Quantity']
                    .sum()
                    .sort_values(ascending=False)
                    .head(3)
                    .to_dict()
                )
                
                seasonal_data.append({
                    "season": season,
                    "total_orders": len(season_df),
                    "total_quantity": int(season_df['Quantity'].sum()),
                    "total_value_usd": float(season_df['Total Price (USD)'].sum()),
                    "percentage_of_annual_orders": 0,  # Will calculate
                    "top_products": [
                        {"product_sku": k, "quantity": int(v)} 
                        for k, v in top_products_season.items()
                    ]
                })
        
        # Calculate percentages
        total_orders = len(df_filtered)
        for season_data in seasonal_data:
            season_data["percentage_of_annual_orders"] = round(
                (season_data["total_orders"] / total_orders) * 100, 2
            )
        
        # Find peak season
        peak_season = max(seasonal_data, key=lambda x: x['total_orders'])
        
        return {
            "success": True,
            "account_code": account_code,
            "peak_season": peak_season["season"],
            "seasonal_patterns": seasonal_data
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error analyzing seasonal patterns: {str(e)}"
        }

# --- TOOLS GỐC (GIỮ NGUYÊN) ---

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
        
        # Lọc DataFrame
        filtered_df = _filter_by_account(df, account_code)
        
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
        "version": "2.0.0"
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
    - MONTH (1-12)
    - QUARTER (Q1-Q4)
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

@mcp.resource("contract://info/available-tools")
def get_available_tools() -> str:
    """Information about available MCP tools for analysis"""
    return """
    Available MCP Tools for Customer Analysis:
    
    1. BASIC DATA RETRIEVAL:
       - get_all_customers_list(): List all customer account codes
       - get_customer_products(): List all products of a customer
       - get_contract_details_by_account(): Get raw contract details
    
    2. HISTORICAL ANALYSIS (5 Years):
       - get_customer_purchase_history(): Detailed 5-year purchase history
       - get_customer_product_trends(): Product trends over time
       - get_customer_order_patterns(): Ordering patterns by quarter/month
    
    3. PRODUCT ANALYSIS BY DIMENSIONS:
       - get_customer_product_by_segment(): Analysis by segment/application
       - get_customer_product_by_color(): Analysis by stone color
       - get_customer_product_by_family(): Analysis by product family
    
    4. ADVANCED ANALYTICS:
       - get_customer_summary_stats(): Comprehensive customer statistics
       - get_customer_seasonal_analysis(): Seasonal ordering patterns
    
    5. PREDICTION SUPPORT:
       All tools provide structured data that ChatGPT can use to:
       - Predict which products customers will order
       - Predict product dimensions they prefer
       - Predict quantities they typically order
       - Predict when (quarter/month) they will order
       - Identify trends and patterns for forecasting
    """

# --- Chạy Server ---

if __name__ == "__main__":
    mcp.run()
```

## Hướng dẫn sử dụng với ChatGPT:

### 1. **Liệt kê sản phẩm của khách hàng:**
```
"List all products of customer ABC-001"
"Show me products customer XYZ ordered in 2024"
```

### 2. **Dự đoán sản phẩm khách hàng sẽ đặt:**
```
"Based on the 5-year purchase history of customer ABC-001, predict:
- What products they will likely order next
- What dimensions they prefer
- How much quantity they typically order
- When (which quarter/month) they will order"
```

### 3. **Phân tích theo góc nhìn khác:**
```
"Analyze customer ABC-001 by:
- Segment/Application
- Stone color preferences
- Product family trends"
```

### 4. **Tương tác với dữ liệu 5 năm:**
```
"Get 5-year purchase history of customer ABC-001"
"Show me ordering patterns of customer ABC-001 over the last 5 years"
"What are the seasonal trends for customer ABC-001?"
```

### 5. **Report dự đoán tổng hợp:**
```
"Create a comprehensive prediction report for customer ABC-001 including:
- Historical trends (5 years)
- Product preferences (type, dimension, color)
- Ordering patterns (when they order)
- Predicted next order details"
