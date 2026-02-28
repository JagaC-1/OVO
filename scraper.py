import os
import requests
import json
from supabase import create_client, Client
import boto3
from botocore.exceptions import NoCredentialsError

# 1. 設定 (請設為環境變數)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY") # 需使用 Service Role Key 以繞過 RLS
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_BUCKET_NAME = "inventory-backup"

# 2. 初始化客戶端
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
)

# 3. 定義資料來源 (範例：香港消委會市價 API)
# 注意：政府開放資料通常有特定的 JSON 結構，此處為模擬邏輯
API_URL = "https://www.consumer.org.hk/json/pricewatch/supermarket/price-watch-listing.json"

def fetch_market_data():
    print("🚀 開始抓取市價資料...")
    try:
        # 抓取資料
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(API_URL, headers=headers)
        if resp.status_code != 200:
            print(f"API Error: {resp.status_code}")
            return
        
        data = resp.json()
        # 假設 data 結構是 [{ "code": "123", "name": "可樂", "price": 5.5, "brand": "CocaCola" }, ...]
        # 實際結構需依 API 文件調整
        products = data.get('products', []) # 調整此 key
        
        print(f"📦 取得 {len(products)} 筆市價資料")

        for item in products:
            # 1. 整理資料
            barcode = item.get('barcode', '') # 假設有 Barcode
            name = item.get('name_chi', item.get('name', ''))
            price = item.get('price', {}).get('value', 0) # 結構可能很深
            
            if not name: continue

            # 2. 更新市價到 Supabase 的 market_data 表 (Raw Data)
            market_record = {
                "barcode": barcode,
                "name": name,
                "price": price,
                "source": "HK_GOV",
                "updated_at": "now()"
            }
            # Upsert
            supabase.table('market_data').upsert(market_record).execute()

            # 3. 智能配對：更新現有庫存的 'market_price'
            # 這裡用名稱模糊搜尋簡單示範
            supabase.table('inventory') \
                .update({"market_price": price, "market_updated_at": "now()"}) \
                .ilike('name', f"%{name}%") \
                .execute()

            # 4. (選用) 圖片轉存 R2
            # 如果 API 有圖且我們需要備份
            img_url = item.get('largeImage', '')
            if img_url:
                upload_to_r2(img_url, f"market/{barcode}.jpg")

        print("✅ 更新完成")

    except Exception as e:
        print(f"❌ 發生錯誤: {e}")

def upload_to_r2(url, key):
    try:
        # 下載圖片流
        img_resp = requests.get(url, stream=True)
        if img_resp.status_code == 200:
            s3_client.upload_fileobj(img_resp.raw, R2_BUCKET_NAME, key)
            print(f"☁️ 圖片已備份至 R2: {key}")
    except Exception as e:
        print(f"R2 Upload Error: {e}")

if __name__ == "__main__":
    fetch_market_data()