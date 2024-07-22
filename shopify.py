from flask import Flask
import os
import requests
from datetime import datetime, timezone
from math import floor
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Загрузка конфигурации из переменных окружения
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
ultra_auth_token = os.getenv('ULTRA_AUTH_TOKEN')
ultra_tenant_id = os.getenv('ULTRA_TENANT_ID')

def robust_request(url, method='get', headers=None, data=None, params=None, retries=5, backoff_factor=0.3):
    for attempt in range(retries):
        if method.lower() == 'get':
            response = requests.get(url, headers=headers, params=params)
        elif method.lower() == 'post':
            response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            sleep_time = backoff_factor * (2 ** attempt)
            time.sleep(sleep_time)
        else:
            return response
    return None

def fetch_product_balance():
    print("Fetching product balance...")
    current_utc_time = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    url = 'https://api.ultra-company.com/tenant/report/productBalance'
    headers = {
        'Authorization': f'Bearer {ultra_auth_token}',
        'X-TenantID': ultra_tenant_id
    }
    params = {
        'groupBy': 'PRODUCT_GROUP',
        'date': current_utc_time,
        'warehouseId': '2',
        'productGroupId': '23',
        'balanceType': 'ALL'
    }
    return robust_request(url, method='get', headers=headers, params=params)

@app.route('/update_inventory')
def update_shopify_inventory():
    response = fetch_product_balance()
    if response and response.status_code == 200:
        product_data = response.json()
        for item in product_data:
            if item['vendorCode'] and item['count'] is not None:
                sku = item['vendorCode']
                variant_id = get_variant_id_by_sku(sku)
                if variant_id:
                    inventory_item_id = get_inventory_item_id(variant_id)
                    if inventory_item_id:
                        count = max(0, floor(item['count']))
                        url = f'{shopify_store_url}/inventory_levels/set.json'
                        data = {
                            'location_id': 89053102423,
                            'inventory_item_id': inventory_item_id,
                            'available': count
                        }
                        update_response = robust_request(url, method='post', headers={
                            'Content-Type': 'application/json',
                            'X-Shopify-Access-Token': access_token
                        }, data=data)
                        if update_response and update_response.status_code == 200:
                            return f"Successfully updated SKU {sku} with count {count}"
                        else:
                            return f"Failed to update SKU {sku}: {update_response.status_code} {update_response.text}"
                else:
                    return f"SKU {sku} not found in Shopify."
    return "Failed to fetch product balance or process data"

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: app.test_client().get('/update_inventory'), 'interval', minutes=120)
    scheduler.start()
    app.run(host='0.0.0.0', port=80, debug=True)