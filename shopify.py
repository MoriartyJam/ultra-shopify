import os
import requests
from datetime import datetime, timezone
from math import floor
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import time


app = Flask(__name__)

# Environment variables
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
ultra_auth_token = os.getenv('ULTRA_AUTH_TOKEN')
ultra_tenant_id = os.getenv('ULTRA_TENANT_ID')


def robust_request(url, method='get', headers=None, data=None, params=None, retries=5, backoff_factor=0.3):
    print(f"Requesting {url} with method {method}")
    for attempt in range(retries):
        try:
            if method.lower() == 'get':
                response = requests.get(url, headers=headers, params=params)
            elif method.lower() == 'post':
                response = requests.post(url, json=data, headers=headers)

            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Handling rate limiting
                sleep_time = backoff_factor * (2 ** attempt)
                print(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed with status {response.status_code}: {response.text}")
                if attempt == retries - 1:
                    return response  # Returns the last attempt's response for further handling
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            time.sleep(backoff_factor * (2 ** attempt))

    print("Max retries exceeded, request failed.")
    return None  # or raise an exception for critical processes


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
    response = robust_request(url, headers=headers, params=params)
    if response and response.status_code == 200:
        data = response.json()
        return data  # Return the whole JSON data
    else:
        print("Failed to fetch data.")
        return None


def get_variant_id_by_sku(sku):
    print(f"Searching for SKU: {sku}")
    url = f'{shopify_store_url}/products.json'
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }
    while url:
        response = robust_request(url, method='get', headers=headers)
        if response and response.status_code == 200:
            products = response.json()['products']
            for product in products:
                for variant in product['variants']:
                    if variant['sku'] == sku:
                        return variant['id']
            # Обработка пагинации
            links = response.headers.get('Link', None)
            if links:
                next_link = [link.split(';')[0].strip('<>') for link in links.split(',') if 'rel="next"' in link]
                url = next_link[0] if next_link else None
            else:
                url = None
        else:
            break
    return None

def get_inventory_item_id(variant_id):
    print(f"Fetching inventory item ID for variant: {variant_id}")
    url = f'{shopify_store_url}/variants/{variant_id}.json'
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }
    response = robust_request(url, method='get', headers=headers)
    if response and response.status_code == 200:
        return response.json()['variant']['inventory_item_id']
    return None

def update_shopify_inventory():
    print("Updating Shopify inventory...")
    product_data = fetch_product_balance()
    if product_data:
        for item in product_data:
            if item['vendorCode'] and item['count'] is not None:  # Убедимся, что count существует
                sku = item['vendorCode']
                variant_id = get_variant_id_by_sku(sku)
                if variant_id:
                    inventory_item_id = get_inventory_item_id(variant_id)
                    if inventory_item_id:
                        count = max(0, floor(item['count']))  # Используем max для обеспечения неотрицательного значения
                        url = f'{shopify_store_url}/inventory_levels/set.json'
                        data = {
                            'location_id': 89053102423,
                            'inventory_item_id': inventory_item_id,
                            'available': count
                        }
                        headers = {
                            'Content-Type': 'application/json',
                            'X-Shopify-Access-Token': access_token
                        }
                        response = robust_request(url, method='post', headers=headers, data=data)
                        if response and response.status_code == 200:
                            print(f"Successfully updated SKU {sku} with count {count}")
                        else:
                            print(f"Failed to update SKU {sku}: {response.status_code} {response.text}")
                    else:
                        print(f"Failed to find inventory item for SKU {sku}")
                else:
                    print(f"SKU {sku} not found in Shopify.")

@app.route('/')
def index():
    return "The scheduler is running. Check your console for output."

executors = {
    'default': ThreadPoolExecutor(20)
}

scheduler = BackgroundScheduler(executors=executors)
scheduler.add_job(func=update_shopify_inventory, trigger='interval', minutes=160)
scheduler.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)







