import os
import requests
from datetime import datetime, timezone
from math import floor
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler

# Загрузка конфигурации из переменных окружения
shopify_store_url = os.getenv('SHOPIFY_STORE_URL')
access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
ultra_auth_token = os.getenv('ULTRA_AUTH_TOKEN')
ultra_tenant_id = os.getenv('ULTRA_TENANT_ID')

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
    response = robust_request(url, method='get', headers=headers, params=params)
    if response:
        return response.json()
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


def robust_request(url, method='get', headers=None, data=None, params=None, retries=5, backoff_factor=0.3):
    print(f"Requesting {url} with method {method}")
    for attempt in range(retries):
        if method.lower() == 'get':
            response = requests.get(url, headers=headers, params=params)
        elif method.lower() == 'post':
            response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            sleep_time = backoff_factor * (2 ** attempt)
            print(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        else:
            print(f"Failed with status {response.status_code}: {response.text}")
            return response
    print("Max retries exceeded, request failed.")
    return None  # or raise an exception for critical processes


scheduler = BackgroundScheduler()
scheduler.add_job(update_shopify_inventory, 'interval', minutes=5)
scheduler.start()

try:

    while True:
        time.sleep(60)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
