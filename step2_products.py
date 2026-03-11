# step2_listing.py

import requests
import pandas as pd
import time
from loguru import logger

HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
}
API = "https://tiki.vn/api/personalish/v1/blocks/listings"
logger.add("logs/step2_{time}.log")

session = requests.Session()


def fetch(url, params):

    for i in range(5):  # retry 5 lần
        try:
            r = session.get(url, params=params, timeout=10)

            if r.status_code == 200:
                return r.json()

        except requests.exceptions.RequestException as e:
            print("Request error:", e)

        time.sleep(2)

    return None


def crawl_listing(category_id):

    products = []
    page = 1

    while True:
        params = {"limit": 40, "category": category_id, "page": page}

        r = requests.get(API, params=params, headers=HEADERS)
        if r.status_code != 200:
            print("Error:", r.status_code)
            break
        data = r.json().get("data")

        if r.status_code != 200:
            logger.error("Error: ", r.status_code)
            break

        for p in data:
            products.append(
                {
                    "product_id": p["id"],
                    "sku": p["sku"],
                    "name": p["name"],
                    "url_key": p["url_key"],
                    "brand": p["brand_name"],
                    "price": p["price"],
                    "original_price": p["original_price"],
                    "discount": p["discount"],
                    "discount_rate": p["discount_rate"],
                    "rating": p["rating_average"],
                    "review_count": p["review_count"],
                    "sold": p["quantity_sold"]["value"] if p["quantity_sold"] else 0,
                    "thumbnail": p["thumbnail_url"],
                    "seller_id": p["seller_id"],
                    "category_id": category_id,
                }
            )

        # logger.info(category_id, "page", page)
        logger.info(f"crawl {category_id} in page {page}")

        page += 1
        time.sleep(0.5)
        if page > 100:
            break

    return products


def main():

    categories = pd.read_csv("data/categories.csv")

    all_products = []

    for cid in categories.category_id:
        products = crawl_listing(cid)
        logger.info(f"Total products in cid: {len(products)}")
        all_products.extend(products)

    df = pd.DataFrame(all_products)
    logger.info(f"Total products: {len(df)}")
    df.to_csv("data/products.csv", index=False)


if __name__ == "__main__":
    main()
