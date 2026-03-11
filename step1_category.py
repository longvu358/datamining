# step1_categories.py

import asyncio
from math import e
import pandas as pd
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup


async def crawl_categories():
    url = "https://tiki.vn"

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)

    html = result.html
    soup = BeautifulSoup(html, "html.parser")

    categories = {}

    for a in soup.select("a[href*='/c']"):
        name = a.text.strip()
        link = a.get("href")

        if "/c" in link:
            try:
                category_id = link.split("c")[-1].replace("?from=header_keyword", "")
                if category_id not in categories:
                    categories[category_id] = {
                        "category_id": category_id,
                        "category_name": name,
                        "category_url": f"https://tiki.vn{link}",
                    }
            except Exception as e:
                raise e

    df = pd.DataFrame(categories.values())
    df.to_csv("data/categories.csv", index=False)

    print("Saved categories:", len(df))


asyncio.run(crawl_categories())
