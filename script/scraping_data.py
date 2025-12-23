# ----------------------------------------------------------------
# 1. Check the 
# ----------------------------------------------------------------

import os
import time
import random
import pandas as pd
from loguru import logger
from curl_cffi import requests
from bs4 import BeautifulSoup

# ----------------------------------------------------------------
#                          Simple Logging
# ----------------------------------------------------------------
logger.remove()

logger.add(
    "scraping_log_{time}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Upgrade-Insecure-Requests": "1",
}

def get_search_page(page_no):
    """Fetch Amazon search result page"""
    url = f"https://www.amazon.in/s?k=laptop&page={page_no}"
    response = requests.get(url, headers=HEADERS, timeout=30)
    return BeautifulSoup(response.text, "html.parser")

def count_page_cards(soup):
    """How many cards available in that page"""
    cards = soup.find_all("div",
    class_="a-section a-spacing-small a-spacing-top-small"
    )
    return len(cards)

def extract_product_links(soup):
    """Full link of the product"""
    links = []
    tags = soup.select(
        "a.a-link-normal.s-line-clamp-2.s-link-style.a-text-normal"
    )

    for tag in tags:
        href = tag.get("href")
        if href:
            links.append("https://www.amazon.in" + href)

    return links

def safe_text(tag, default=None):
    """Safely extract text from a BeautifulSoup tag"""
    return tag.get_text(strip=True) if tag else default

session = requests.Session()
session = requests.Session()
session.get(
    "https://www.amazon.in",
    headers=HEADERS,
    impersonate="chrome120"
)

def extract_product_details(new_soup):
    """Help to Clean the table data"""
    
    product_table = new_soup.find("table", class_="a-normal a-spacing-micro")

    product_details = {}

    if product_table:
        rows = product_table.find_all("tr")
        
        for row in rows:
            cols = row.find_all("td")
            
            if len(cols)!= 2:
                continue
            
            key = cols[0].get_text(strip=True)
            value = cols[1].get_text(strip=True)
            
            product_details[key] = value
            
    return product_details

def extract_technical_details(new_soup):
    """More technical stuff about the laptop"""
    
    technical_table = new_soup.find(
        "table",
        id="productDetails_techSpec_section_1"
    )

    technical_details = {}

    if technical_table:
        rows = technical_table.find_all("tr")
        
        for row in rows:
            key_tag = row.find("th")
            value_tag = row.find("td")
            
            if not key_tag or not value_tag:
                continue
            
            key = key_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)
            
            technical_details[key] = value
            
    return technical_details


def fetch_url_data(url):
    """Fetch each url"""
    #url = extract_product_links(soup)
    new_response = requests.get(url, headers=HEADERS, timeout=20)
    new_soup = BeautifulSoup(new_response.text, "html.parser")
    
    return new_soup

def fetch_data(new_soup):
    """Fetch the data form the each page"""
    
    product_details= extract_product_details(new_soup)
    technical_details= extract_technical_details(new_soup)
   
    data = {}
    
    # --- Product title, Ratings ---
    data["product_title"] = safe_text(new_soup.find("span", attrs={'id':"productTitle"}))
    data["rating"] = safe_text(new_soup.find("span", class_="a-size-small a-color-base"))
    data["total_rating"] = safe_text(new_soup.find("span", class_="a-size-small", attrs={'id':"acrCustomerReviewText"}))
    
    # --- Pricing ----
    data["price"] = safe_text(new_soup.find("span", class_="a-price-whole"))
    
    data["next_month_bought"] = safe_text(new_soup.find(
        "span",
        id='social-proofing-faceout-title-tk_bought',
        class_="a-size-small social-proofing-faceout-title-text social-proofing-faceout-cx-enhancement-T2"
    ))
    
    # --- offer, mrp ---
    data["offer"] = safe_text(new_soup.find(
      "span"  ,
      class_="a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage"
    ))
    
    data["mrp"] = safe_text(new_soup.find(
      "span"  ,
      class_="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"
    ))
    
    # --- Product Details ---
    data["product_details_dict"] = product_details
    data["brand"] = product_details.get("Brand")
    data["model_name"]= product_details.get("Model Name")
    data["screen_size"]= product_details.get("Screen Size")
    data["colour"]= product_details.get("Colour")
    data["hard_disk_size"]= product_details.get("Hard Disk Size")
    data["cpu_model"]= product_details.get("CPU Model")
    data["ram"]= product_details.get("RAM Memory Installed Size")
    data["os"]= product_details.get("Operating System")
    data["special_features"]= product_details.get("Special Feature")
    data["graphics_card"]= product_details.get("Graphics Card Description")
    
    # --- about the item ---
    data["about"] = safe_text(new_soup.find("div", class_="a-section a-spacing-medium a-spacing-top-small"))
    data["technical_details_dict"] = technical_details
    
    return data

START_PAGE = 3
END_PAGE = 5

for page in range(START_PAGE, END_PAGE +1):
    logger.info(f"Scrapping page {page}")
    
    search_soup = get_search_page(page_no=page)
    count_page_cards(search_soup)
    product_links = extract_product_links(search_soup)
    
    page_data = []
    
    for idx, link in enumerate(product_links, start=1):
        logger.info(f"    Product: {idx}/{len(product_links)}")
        try:
            product_soup = fetch_url_data(link)
            data = fetch_data(product_soup)
            page_data.append(data)
            
            time.sleep(random.uniform(3, 10))
            
        except Exception as e:
            logger.error(f"    --->>> Error: {e}")
    
    # Save the files
    df = pd.DataFrame(page_data)
    filename = f"data/amazon_laptop_{page}.csv"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    logger.success(f"\n\n\t ----- Saved {filename} -----\n")

logger.success("\n\n\n ======= Scraping completed! ===== ")