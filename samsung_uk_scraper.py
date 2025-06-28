#!/usr/bin/env python3
"""
Samsung UK Product Scraper

A comprehensive scraper for Samsung UK's product catalog that:
- Discovers products via sitemap parsing + hardcoded fallback URLs
- Extracts data using JSON-LD (primary) + CSS selectors (fallback)
- Validates output with Pydantic models
- Handles failures with retry logic and failed URL logging
- Manages concurrency with rate limiting
- Outputs data as NDJSON + Parquet with comprehensive logging

Usage:
    python3 -m pip install playwright aiohttp pydantic tenacity pandas pyarrow bs4 lxml
    playwright install
    python samsung_uk_scraper.py
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional, Set
import argparse
import os

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.samsung.com/uk"
SITEMAP_URL = f"{BASE_URL}/info/sitemap/"
USER_AGENT = "Samsung-UK-Scraper/1.0 (contact: scraper@example.com)"

# Fallback category URLs
FALLBACK_URLS = [
    "/smartphones/all-smartphones/",
    "/tablets/all-tablets/",
    "/watches/all-watches/",
    "/tvs/all-tvs/",
    "/monitors/all-monitors/",
    "/audio-sound/all-audio-sound/",
    "/refrigerators/all-refrigerators/",
    "/washers-and-dryers/all-washers-and-dryers/",
    "/vacuum-cleaners/all-vacuum-cleaners/"
]

class ProductSchema(BaseModel):
    """Pydantic model for product data validation"""
    url: str = Field(..., description="Product URL")
    sku: Optional[str] = Field(None, description="Product SKU")
    name: str = Field(..., description="Product name")
    category: Optional[str] = Field(None, description="Main category")
    sub_category: Optional[str] = Field(None, description="Sub category")
    price_gbp: Optional[float] = Field(None, description="Price in GBP")
    currency: Optional[str] = Field(None, description="Currency code")
    availability: Optional[str] = Field(None, description="Availability status")
    image_url: Optional[str] = Field(None, description="Product image URL")
    timestamp_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('price_gbp', mode='before')
    @classmethod
    def parse_price(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            # Remove currency symbols and convert to float
            price_str = re.sub(r'[£$€,]', '', v)
            try:
                return float(price_str)
            except ValueError:
                return None
        return float(v) if v else None

    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        if not v.startswith('http'):
            raise ValueError('URL must be absolute')
        return v

class SamsungUKScraper:
    def __init__(self, concurrency: int = 10):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
        self.playwright = None
        self.browser = None
        
        # Statistics
        self.stats = {
            'discovered_urls': 0,
            'processed_urls': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'retries': 0,
            'start_time': None,
            'end_time': None
        }
        
        # URL tracking
        self.discovered_urls: Set[str] = set()
        self.failed_urls: List[str] = []
        self.products: List[ProductSchema] = []

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': USER_AGENT}
        )
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--lang=en-GB', '--no-sandbox', '--disable-dev-shm-usage']
        )
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def get_robots_delay(self) -> float:
        """Get crawl delay from robots.txt"""
        try:
            async with self.session.get(f"{BASE_URL}/robots.txt") as response:
                if response.status == 200:
                    content = await response.text()
                    for line in content.split('\n'):
                        if 'crawl-delay' in line.lower():
                            delay = re.search(r'crawl-delay:\s*(\d+)', line.lower())
                            if delay:
                                return float(delay.group(1))
        except Exception as e:
            logger.debug(f"Could not fetch robots.txt: {e}")
        
        return 1.0  # Default 1 second delay

    async def discover_urls_from_sitemap(self) -> Set[str]:
        """Discover product URLs from Samsung UK sitemap"""
        logger.info("Discovering URLs from sitemap...")
        urls = set()
        
        try:
            async with self.session.get(SITEMAP_URL) as response:
                if response.status != 200:
                    logger.warning(f"Sitemap fetch failed: {response.status}")
                    return urls
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find all links under "Shop" section
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/'):
                        full_url = BASE_URL + href
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue
                    
                    # Filter for Samsung UK shop URLs
                    if 'samsung.com/uk' in full_url and self._is_product_category_url(full_url):
                        urls.add(full_url)
                        logger.debug(f"Discovered category URL: {full_url}")
        
        except Exception as e:
            logger.error(f"Error discovering URLs from sitemap: {e}")
        
        # Add fallback URLs
        for fallback in FALLBACK_URLS:
            full_url = BASE_URL + fallback
            urls.add(full_url)
            logger.debug(f"Added fallback URL: {full_url}")
        
        logger.info(f"Discovered {len(urls)} category URLs")
        return urls

    def _is_product_category_url(self, url: str) -> bool:
        """Check if URL is a product category or listing page"""
        category_patterns = [
            r'/smartphones/',
            r'/tablets/',
            r'/watches/',
            r'/tvs/',
            r'/monitors/',
            r'/audio-sound/',
            r'/refrigerators/',
            r'/washers-and-dryers/',
            r'/vacuum-cleaners/',
            r'/all-',
            r'/category/',
            r'/products/'
        ]
        
        return any(re.search(pattern, url) for pattern in category_patterns)

    async def discover_product_urls_from_category(self, category_url: str) -> Set[str]:
        """Discover individual product URLs from a category page"""
        product_urls = set()
        
        try:
            # Try static first
            async with self.session.get(category_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for product links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if self._is_product_detail_url(href):
                            if href.startswith('/'):
                                full_url = BASE_URL + href
                            else:
                                full_url = href
                            product_urls.add(full_url)
            
            # If few products found, try dynamic scraping
            if len(product_urls) < 5:
                dynamic_urls = await self._discover_urls_dynamic(category_url)
                product_urls.update(dynamic_urls)
                
        except Exception as e:
            logger.error(f"Error discovering products from {category_url}: {e}")
        
        logger.info(f"Found {len(product_urls)} products in {category_url}")
        return product_urls

    def _is_product_detail_url(self, url: str) -> bool:
        """Check if URL is a product detail page"""
        # Samsung product URLs typically have model numbers or specific patterns
        product_patterns = [
            r'/galaxy-',
            r'/buy-',
            r'/\w+/[\w-]+/?$',  # Generic product pattern
            r'/p/',  # Some sites use /p/ for products
        ]
        
        return any(re.search(pattern, url) for pattern in product_patterns)

    async def _discover_urls_dynamic(self, url: str) -> Set[str]:
        """Use Playwright to discover URLs from dynamic content"""
        product_urls = set()
        
        try:
            page = await self.browser.new_page()
            await page.goto(url, wait_until='networkidle')
            
            # Handle "Load more" buttons
            load_more_attempts = 0
            while load_more_attempts < 10:  # Prevent infinite loops
                load_more_button = await page.query_selector('button:has-text("Load more"), button:has-text("Show more"), .load-more')
                if load_more_button:
                    await load_more_button.click()
                    await page.wait_for_timeout(2000)
                    load_more_attempts += 1
                else:
                    break
            
            # Extract product links
            links = await page.query_selector_all('a[href]')
            for link in links:
                href = await link.get_attribute('href')
                if href and self._is_product_detail_url(href):
                    if href.startswith('/'):
                        full_url = BASE_URL + href
                    else:
                        full_url = href
                    product_urls.add(full_url)
            
            await page.close()
            
        except Exception as e:
            logger.error(f"Error in dynamic URL discovery for {url}: {e}")
        
        return product_urls

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_product_data(self, url: str) -> Optional[ProductSchema]:
        """Fetch and extract product data from a URL"""
        async with self.semaphore:
            try:
                # Layer 1: Try static fetch first
                product_data = await self._fetch_static(url)
                if product_data:
                    return product_data
                
                # Layer 2: Try dynamic fetch
                product_data = await self._fetch_dynamic(url)
                return product_data
                
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                self.stats['retries'] += 1
                raise

    async def _fetch_static(self, url: str) -> Optional[ProductSchema]:
        """Fetch product data using static HTTP request"""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Try JSON-LD first
                product_data = self._extract_from_json_ld(soup, url)
                if product_data:
                    return product_data
                
                # Fallback to CSS selectors
                return self._extract_from_css_selectors(soup, url)
                
        except Exception as e:
            logger.debug(f"Static fetch failed for {url}: {e}")
            return None

    async def _fetch_dynamic(self, url: str) -> Optional[ProductSchema]:
        """Fetch product data using Playwright"""
        try:
            page = await self.browser.new_page()
            
            # Intercept JSON responses
            json_data = {}
            async def handle_response(response):
                if 'json' in response.headers.get('content-type', ''):
                    try:
                        data = await response.json()
                        json_data.update(data)
                    except:
                        pass
            
            page.on('response', handle_response)
            
            await page.goto(url, wait_until='networkidle')
            
            # Extract data
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try JSON-LD first
            product_data = self._extract_from_json_ld(soup, url)
            if product_data:
                await page.close()
                return product_data
            
            # Try intercepted JSON
            if json_data:
                product_data = self._extract_from_intercepted_json(json_data, url)
                if product_data:
                    await page.close()
                    return product_data
            
            # Fallback to CSS selectors
            product_data = self._extract_from_css_selectors(soup, url)
            await page.close()
            return product_data
            
        except Exception as e:
            logger.debug(f"Dynamic fetch failed for {url}: {e}")
            return None

    def _extract_from_json_ld(self, soup: BeautifulSoup, url: str) -> Optional[ProductSchema]:
        """Extract product data from JSON-LD structured data"""
        try:
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    
                    # Handle different JSON-LD structures
                    if isinstance(data, list):
                        data = data[0]
                    
                    if data.get('@type') == 'Product':
                        return self._create_product_from_json_ld(data, url)
                    
                    # Check for nested products
                    if 'product' in data:
                        return self._create_product_from_json_ld(data['product'], url)
                        
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"JSON-LD parsing error for {url}: {e}")
                    continue
            
        except Exception as e:
            logger.debug(f"JSON-LD extraction failed for {url}: {e}")
        
        return None

    def _create_product_from_json_ld(self, data: dict, url: str) -> Optional[ProductSchema]:
        """Create ProductSchema from JSON-LD data"""
        try:
            # Extract offers data
            offers = data.get('offers', {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            
            # Extract image
            image = data.get('image')
            if isinstance(image, list):
                image = image[0] if image else None
            if isinstance(image, dict):
                image = image.get('url', image.get('@id'))
            
            # Extract category
            category = data.get('category')
            sub_category = None
            if isinstance(category, list):
                category = category[0] if category else None
                sub_category = category[1] if len(category) > 1 else None
            
            product = ProductSchema(
                url=url,
                sku=data.get('sku', data.get('mpn', data.get('productID'))),
                name=data.get('name', ''),
                category=category,
                sub_category=sub_category,
                price_gbp=offers.get('price'),
                currency=offers.get('priceCurrency', 'GBP'),
                availability=offers.get('availability', '').replace('https://schema.org/', ''),
                image_url=image
            )
            
            return product
            
        except Exception as e:
            logger.debug(f"Error creating product from JSON-LD: {e}")
            return None

    def _extract_from_css_selectors(self, soup: BeautifulSoup, url: str) -> Optional[ProductSchema]:
        """Extract product data using CSS selectors as fallback"""
        try:
            # Common Samsung UK CSS selectors
            name_selectors = [
                'h1.product-title',
                'h1.pdp-product-name',
                '.product-name h1',
                'h1[data-test="product-name"]',
                '.pdp-product-name'
            ]
            
            price_selectors = [
                '.price-current',
                '.price .current',
                '[data-test="price"]',
                '.product-price .current',
                '.price-value'
            ]
            
            image_selectors = [
                '.product-image img',
                '.hero-image img',
                '.pdp-image img',
                '.product-gallery img'
            ]
            
            sku_selectors = [
                '[data-test="model-code"]',
                '.model-code',
                '.product-sku',
                '.sku-value'
            ]
            
            # Extract data
            name = self._extract_text_by_selectors(soup, name_selectors)
            price_text = self._extract_text_by_selectors(soup, price_selectors)
            image_url = self._extract_attr_by_selectors(soup, image_selectors, 'src')
            sku = self._extract_text_by_selectors(soup, sku_selectors)
            
            if not name:
                return None
            
            # Parse price
            price_gbp = None
            if price_text:
                price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                if price_match:
                    price_gbp = float(price_match.group())
            
            # Make image URL absolute
            if image_url and image_url.startswith('/'):
                image_url = BASE_URL + image_url
            
            product = ProductSchema(
                url=url,
                sku=sku,
                name=name,
                price_gbp=price_gbp,
                currency='GBP',
                image_url=image_url
            )
            
            return product
            
        except Exception as e:
            logger.debug(f"CSS selector extraction failed for {url}: {e}")
            return None

    def _extract_text_by_selectors(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Extract text using list of CSS selectors"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return None

    def _extract_attr_by_selectors(self, soup: BeautifulSoup, selectors: List[str], attr: str) -> Optional[str]:
        """Extract attribute using list of CSS selectors"""
        for selector in selectors:
            element = soup.select_one(selector)
            if element and element.get(attr):
                return element[attr]
        return None

    def _extract_from_intercepted_json(self, json_data: dict, url: str) -> Optional[ProductSchema]:
        """Extract product data from intercepted JSON responses"""
        try:
            # This would need to be customized based on Samsung's actual API responses
            # For now, return None to fall back to other methods
            return None
        except Exception as e:
            logger.debug(f"Intercepted JSON extraction failed for {url}: {e}")
            return None

    async def save_products_ndjson(self, filename: str = 'products.ndjson'):
        """Save products to NDJSON file"""
        logger.info(f"Saving {len(self.products)} products to {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            for product in self.products:
                json_line = product.model_dump_json()
                f.write(json_line + '\n')

    async def save_products_parquet(self, filename: str = 'products.parquet'):
        """Convert NDJSON to Parquet format"""
        logger.info(f"Converting to Parquet format: {filename}")
        
        try:
            # Read NDJSON and convert to DataFrame
            df = pd.read_json('products.ndjson', lines=True)
            df.to_parquet(filename, engine='pyarrow')
            logger.info(f"Parquet file saved: {filename}")
        except Exception as e:
            logger.error(f"Error saving Parquet file: {e}")

    async def save_failed_urls(self, filename: str = 'failed_urls.txt'):
        """Save failed URLs to file"""
        if self.failed_urls:
            logger.info(f"Saving {len(self.failed_urls)} failed URLs to {filename}")
            with open(filename, 'w') as f:
                for url in self.failed_urls:
                    f.write(url + '\n')

    def print_summary(self):
        """Print scraping summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() if self.stats['end_time'] and self.stats['start_time'] else 0
        
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total URLs discovered: {self.stats['discovered_urls']}")
        print(f"Total URLs processed: {self.stats['processed_urls']}")
        print(f"Successful extractions: {self.stats['successful_extractions']}")
        print(f"Failed extractions: {self.stats['failed_extractions']}")
        print(f"Total retries: {self.stats['retries']}")
        print(f"Total runtime: {duration:.2f} seconds")
        print(f"Average time per URL: {duration/max(self.stats['processed_urls'], 1):.2f} seconds")
        print("="*60)

    async def run(self):
        """Main scraping orchestration"""
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("Starting Samsung UK product scraping...")
        
        # Get crawl delay
        crawl_delay = await self.get_robots_delay()
        logger.info(f"Using crawl delay: {crawl_delay} seconds")
        
        # Discover category URLs
        category_urls = await self.discover_urls_from_sitemap()
        
        # Discover product URLs from all categories
        all_product_urls = set()
        for category_url in category_urls:
            product_urls = await self.discover_product_urls_from_category(category_url)
            all_product_urls.update(product_urls)
            await asyncio.sleep(crawl_delay)  # Respect rate limit
        
        self.stats['discovered_urls'] = len(all_product_urls)
        logger.info(f"Total product URLs discovered: {len(all_product_urls)}")
        
        # Process products in batches
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def process_url(url):
            async with semaphore:
                try:
                    product = await self.fetch_product_data(url)
                    if product:
                        self.products.append(product)
                        self.stats['successful_extractions'] += 1
                        logger.debug(f"Successfully extracted: {product.name}")
                    else:
                        self.failed_urls.append(url)
                        self.stats['failed_extractions'] += 1
                        logger.debug(f"Failed to extract: {url}")
                    
                    self.stats['processed_urls'] += 1
                    
                    # Rate limiting
                    await asyncio.sleep(crawl_delay)
                    
                except Exception as e:
                    self.failed_urls.append(url)
                    self.stats['failed_extractions'] += 1
                    logger.error(f"Error processing {url}: {e}")
        
        # Execute all tasks
        tasks = [process_url(url) for url in all_product_urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results
        await self.save_products_ndjson()
        await self.save_products_parquet()
        await self.save_failed_urls()
        
        self.stats['end_time'] = datetime.now(timezone.utc)
        self.print_summary()
        
        logger.info("Scraping completed successfully!")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Samsung UK Product Scraper')
    parser.add_argument('--concurrency', type=int, default=10, help='Number of concurrent requests')
    args = parser.parse_args()
    
    # Override concurrency from environment variable if set
    concurrency = int(os.getenv('SCRAPER_CONCURRENCY', args.concurrency))
    
    async with SamsungUKScraper(concurrency=concurrency) as scraper:
        await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())