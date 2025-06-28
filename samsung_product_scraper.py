#!/usr/bin/env python3
"""
Samsung UK Product Scraper

This script extracts product data from a list of pre-discovered Samsung UK product URLs.
It focuses purely on data extraction with improved selectors and error handling.

Usage:
    python3 -m pip install playwright aiohttp pydantic tenacity pandas pyarrow bs4 lxml
    playwright install
    python samsung_product_scraper.py --input product_urls.txt
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
        logging.FileHandler('product_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.samsung.com/uk"
USER_AGENT = "Samsung-UK-Product-Scraper/1.0 (contact: scraper@example.com)"

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
    description: Optional[str] = Field(None, description="Product description")
    model_code: Optional[str] = Field(None, description="Model code")
    brand: str = Field(default="Samsung", description="Brand name")
    timestamp_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('price_gbp', mode='before')
    @classmethod
    def parse_price(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            # Remove currency symbols and convert to float
            price_str = re.sub(r'[£$€,\s]', '', v)
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

class SamsungProductScraper:
    def __init__(self, concurrency: int = 8):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
        self.playwright = None
        self.browser = None
        
        # Statistics
        self.stats = {
            'total_urls': 0,
            'processed_urls': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'retries': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Results
        self.products: List[ProductSchema] = []
        self.failed_urls: List[str] = []

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=60, connect=15)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': USER_AGENT}
        )
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-web-security']
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

    def load_urls(self, filename: str) -> List[str]:
        """Load URLs from file"""
        urls = []
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    url = line.strip()
                    if url and url.startswith('http'):
                        urls.append(url)
            logger.info(f"Loaded {len(urls)} URLs from {filename}")
        except FileNotFoundError:
            logger.error(f"File not found: {filename}")
        except Exception as e:
            logger.error(f"Error loading URLs from {filename}: {e}")
        
        return urls

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_product_data(self, url: str) -> Optional[ProductSchema]:
        """Fetch and extract product data from a URL"""
        async with self.semaphore:
            try:
                # Try static fetch first
                product_data = await self._fetch_static(url)
                if product_data:
                    return product_data
                
                # Fall back to dynamic fetch
                product_data = await self._fetch_dynamic(url)
                return product_data
                
            except Exception as e:
                logger.debug(f"Failed to fetch {url}: {e}")
                self.stats['retries'] += 1
                raise

    async def _fetch_static(self, url: str) -> Optional[ProductSchema]:
        """Fetch product data using static HTTP request"""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.debug(f"HTTP {response.status} for {url}")
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Try JSON-LD first (most reliable)
                product_data = self._extract_from_json_ld(soup, url)
                if product_data:
                    return product_data
                
                # Fall back to CSS selectors
                return self._extract_from_css_selectors(soup, url)
                
        except Exception as e:
            logger.debug(f"Static fetch failed for {url}: {e}")
            return None

    async def _fetch_dynamic(self, url: str) -> Optional[ProductSchema]:
        """Fetch product data using Playwright for dynamic content"""
        try:
            page = await self.browser.new_page()
            
            # Intercept network responses for additional data
            api_data = {}
            async def handle_response(response):
                if ('json' in response.headers.get('content-type', '') and 
                    any(keyword in response.url for keyword in ['product', 'api', 'data'])):
                    try:
                        data = await response.json()
                        api_data.update(data)
                    except Exception:
                        pass
            
            page.on('response', handle_response)
            
            # Navigate and wait for content
            await page.goto(url, wait_until='networkidle', timeout=45000)
            
            # Wait for potential dynamic content loading
            await page.wait_for_timeout(2000)
            
            # Get final HTML
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try JSON-LD from dynamic content
            product_data = self._extract_from_json_ld(soup, url)
            if product_data:
                await page.close()
                return product_data
            
            # Try API data if available
            if api_data:
                product_data = self._extract_from_api_data(api_data, url)
                if product_data:
                    await page.close()
                    return product_data
            
            # Fall back to CSS selectors on dynamic content
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
                if not script.string:
                    continue
                    
                try:
                    data = json.loads(script.string)
                    
                    # Handle different JSON-LD structures
                    if isinstance(data, list):
                        for item in data:
                            if item.get('@type') == 'Product':
                                return self._create_product_from_json_ld(item, url)
                    elif data.get('@type') == 'Product':
                        return self._create_product_from_json_ld(data, url)
                    elif 'product' in data:
                        product_data = data['product']
                        if isinstance(product_data, dict):
                            return self._create_product_from_json_ld(product_data, url)
                        
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
            
            # Make image URL absolute
            if image and image.startswith('/'):
                image = BASE_URL + image
            
            # Extract category information
            category = data.get('category')
            sub_category = None
            if isinstance(category, list):
                category = category[0] if category else None
                sub_category = category[1] if len(category) > 1 else None
            
            # Extract description
            description = data.get('description', data.get('text', ''))
            if isinstance(description, list):
                description = ' '.join(description)
            
            product = ProductSchema(
                url=url,
                sku=data.get('sku', data.get('mpn', data.get('productID'))),
                name=data.get('name', ''),
                category=category,
                sub_category=sub_category,
                price_gbp=offers.get('price'),
                currency=offers.get('priceCurrency', 'GBP'),
                availability=offers.get('availability', '').replace('https://schema.org/', ''),
                image_url=image,
                description=description[:500] if description else None,  # Limit description length
                model_code=data.get('model', data.get('modelCode', data.get('mpn')))
            )
            
            return product
            
        except Exception as e:
            logger.debug(f"Error creating product from JSON-LD: {e}")
            return None

    def _extract_from_css_selectors(self, soup: BeautifulSoup, url: str) -> Optional[ProductSchema]:
        """Extract product data using CSS selectors as fallback"""
        try:
            # Samsung UK specific CSS selectors
            name_selectors = [
                'h1[data-testid="pdp-product-name"]',
                'h1.pdp-product-name',
                'h1.product-title',
                '.product-name h1',
                '.pdp-product-name',
                'h1[class*="product"]',
                'h1[id*="product"]'
            ]
            
            price_selectors = [
                '[data-testid="price-current"]',
                '.price-current',
                '.current-price',
                '.price .current',
                '.product-price .current',
                '.price-value',
                '[class*="price"][class*="current"]'
            ]
            
            image_selectors = [
                '.pdp-gallery img[src]',
                '.product-image img[src]',
                '.hero-image img[src]',
                '.product-gallery img[src]',
                '.main-image img[src]',
                '[data-testid="pdp-gallery"] img[src]'
            ]
            
            sku_selectors = [
                '[data-testid="model-code"]',
                '.model-code',
                '.product-sku',
                '.sku-value',
                '[class*="model-code"]',
                '[id*="model-code"]'
            ]
            
            description_selectors = [
                '.product-description',
                '.pdp-description',
                '.product-overview',
                '[data-testid="product-description"]',
                '.product-details p'
            ]
            
            availability_selectors = [
                '.availability-status',
                '.stock-status',
                '[data-testid="availability"]',
                '.product-availability'
            ]
            
            # Extract data using selectors
            name = self._extract_text_by_selectors(soup, name_selectors)
            price_text = self._extract_text_by_selectors(soup, price_selectors)
            image_url = self._extract_attr_by_selectors(soup, image_selectors, 'src')
            sku = self._extract_text_by_selectors(soup, sku_selectors)
            description = self._extract_text_by_selectors(soup, description_selectors)
            availability = self._extract_text_by_selectors(soup, availability_selectors)
            
            # If no name found, try title tag
            if not name:
                title_tag = soup.find('title')
                if title_tag:
                    name = title_tag.get_text(strip=True).split('|')[0].strip()
            
            if not name:
                logger.debug(f"No product name found for {url}")
                return None
            
            # Parse price
            price_gbp = None
            if price_text:
                # Remove non-numeric characters except decimal point
                price_cleaned = re.sub(r'[^\d.]', '', price_text)
                try:
                    price_gbp = float(price_cleaned) if price_cleaned else None
                except ValueError:
                    pass
            
            # Make image URL absolute
            if image_url and image_url.startswith('/'):
                image_url = BASE_URL + image_url
            
            # Extract category from URL
            category = self._extract_category_from_url(url)
            
            product = ProductSchema(
                url=url,
                sku=sku,
                name=name,
                category=category,
                price_gbp=price_gbp,
                currency='GBP',
                availability=availability,
                image_url=image_url,
                description=description[:500] if description else None,
                model_code=sku
            )
            
            return product
            
        except Exception as e:
            logger.debug(f"CSS selector extraction failed for {url}: {e}")
            return None

    def _extract_text_by_selectors(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Extract text using list of CSS selectors"""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text:
                        return text
            except Exception:
                continue
        return None

    def _extract_attr_by_selectors(self, soup: BeautifulSoup, selectors: List[str], attr: str) -> Optional[str]:
        """Extract attribute using list of CSS selectors"""
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element and element.get(attr):
                    return element[attr]
            except Exception:
                continue
        return None

    def _extract_category_from_url(self, url: str) -> Optional[str]:
        """Extract category from URL path"""
        try:
            from urllib.parse import urlparse
            path = urlparse(url).path.strip('/')
            parts = path.split('/')
            
            # Skip language/region parts
            if len(parts) > 0 and parts[0] == 'uk':
                parts = parts[1:]
            
            # Return first meaningful category
            if parts and parts[0] not in ['buy', 'product']:
                return parts[0].replace('-', ' ').title()
        except Exception:
            pass
        return None

    def _extract_from_api_data(self, api_data: dict, url: str) -> Optional[ProductSchema]:
        """Extract product data from intercepted API responses"""
        try:
            # This would need to be customized based on Samsung's actual API structure
            # For now, return None to fall back to other methods
            return None
        except Exception as e:
            logger.debug(f"API data extraction failed for {url}: {e}")
            return None

    async def save_products_ndjson(self, filename: str = 'products.ndjson'):
        """Save products to NDJSON file"""
        logger.info(f"Saving {len(self.products)} products to {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            for product in self.products:
                json_line = product.model_dump_json()
                f.write(json_line + '\n')

    async def save_products_parquet(self, filename: str = 'products.parquet'):
        """Convert products to Parquet format"""
        if not self.products:
            logger.warning("No products to save to Parquet")
            return
            
        logger.info(f"Converting {len(self.products)} products to Parquet format: {filename}")
        
        try:
            # Convert products to dict format
            products_dict = [product.model_dump() for product in self.products]
            df = pd.DataFrame(products_dict)
            df.to_parquet(filename, engine='pyarrow', index=False)
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
        print("PRODUCT EXTRACTION SUMMARY")
        print("="*60)
        print(f"Total URLs: {self.stats['total_urls']}")
        print(f"Processed URLs: {self.stats['processed_urls']}")
        print(f"Successful extractions: {self.stats['successful_extractions']}")
        print(f"Failed extractions: {self.stats['failed_extractions']}")
        print(f"Success rate: {(self.stats['successful_extractions']/max(self.stats['processed_urls'], 1)*100):.1f}%")
        print(f"Total retries: {self.stats['retries']}")
        print(f"Total runtime: {duration:.2f} seconds")
        print(f"Average time per URL: {duration/max(self.stats['processed_urls'], 1):.2f} seconds")
        
        # Category breakdown
        if self.products:
            categories = {}
            for product in self.products:
                cat = product.category or 'Unknown'
                categories[cat] = categories.get(cat, 0) + 1
            
            print("\nProducts by category:")
            for category, count in sorted(categories.items()):
                print(f"  {category}: {count}")
        
        print("="*60)

    async def run(self, input_file: str):
        """Main scraping orchestration"""
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("Starting Samsung UK product extraction...")
        
        # Load URLs
        urls = self.load_urls(input_file)
        if not urls:
            logger.error("No URLs to process")
            return
        
        self.stats['total_urls'] = len(urls)
        logger.info(f"Processing {len(urls)} product URLs...")
        
        # Process URLs with concurrency control
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def process_url(url):
            async with semaphore:
                try:
                    product = await self.fetch_product_data(url)
                    if product:
                        self.products.append(product)
                        self.stats['successful_extractions'] += 1
                        logger.debug(f"✓ Extracted: {product.name[:50]}...")
                    else:
                        self.failed_urls.append(url)
                        self.stats['failed_extractions'] += 1
                        logger.debug(f"✗ Failed: {url}")
                    
                    self.stats['processed_urls'] += 1
                    
                    # Progress logging
                    if self.stats['processed_urls'] % 50 == 0:
                        logger.info(f"Progress: {self.stats['processed_urls']}/{len(urls)} "
                                  f"({self.stats['successful_extractions']} successful)")
                    
                    # Rate limiting
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    self.failed_urls.append(url)
                    self.stats['failed_extractions'] += 1
                    logger.error(f"Error processing {url}: {e}")
        
        # Execute all tasks
        tasks = [process_url(url) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Save results
        await self.save_products_ndjson()
        await self.save_products_parquet()
        await self.save_failed_urls()
        
        self.stats['end_time'] = datetime.now(timezone.utc)
        self.print_summary()
        
        logger.info("Product extraction completed!")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Samsung UK Product Scraper')
    parser.add_argument('--input', default='product_urls.txt', 
                       help='Input file containing product URLs')
    parser.add_argument('--concurrency', type=int, default=8, 
                       help='Number of concurrent requests')
    args = parser.parse_args()
    
    # Override concurrency from environment variable if set
    concurrency = int(os.getenv('SCRAPER_CONCURRENCY', args.concurrency))
    
    async with SamsungProductScraper(concurrency=concurrency) as scraper:
        await scraper.run(args.input)

if __name__ == "__main__":
    asyncio.run(main())