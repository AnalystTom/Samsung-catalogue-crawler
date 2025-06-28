#!/usr/bin/env python3
"""
Samsung UK URL Discovery Script

This script systematically discovers all product URLs from Samsung UK's sitemap.
It separates URL discovery from data extraction for better performance and reliability.

Usage:
    python3 -m pip install aiohttp beautifulsoup4 playwright
    playwright install
    python samsung_url_discovery.py
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import List, Set, Dict, Optional
from urllib.parse import urljoin, urlparse
import argparse

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('url_discovery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.samsung.com/uk"
SITEMAP_URL = f"{BASE_URL}/info/sitemap/"
USER_AGENT = "Samsung-UK-URL-Discovery/1.0 (contact: scraper@example.com)"

class URLDiscovery:
    def __init__(self, concurrency: int = 5):
        self.concurrency = concurrency
        self.session: Optional[aiohttp.ClientSession] = None
        self.playwright = None
        self.browser = None
        
        # URL tracking
        self.category_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.url_metadata: Dict[str, Dict] = {}
        
        # Statistics
        self.stats = {
            'categories_discovered': 0,
            'products_discovered': 0,
            'pages_processed': 0,
            'start_time': None,
            'end_time': None
        }

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': USER_AGENT}
        )
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
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

    def is_samsung_uk_url(self, url: str) -> bool:
        """Check if URL is a Samsung UK URL"""
        return 'samsung.com/uk' in url and not any(excluded in url for excluded in [
            '/info/', '/support/', '/business/', '/offer/', '/estore/', 
            '/legal/', '/privacy/', '/sustainability/', '/mypage/', 
            '/members/', '/account/', '/login', '/register', '/cart'
        ])

    def is_product_category_url(self, url: str) -> bool:
        """Check if URL is a product category page"""
        category_patterns = [
            # Mobile & Wearables
            r'/smartphones/',
            r'/tablets/',
            r'/watches/',
            r'/computers/',
            r'/audio-sound/',
            r'/mobile-accessories/',
            
            # TV & AV
            r'/tvs/',
            r'/neo-qled-tvs/',
            r'/oled-tvs/',
            r'/qled-tv/',
            r'/lifestyle-tvs/',
            r'/audio-devices/',
            
            # Home Appliances
            r'/refrigerators/',
            r'/washers-and-dryers/',
            r'/dishwashers/',
            r'/cooking-appliances/',
            r'/microwave-ovens/',
            r'/vacuum-cleaners/',
            
            # Computing & Storage
            r'/monitors/',
            r'/memory-storage/',
            
            # Business
            r'/business/projectors/',
            r'/projectors/',
            
            # Generic patterns
            r'/all-',
            r'/category/',
            r'/products/'
        ]
        
        return any(re.search(pattern, url) for pattern in category_patterns)

    def is_product_detail_url(self, url: str) -> bool:
        """Check if URL is a product detail page"""
        # Samsung UK product detail patterns - exclude /buy/ URLs
        product_patterns = [
            # Galaxy device patterns
            r'/galaxy-[^/]+/$',  # Galaxy product pages (not /buy/)
            r'/galaxy-watch[^/]*-[^/]+-[^/]+/$',  # Galaxy Watch specific patterns
            r'/galaxy-fit[^/]*-[^/]+-[^/]+/$',  # Galaxy Fit specific patterns
            r'/galaxy-ring[^/]*-[^/]+-[^/]+/$',  # Galaxy Ring specific patterns
            r'/galaxy-buds[^/]*-[^/]+-[^/]+/$',  # Galaxy Buds specific patterns
            r'/galaxy-tab[^/]*-[^/]+-[^/]+/$',  # Galaxy Tab specific patterns
            r'/galaxy-book[^/]*-[^/]+-[^/]+/$',  # Galaxy Book specific patterns
            
            # TV model patterns 
            r'/qe\d+[a-z]+\d+[a-z]+/',  # QLED/OLED model codes (e.g., qe65s95fatxxu)
            r'/ls\d+[a-z]+\d+[a-z]+/',  # Lifestyle TV codes (e.g., ls03fw)
            r'/the-frame[^/]*-[^/]+-[^/]+/',  # The Frame TV models
            r'/the-serif[^/]*-[^/]+-[^/]+/',  # The Serif TV models
            r'/the-terrace[^/]*-[^/]+-[^/]+/',  # The Terrace TV models
            r'/the-sero[^/]*-[^/]+-[^/]+/',  # The Sero TV models
            
            # Audio device patterns
            r'/hw-[a-z0-9]+/',  # Audio device codes (e.g., hw-q990d-xu)
            r'/q\d+[a-z]+-[^/]+-[^/]+/',  # Soundbar models (e.g., q990d-q-series)
            r'/s\d+[a-z]+-[^/]+-[^/]+/',  # Sound device models
            
            # Appliance model patterns
            r'/ww\d+[a-z]+\d+[a-z]+/',  # Washer model codes (e.g., ww11db8b95gbu1)
            r'/rl\d+[a-z]+\d+[a-z]+/',  # Refrigerator codes (e.g., rl38c776asr)
            r'/vs\d+[a-z]+\d+[a-z]+/',  # Vacuum cleaner codes
            r'/bespoke-[^/]+-[^/]+/',  # Bespoke appliance models
            
            # Samsung device prefixes
            r'/sm-[a-z0-9]+-[^/]+/',  # Galaxy devices (SM- prefix)
            r'/np\d+[a-z]+-[^/]+/',  # Galaxy Book codes (NP prefix)
            
            # Monitor patterns
            r'/odyssey-[^/]+-[^/]+-[^/]+/',  # Gaming monitors
            r'/viewfinity-[^/]+-[^/]+-[^/]+/',  # Professional monitors
            r'/ls\d+[a-z]+\d+[a-z]+/',  # Monitor model codes
            
            # General product model patterns
            r'/[^/]+/[^/]+-[^/]+-[^/]+/$',  # Product model pages
            r'/[^/]+/[^/]+-\w{2}-\w{2,4}/$',  # Product with model codes
            r'/[^/]+/[^/]+-\w{10,}/$',  # Product with long model codes
        ]
        
        # Exclude category, listing pages, and buy/configuration pages
        exclude_patterns = [
            r'/buy/$',  # URLs ending with /buy/
            r'/buy/\?',  # URLs with /buy/?parameters
            r'/all-',
            r'/category/',
            r'/products/$',
            r'/help-me-choose/',
            r'/highlights/',
            r'/buying-guide/',
            r'/learn/',
            r'/compare/',
            r'/accessories/$'
        ]
        
        is_product = any(re.search(pattern, url) for pattern in product_patterns)
        is_excluded = any(re.search(pattern, url) for pattern in exclude_patterns)
        
        return is_product and not is_excluded

    def extract_category_from_url(self, url: str) -> Optional[str]:
        """Extract category from URL path"""
        path = urlparse(url).path.strip('/')
        parts = path.split('/')
        
        # Skip language/region parts
        if len(parts) > 0 and parts[0] == 'uk':
            parts = parts[1:]
        
        # Return first meaningful category
        if parts:
            return parts[0]
        return None

    async def discover_sitemap_categories(self) -> Set[str]:
        """Discover category URLs from Samsung UK sitemap"""
        logger.info("Discovering categories from sitemap...")
        category_urls = set()
        
        try:
            async with self.session.get(SITEMAP_URL) as response:
                if response.status != 200:
                    logger.warning(f"Sitemap fetch failed: {response.status}")
                    return category_urls
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find all links in the sitemap
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/'):
                        full_url = urljoin(BASE_URL, href)
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue
                    
                    # Filter for Samsung UK product category URLs
                    if (self.is_samsung_uk_url(full_url) and 
                        self.is_product_category_url(full_url)):
                        category_urls.add(full_url)
                        logger.debug(f"Discovered category: {full_url}")
        
        except Exception as e:
            logger.error(f"Error discovering categories from sitemap: {e}")
        
        # Add fallback category URLs
        fallback_categories = [
            f"{BASE_URL}/smartphones/all-smartphones/",
            f"{BASE_URL}/tablets/all-tablets/",
            f"{BASE_URL}/watches/all-watches/",
            f"{BASE_URL}/tvs/all-tvs/",
            f"{BASE_URL}/monitors/all-monitors/",
            f"{BASE_URL}/audio-sound/all-audio-sound/",
            f"{BASE_URL}/refrigerators/all-refrigerators/",
            f"{BASE_URL}/washers-and-dryers/all-washers-and-dryers/",
            f"{BASE_URL}/vacuum-cleaners/all-vacuum-cleaners/",
            f"{BASE_URL}/dishwashers/all-dishwashers/",
            f"{BASE_URL}/microwave-ovens/all-microwave-ovens/",
            f"{BASE_URL}/computers/all-computers/",
            f"{BASE_URL}/projectors/all-projectors/"
        ]
        
        for fallback in fallback_categories:
            category_urls.add(fallback)
            logger.debug(f"Added fallback category: {fallback}")
        
        logger.info(f"Discovered {len(category_urls)} category URLs")
        return category_urls

    async def discover_products_from_category(self, category_url: str) -> Set[str]:
        """Discover product URLs from a category page"""
        product_urls = set()
        category = self.extract_category_from_url(category_url)
        
        try:
            # Try static scraping first
            async with self.session.get(category_url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find product links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if href.startswith('/'):
                            full_url = urljoin(BASE_URL, href)
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue
                        
                        if (self.is_samsung_uk_url(full_url) and 
                            self.is_product_detail_url(full_url)):
                            product_urls.add(full_url)
                            
                            # Store metadata
                            self.url_metadata[full_url] = {
                                'category': category,
                                'source_category_url': category_url,
                                'discovered_at': datetime.now(timezone.utc).isoformat()
                            }
            
            # If few products found, try dynamic scraping
            if len(product_urls) < 5:
                dynamic_urls = await self.discover_products_dynamic(category_url, category)
                product_urls.update(dynamic_urls)
        
        except Exception as e:
            logger.error(f"Error discovering products from {category_url}: {e}")
        
        logger.info(f"Found {len(product_urls)} products in category: {category}")
        return product_urls

    async def discover_products_dynamic(self, category_url: str, category: str) -> Set[str]:
        """Use Playwright to discover products from dynamic content"""
        product_urls = set()
        
        try:
            page = await self.browser.new_page()
            
            # Increased timeout for heavy category pages
            await page.goto(category_url, wait_until='networkidle', timeout=45000)
            
            # Wait for initial content to load
            await page.wait_for_timeout(3000)
            
            # Scroll to trigger lazy loading
            await self._scroll_to_load_content(page)
            
            # Handle filter parameters for specific categories
            if 'crystal-uhd' in category_url:
                # Handle filtered TV category
                await page.wait_for_timeout(2000)
            
            # Handle pagination and "Load more" buttons
            load_attempts = 0
            while load_attempts < 15:  # Increased from 10 to 15
                # Enhanced load more button selectors
                load_more_selectors = [
                    'button:has-text("Load more")',
                    'button:has-text("Show more")',
                    'button:has-text("View more")',
                    'button:has-text("See more")',
                    '.load-more',
                    '.show-more',
                    '.view-more',
                    '[data-testid="load-more"]',
                    '[class*="load-more"]',
                    '[class*="show-more"]',
                    'button[class*="pagination"]'
                ]
                
                button_found = False
                for selector in load_more_selectors:
                    try:
                        button = await page.query_selector(selector)
                        if button and await button.is_visible():
                            await button.click()
                            await page.wait_for_timeout(3000)  # Increased wait time
                            # Scroll again after loading more content
                            await self._scroll_to_load_content(page)
                            button_found = True
                            break
                    except Exception:
                        continue
                
                if not button_found:
                    break
                
                load_attempts += 1
            
            # Extract all links from the page
            links = await page.query_selector_all('a[href]')
            for link in links:
                href = await link.get_attribute('href')
                if not href:
                    continue
                
                if href.startswith('/'):
                    full_url = urljoin(BASE_URL, href)
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                if (self.is_samsung_uk_url(full_url) and 
                    self.is_product_detail_url(full_url)):
                    product_urls.add(full_url)
                    
                    # Store metadata
                    self.url_metadata[full_url] = {
                        'category': category,
                        'source_category_url': category_url,
                        'discovered_at': datetime.now(timezone.utc).isoformat(),
                        'dynamic_discovery': True
                    }
            
            await page.close()
        
        except Exception as e:
            logger.error(f"Error in dynamic discovery for {category_url}: {e}")
        
        return product_urls

    async def _scroll_to_load_content(self, page):
        """Scroll down the page to trigger lazy loading of products"""
        try:
            # Get page height
            height = await page.evaluate('document.body.scrollHeight')
            
            # Scroll in steps to trigger lazy loading
            scroll_steps = 5
            step_size = height // scroll_steps
            
            for i in range(scroll_steps):
                scroll_position = step_size * (i + 1)
                await page.evaluate(f'window.scrollTo(0, {scroll_position})')
                await page.wait_for_timeout(1000)  # Wait for content to load
            
            # Scroll back to top
            await page.evaluate('window.scrollTo(0, 0)')
            await page.wait_for_timeout(1000)
            
        except Exception as e:
            logger.debug(f"Error during scrolling: {e}")

    async def validate_product_urls(self, urls: Set[str]) -> Set[str]:
        """Filter URLs using pattern matching (skip HTTP validation due to bot detection)"""
        valid_urls = set()
        
        logger.info(f"Filtering {len(urls)} product URLs using pattern matching...")
        
        for url in urls:
            # Apply additional filtering beyond the initial product detection
            if self.is_product_detail_url(url):
                # Additional quality checks
                parsed = urlparse(url)
                path_parts = parsed.path.strip('/').split('/')
                
                # Ensure URL has meaningful depth (not just category pages)
                if len(path_parts) >= 2:
                    # Check for model codes or specific product identifiers
                    has_product_identifier = any(
                        len(part) > 10 or  # Long identifiers (model codes)
                        re.search(r'-[a-z]{2,3}[\d-]+', part) or  # Model pattern like -sm-r630
                        part.startswith(('sm-', 'qe', 'ls', 'np'))  # Samsung model prefixes
                        for part in path_parts
                    )
                    
                    if has_product_identifier or len(path_parts) >= 3:
                        valid_urls.add(url)
                        logger.debug(f"Pattern-validated: {url}")
                    else:
                        logger.debug(f"No product identifier found: {url}")
                else:
                    logger.debug(f"URL too shallow: {url}")
            else:
                logger.debug(f"Failed product pattern check: {url}")
        
        logger.info(f"Pattern-filtered {len(valid_urls)} out of {len(urls)} URLs")
        return valid_urls

    async def save_urls(self, filename: str = 'product_urls.txt'):
        """Save discovered product URLs to file"""
        logger.info(f"Saving {len(self.product_urls)} product URLs to {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            for url in sorted(self.product_urls):
                f.write(url + '\n')

    async def save_metadata(self, filename: str = 'url_metadata.json'):
        """Save URL metadata to JSON file"""
        logger.info(f"Saving metadata for {len(self.url_metadata)} URLs to {filename}")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.url_metadata, f, indent=2, ensure_ascii=False)

    def print_summary(self):
        """Print discovery summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() if self.stats['end_time'] and self.stats['start_time'] else 0
        
        print("\n" + "="*60)
        print("URL DISCOVERY SUMMARY")
        print("="*60)
        print(f"Categories discovered: {self.stats['categories_discovered']}")
        print(f"Product URLs discovered: {self.stats['products_discovered']}")
        print(f"Pages processed: {self.stats['pages_processed']}")
        print(f"Total runtime: {duration:.2f} seconds")
        
        # Category breakdown
        categories = {}
        for url, metadata in self.url_metadata.items():
            cat = metadata.get('category', 'unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        print("\nCategory breakdown:")
        for category, count in sorted(categories.items()):
            print(f"  {category}: {count} products")
        
        print("="*60)

    async def run(self):
        """Main URL discovery orchestration"""
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("Starting Samsung UK URL discovery...")
        
        # Discover category URLs
        category_urls = await self.discover_sitemap_categories()
        self.category_urls = category_urls
        self.stats['categories_discovered'] = len(category_urls)
        
        # Discover product URLs from categories
        all_product_urls = set()
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def process_category(category_url):
            async with semaphore:
                product_urls = await self.discover_products_from_category(category_url)
                all_product_urls.update(product_urls)
                self.stats['pages_processed'] += 1
                # Small delay to be respectful
                await asyncio.sleep(1)
        
        # Process categories concurrently
        tasks = [process_category(url) for url in category_urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Validate product URLs
        self.product_urls = await self.validate_product_urls(all_product_urls)
        self.stats['products_discovered'] = len(self.product_urls)
        
        # Save results
        await self.save_urls()
        await self.save_metadata()
        
        self.stats['end_time'] = datetime.now(timezone.utc)
        self.print_summary()
        
        logger.info("URL discovery completed successfully!")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Samsung UK URL Discovery')
    parser.add_argument('--concurrency', type=int, default=5, help='Number of concurrent requests')
    args = parser.parse_args()
    
    async with URLDiscovery(concurrency=args.concurrency) as discovery:
        await discovery.run()

if __name__ == "__main__":
    asyncio.run(main())