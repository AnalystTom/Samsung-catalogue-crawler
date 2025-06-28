#!/usr/bin/env python3
"""
Samsung UK Product URL Expansion Script

This script takes category listing URLs and expands them by crawling each page
to extract all individual product URLs. This ensures comprehensive coverage
of Samsung's product catalog.

Usage:
    python expand_product_urls.py --input product_urls.txt --output expanded_product_urls.txt
"""

import asyncio
import json
import logging
import re
import argparse
from datetime import datetime, timezone
from typing import List, Set, Dict, Optional
from urllib.parse import urljoin, urlparse
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('url_expansion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.samsung.com/uk"
USER_AGENT = "Samsung-UK-URL-Expansion/1.0 (contact: scraper@example.com)"

class ProductURLExpander:
    def __init__(self, concurrency: int = 3):
        self.concurrency = concurrency
        self.session: Optional[aiohttp.ClientSession] = None
        self.playwright = None
        self.browser = None
        
        # URL tracking
        self.input_urls: Set[str] = set()
        self.category_urls: Set[str] = set()
        self.individual_urls: Set[str] = set()
        self.expanded_urls: Set[str] = set()
        
        # Metadata tracking
        self.expansion_metadata: Dict[str, Dict] = {}
        
        # Stats
        self.stats = {
            'start_time': None,
            'end_time': None,
            'input_urls_count': 0,
            'category_urls_identified': 0,
            'individual_urls_found': 0,
            'total_expanded_urls': 0,
            'pages_processed': 0
        }

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"User-Agent": USER_AGENT}
        
        self.session = aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout, 
            headers=headers
        )
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    def is_samsung_uk_url(self, url: str) -> bool:
        """Check if URL belongs to Samsung UK"""
        return url.startswith(BASE_URL) and '/uk/' in url

    def is_category_listing_url(self, url: str) -> bool:
        """Check if URL is a category listing page that should be expanded"""
        category_listing_patterns = [
            r'/all-[^/]+/?$',  # URLs like /all-watches/, /all-smartphones/
            r'/(smartphones|tablets|watches|audio-sound|galaxy-buds|computers|galaxy-book|home-appliances|tvs|monitors|refrigerators|dishwashers|cooking-appliances|vacuum-cleaners|memory-storage|audio-devices)/?$',  # Category root pages
            r'/[^/]+/[^/]+/$',  # Two-level category pages
        ]
        
        # Exclude already specific product pages
        exclude_patterns = [
            r'/[^/]+-[a-z]{2}-[a-z0-9]+/',  # Model codes like -sm-r630nzaaeua
            r'/[a-z]{2}\d+[a-z0-9]+/',  # Model patterns like qe65s95fatxxu
            r'/hw-[a-z0-9]+/',  # Audio hardware codes
            r'/np\d+[a-z]+/',  # Laptop model codes
            r'/vs\d+[a-z]+/',  # Vacuum model codes
            r'/buying-guide/',  # Buying guides
            r'/learn/',  # Learning pages
            r'/compare/',  # Comparison pages
        ]
        
        is_category = any(re.search(pattern, url) for pattern in category_listing_patterns)
        is_excluded = any(re.search(pattern, url) for pattern in exclude_patterns)
        
        return is_category and not is_excluded

    def is_individual_product_url(self, url: str) -> bool:
        """Check if URL is an individual product page"""
        product_patterns = [
            # Samsung model patterns
            r'/[^/]+-[a-z]{2}-[a-z0-9]{10,}/',  # Model codes like -sm-r630nzaaeua
            r'/qe\d+[a-z]+\d+[a-z]+/',  # TV models like qe65s95fatxxu
            r'/ls\d+[a-z]+\d+[a-z]+/',  # Monitor/TV model codes
            r'/hw-[a-z0-9]+/',  # Audio hardware codes like hw-q990d-xu
            r'/np\d+[a-z]+-[a-z0-9]+/',  # Laptop model codes
            r'/vs\d+[a-z]+\d+[a-z]+/',  # Vacuum model codes
            r'/sm-[a-z0-9]+-[a-z]+/',  # Galaxy device codes
            r'/ww\d+[a-z]+\d+[a-z]+/',  # Washer model codes
            r'/rl\d+[a-z]+\d+[a-z]+/',  # Refrigerator codes
            
            # Product name patterns with specifications
            r'/[^/]+-\d+[^/]*-inch-[^/]+/',  # Size specifications like 65-inch, 27-inch
            r'/[^/]+-\d+hz-[^/]+/',  # Frequency specifications like 240hz
            r'/[^/]+-\d+gb-[^/]+/',  # Memory specifications like 16gb
            r'/[^/]+-ultra-[^/]+/',  # Ultra model variations
            r'/[^/]+-pro-[^/]+/',  # Pro model variations
            r'/galaxy-[^/]+-[^/]+-[^/]+/',  # Galaxy product with specifications
            r'/bespoke-[^/]+-[^/]+/',  # Bespoke appliance models
        ]
        
        # Exclude general pages
        exclude_patterns = [
            r'/all-',  # Category listing pages
            r'/buying-guide/',
            r'/learn/',
            r'/compare/',
            r'/highlights/',
            r'/why-',
            r'/$',  # URLs ending with just slash (category pages)
        ]
        
        is_product = any(re.search(pattern, url) for pattern in product_patterns)
        is_excluded = any(re.search(pattern, url) for pattern in exclude_patterns)
        
        return is_product and not is_excluded

    async def load_input_urls(self, input_file: str):
        """Load URLs from input file"""
        logger.info(f"Loading URLs from {input_file}")
        
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and self.is_samsung_uk_url(url):
                    self.input_urls.add(url)
        
        self.stats['input_urls_count'] = len(self.input_urls)
        logger.info(f"Loaded {len(self.input_urls)} URLs from input file")

    def categorize_urls(self):
        """Categorize input URLs into category listing URLs and individual product URLs"""
        logger.info("Categorizing input URLs...")
        
        for url in self.input_urls:
            if self.is_category_listing_url(url):
                self.category_urls.add(url)
                logger.debug(f"Category URL: {url}")
            elif self.is_individual_product_url(url):
                self.individual_urls.add(url)
                logger.debug(f"Individual product URL: {url}")
            else:
                # These might be other types of URLs (guides, etc.)
                logger.debug(f"Other URL type: {url}")
        
        self.stats['category_urls_identified'] = len(self.category_urls)
        self.stats['individual_urls_found'] = len(self.individual_urls)
        
        logger.info(f"Identified {len(self.category_urls)} category listing URLs")
        logger.info(f"Identified {len(self.individual_urls)} individual product URLs")

    async def expand_category_url(self, category_url: str) -> Set[str]:
        """Expand a single category URL to extract all product URLs"""
        logger.info(f"Expanding category URL: {category_url}")
        product_urls = set()
        page = None
        
        try:
            page = await self.browser.new_page()
            
            # Set shorter timeout and try domcontentloaded first
            try:
                await page.goto(category_url, wait_until='domcontentloaded', timeout=30000)
            except Exception:
                # Fallback to no wait condition with even shorter timeout
                await page.goto(category_url, timeout=15000)
            
            # Wait for initial content
            await page.wait_for_timeout(2000)
            
            # Scroll to load all content
            await self._scroll_to_load_all_content(page)
            
            # Handle "Load More" buttons and pagination
            await self._handle_pagination(page)
            
            # Extract all product links
            product_urls = await self._extract_product_links(page, category_url)
            
        except Exception as e:
            logger.error(f"Error expanding category URL {category_url}: {e}")
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
        
        product_count = len(product_urls)
        logger.info(f"Found {product_count} products in {category_url}")
        
        # Add validation warning for suspiciously low counts on major categories
        if self._is_major_category(category_url) and product_count < 10:
            logger.warning(f"⚠️  Low product count for major category {category_url}: only {product_count} products found")
            logger.warning("This may indicate incomplete pagination or page loading issues")
        
        return product_urls

    async def _scroll_to_load_all_content(self, page):
        """Scroll through the page to trigger lazy loading"""
        try:
            # Get initial page height
            last_height = await page.evaluate('document.body.scrollHeight')
            
            scroll_attempts = 0
            max_scrolls = 5  # Reduced from 10
            
            while scroll_attempts < max_scrolls:
                # Scroll to bottom
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                # Wait for new content to load (reduced timeout)
                await page.wait_for_timeout(1000)
                
                # Check if page height changed (new content loaded)
                new_height = await page.evaluate('document.body.scrollHeight')
                
                if new_height == last_height:
                    break
                
                last_height = new_height
                scroll_attempts += 1
            
            # Scroll back to top
            await page.evaluate('window.scrollTo(0, 0)')
            await page.wait_for_timeout(500)
            
        except Exception as e:
            logger.debug(f"Error during scrolling: {e}")

    async def _count_products_for_pagination(self, page) -> int:
        """Count products on page for pagination verification"""
        try:
            # Use the main product counting logic but optimized for speed
            product_selectors = [
                'a[href*="/uk/"][href*="-sm-"], a[href*="/uk/"][href*="-qe-"], a[href*="/uk/"][href*="-hw-"]',  # Samsung product URLs
                'a[href*="/uk/smartphones/"], a[href*="/uk/computers/"], a[href*="/uk/tvs/"]',
                'a[href*="/uk/audio"], a[href*="/uk/monitors/"], a[href*="/uk/tablets/"]'
            ]
            
            max_count = 0
            for selector in product_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    # Quick filter for product URLs
                    valid_count = 0
                    for element in elements:
                        href = await element.get_attribute('href')
                        if href and self._is_likely_product_url(href):
                            valid_count += 1
                    
                    if valid_count > max_count:
                        max_count = valid_count
                        
                except Exception:
                    continue
                    
            return max_count
        except Exception:
            return 0
            
    def _is_likely_product_url(self, url: str) -> bool:
        """Quick check if URL is likely a product URL"""
        if not url:
            return False
            
        # Product URL patterns (simplified for speed)
        has_product_pattern = any(pattern in url for pattern in ['-sm-', '-qe-', '-hw-', '-np-', '-ls-', '-xe-'])
        has_category = any(cat in url for cat in ['/smartphones/', '/computers/', '/tvs/', '/audio', '/monitors/', '/tablets/'])
        
        # Quick exclusion check
        excludes = ['#', '/all-', '/buy', '/compare', '/support', '/?']
        is_excluded = any(exc in url for exc in excludes)
        
        return has_product_pattern and has_category and not is_excluded

    def _is_major_category(self, url: str) -> bool:
        """Check if this is a major Samsung category that should have many products"""
        major_categories = [
            'galaxy-s', 'galaxy-a', 'galaxy-z',  # Major phone lines
            'all-smartphones', 'all-computers', 'all-tablets',  # Comprehensive categories
            'tvs/', 'monitors/', 'audio',  # Major product categories
            'soundbar', 'galaxy-buds', 'galaxy-book'
        ]
        return any(cat in url.lower() for cat in major_categories)

    async def _handle_pagination(self, page):
        """Handle Load More buttons and pagination with improved logic"""
        try:
            load_more_attempts = 0
            max_attempts = 15  # Increased for better coverage
            
            while load_more_attempts < max_attempts:
                button_found = False
                products_before = await self._count_products_for_pagination(page)
                
                # First try Samsung-specific product "View more" button (enhanced)
                samsung_product_selectors = [
                    '.pd19-product-finder__view-more-btn',  # Samsung's specific product listing button
                ]
                
                for selector in samsung_product_selectors:
                    try:
                        buttons = await page.query_selector_all(selector)
                        for button in buttons:
                            # Enhanced visibility check - try invisible buttons too for Galaxy Z fix
                            is_enabled = await button.is_enabled()
                            is_visible = await button.is_visible()
                            
                            # Get button classes to check context
                            button_class = await button.get_attribute('class') or ""
                            
                            # Skip filter buttons
                            if 'filter' in button_class:
                                logger.debug(f"Skipping filter button: {button_class}")
                                continue
                                
                            if is_enabled and (is_visible or 'pd19-product-finder__view-more-btn' in button_class):
                                logger.debug(f"Attempting Samsung product View more: {selector} (visible: {is_visible})")
                                
                                try:
                                    # Force click using JavaScript - works even if not visible
                                    await button.evaluate('button => button.click()')
                                    await page.wait_for_timeout(5000)  # Longer wait for Samsung pages
                                    await self._scroll_to_load_all_content(page)
                                    
                                    # Check if products were actually added
                                    products_after = await self._count_products_for_pagination(page)
                                    if products_after > products_before:
                                        logger.debug(f"✅ Pagination success: {products_before} → {products_after} products")
                                        button_found = True
                                        break
                                    else:
                                        logger.debug(f"❌ No new products after click: {products_before} → {products_after}")
                                        
                                except Exception as click_error:
                                    logger.debug(f"Error clicking button: {click_error}")
                                    continue
                                    
                        if button_found:
                            break
                            
                    except Exception as e:
                        logger.debug(f"Error with Samsung selector {selector}: {e}")
                        continue
                
                # Then try product-specific "View more" buttons (not filter buttons) with validation
                if not button_found:
                    product_view_more_selectors = [
                        '.product-list button:has-text("View more")',
                        '.products button:has-text("View more")',
                        '.grid button:has-text("View more")',
                        '[class*="product"] button:has-text("View more")',
                        '[data-testid*="product"] button:has-text("View more")',
                        '.listing button:has-text("View more")',
                        'main button:has-text("View more")',
                        'section button:has-text("View more")',
                    ]
                    
                    for selector in product_view_more_selectors:
                        try:
                            button = await page.query_selector(selector)
                            if button and await button.is_visible() and await button.is_enabled():
                                logger.debug(f"Attempting product listing View more: {selector}")
                                await button.scroll_into_view_if_needed()
                                
                                try:
                                    # Try force click first, fallback to regular click
                                    await button.evaluate('button => button.click()')
                                except Exception:
                                    await button.click()
                                    
                                await page.wait_for_timeout(3000)
                                await self._scroll_to_load_all_content(page)
                                
                                # Validate that products were actually added
                                products_after = await self._count_products_for_pagination(page)
                                if products_after > products_before:
                                    logger.debug(f"✅ Product pagination success: {products_before} → {products_after}")
                                    button_found = True
                                    break
                                else:
                                    logger.debug(f"❌ No new products from selector: {selector}")
                                    
                        except Exception:
                            continue
                
                # Special handling for problematic categories (Galaxy Z, All Computers)
                if not button_found:
                    url = page.url
                    if 'galaxy-z' in url or 'all-computers' in url:
                        logger.debug(f"Applying special handling for problematic category: {url}")
                        # Try to trigger pagination with different approaches
                        special_selectors = [
                            '.pd19-product-finder__view-more-btn',  # Force try even if invisible
                            'button[class*="view-more"]',
                            'button[class*="load-more"]',
                            '[data-testid*="view-more"]',
                            '[data-testid*="load-more"]'
                        ]
                        
                        for selector in special_selectors:
                            try:
                                buttons = await page.query_selector_all(selector)
                                for button in buttons:
                                    # Try ALL buttons, even invisible ones for problematic categories
                                    if await button.is_enabled():
                                        logger.debug(f"Force-trying button: {selector}")
                                        try:
                                            await button.evaluate('button => button.click()')
                                            await page.wait_for_timeout(5000)  # Longer wait
                                            await self._scroll_to_load_all_content(page)
                                            
                                            products_after = await self._count_products_for_pagination(page)
                                            if products_after > products_before:
                                                logger.debug(f"✅ Special handling success: {products_before} → {products_after}")
                                                button_found = True
                                                break
                                        except Exception as e:
                                            logger.debug(f"Special handling failed for {selector}: {e}")
                                            continue
                                
                                if button_found:
                                    break
                            except Exception:
                                continue

                # If no product-specific button found, try generic but avoid filter buttons (with validation)
                if not button_found:
                    try:
                        view_more_buttons = await page.query_selector_all('button:has-text("View more")')
                        for button in view_more_buttons:
                            if await button.is_visible() and await button.is_enabled():
                                # Get button class to check for filters
                                button_class = await button.get_attribute('class') or ""
                                
                                # Skip filter-related buttons based on class
                                if any(term in button_class.lower() for term in ['filter', 'pd19-product-finder-filter']):
                                    logger.debug(f"Skipping filter button with class: {button_class}")
                                    continue
                                
                                # Check parent context to avoid filter buttons
                                parent = await button.query_selector('xpath=..')
                                parent_class = await parent.get_attribute('class') if parent else ""
                                
                                # Skip filter-related buttons based on parent
                                if any(term in parent_class.lower() for term in ['filter', 'sidebar', 'nav', 'menu']):
                                    logger.debug(f"Skipping button with filter parent: {parent_class}")
                                    continue
                                
                                logger.debug(f"Attempting generic View more: {button_class}")
                                await button.scroll_into_view_if_needed()
                                
                                try:
                                    await button.evaluate('button => button.click()')
                                except Exception:
                                    await button.click()
                                    
                                await page.wait_for_timeout(3000)
                                await self._scroll_to_load_all_content(page)
                                
                                # Validate products were added
                                products_after = await self._count_products_for_pagination(page)
                                if products_after > products_before:
                                    logger.debug(f"✅ Generic pagination success: {products_before} → {products_after}")
                                    button_found = True
                                    break
                                else:
                                    logger.debug(f"❌ No new products from generic button")
                                    
                    except Exception:
                        pass
                
                if not button_found:
                    break
                
                load_more_attempts += 1
                logger.debug(f"Completed pagination attempt {load_more_attempts}/{max_attempts}")
            
        except Exception as e:
            logger.debug(f"Error handling pagination: {e}")

    async def _extract_product_links(self, page, source_url: str) -> Set[str]:
        """Extract all product links from the current page"""
        product_urls = set()
        
        try:
            # Get all links on the page
            links = await page.query_selector_all('a[href]')
            
            for link in links:
                href = await link.get_attribute('href')
                if not href:
                    continue
                
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    full_url = urljoin(BASE_URL, href)
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                # Check if it's a Samsung UK URL and an individual product
                if (self.is_samsung_uk_url(full_url) and 
                    self.is_individual_product_url(full_url)):
                    product_urls.add(full_url)
                    
                    # Store metadata
                    self.expansion_metadata[full_url] = {
                        'source_category_url': source_url,
                        'discovered_at': datetime.now(timezone.utc).isoformat(),
                        'extraction_method': 'dynamic_expansion'
                    }
            
        except Exception as e:
            logger.error(f"Error extracting product links: {e}")
        
        return product_urls

    async def expand_all_categories(self):
        """Expand all identified category URLs"""
        logger.info(f"Expanding {len(self.category_urls)} category URLs...")
        
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def process_category(category_url):
            async with semaphore:
                try:
                    product_urls = await self.expand_category_url(category_url)
                    self.expanded_urls.update(product_urls)
                    self.stats['pages_processed'] += 1
                    # Small delay to be respectful
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Error processing category {category_url}: {e}")
        
        # Process categories concurrently
        tasks = [process_category(url) for url in self.category_urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        self.stats['total_expanded_urls'] = len(self.expanded_urls)
        logger.info(f"Expansion complete: {len(self.expanded_urls)} product URLs found")

    def combine_all_urls(self) -> Set[str]:
        """Combine individual URLs with expanded URLs, removing duplicates"""
        all_urls = self.individual_urls.union(self.expanded_urls)
        logger.info(f"Combined URLs: {len(self.individual_urls)} individual + {len(self.expanded_urls)} expanded = {len(all_urls)} total unique URLs")
        return all_urls

    async def save_expanded_urls(self, output_file: str):
        """Save all expanded product URLs to file"""
        all_urls = self.combine_all_urls()
        
        logger.info(f"Saving {len(all_urls)} expanded URLs to {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for url in sorted(all_urls):
                f.write(url + '\n')

    async def save_expansion_metadata(self, metadata_file: str = 'expansion_metadata.json'):
        """Save expansion metadata to JSON file"""
        logger.info(f"Saving expansion metadata to {metadata_file}")
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.expansion_metadata, f, indent=2, ensure_ascii=False)

    def print_summary(self):
        """Print expansion summary"""
        if self.stats['start_time'] and self.stats['end_time']:
            runtime = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        else:
            runtime = 0
        
        print("\n" + "="*60)
        print("PRODUCT URL EXPANSION SUMMARY")
        print("="*60)
        print(f"Input URLs: {self.stats['input_urls_count']}")
        print(f"Category URLs identified: {self.stats['category_urls_identified']}")
        print(f"Individual URLs found: {self.stats['individual_urls_found']}")
        print(f"New URLs from expansion: {self.stats['total_expanded_urls']}")
        print(f"Total unique URLs: {len(self.combine_all_urls())}")
        print(f"Pages processed: {self.stats['pages_processed']}")
        print(f"Total runtime: {runtime:.2f} seconds")
        print("="*60)

    async def run(self, input_file: str, output_file: str):
        """Main expansion orchestration"""
        self.stats['start_time'] = datetime.now(timezone.utc)
        logger.info("Starting Samsung UK product URL expansion...")
        
        # Load and categorize URLs
        await self.load_input_urls(input_file)
        self.categorize_urls()
        
        # Expand category URLs
        if self.category_urls:
            await self.expand_all_categories()
        else:
            logger.info("No category URLs found to expand")
        
        # Save results
        await self.save_expanded_urls(output_file)
        await self.save_expansion_metadata()
        
        self.stats['end_time'] = datetime.now(timezone.utc)
        self.print_summary()
        
        logger.info("Product URL expansion completed successfully!")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Samsung UK Product URL Expansion')
    parser.add_argument('--input', default='product_urls.txt', help='Input URL file')
    parser.add_argument('--output', default='expanded_product_urls.txt', help='Output expanded URL file')
    parser.add_argument('--concurrency', type=int, default=3, help='Number of concurrent requests')
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        logger.error(f"Input file {args.input} not found")
        return
    
    async with ProductURLExpander(concurrency=args.concurrency) as expander:
        await expander.run(args.input, args.output)

if __name__ == "__main__":
    asyncio.run(main())