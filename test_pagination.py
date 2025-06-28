#!/usr/bin/env python3
"""
Test script to analyze pagination patterns on Samsung UK category pages
"""

import asyncio
import logging
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_category_pagination(url: str):
    """Test pagination on a specific category URL"""
    logger.info(f"Testing pagination for: {url}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Visible browser for debugging
        page = await browser.new_page()
        
        try:
            # Navigate to the page
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(3000)
            
            # Count initial products - use generic selector for any category
            initial_products = await page.query_selector_all('a[href]')
            product_links = []
            for link in initial_products:
                href = await link.get_attribute('href')
                if href and ('samsung.com' in href or href.startswith('/')):
                    # Look for URLs that seem like product pages
                    if any(pattern in href for pattern in ['-sm-', '-qe', '-ls', '-hw-', '-np', '-vs', 'galaxy-', 'neo-qled', 'oled']):
                        product_links.append(href)
            
            logger.info(f"Initial product-like links found: {len(product_links)}")
            
            # Look for load more buttons
            load_more_selectors = [
                'button:has-text("Load more")',
                'button:has-text("Show more")',
                'button:has-text("View more")',
                'button:has-text("See all")',
                '.load-more',
                '.show-more',
                '.view-more',
                '[data-testid="load-more"]',
                '[class*="load-more"]',
                '[class*="show-more"]',
                'button[class*="pagination"]',
                'button[class*="load"]',
                'button[class*="more"]',
            ]
            
            # Check which load more buttons exist
            for selector in load_more_selectors:
                try:
                    buttons = await page.query_selector_all(selector)
                    if buttons:
                        logger.info(f"Found {len(buttons)} buttons with selector: {selector}")
                        for i, button in enumerate(buttons):
                            text = await button.inner_text()
                            is_visible = await button.is_visible()
                            logger.info(f"  Button {i}: '{text}' (visible: {is_visible})")
                except Exception as e:
                    logger.debug(f"Error checking selector {selector}: {e}")
            
            # Try clicking load more buttons
            load_attempts = 0
            max_attempts = 20
            
            while load_attempts < max_attempts:
                button_clicked = False
                
                # Try to find the product listing "View more" button (not filter "View more")
                try:
                    # Look for "View more" button that's associated with product listings
                    # This is usually in a products container or has specific classes
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
                        button = await page.query_selector(selector)
                        if button and await button.is_visible() and await button.is_enabled():
                            logger.info(f"Clicking product listing View more button: {selector}")
                            await button.scroll_into_view_if_needed()
                            await button.click()
                            await page.wait_for_timeout(4000)
                            button_clicked = True
                            break
                    
                    # If no specific selector worked, try generic "View more" but avoid filter areas
                    if not button_clicked:
                        view_more_buttons = await page.query_selector_all('button:has-text("View more")')
                        for i, button in enumerate(view_more_buttons):
                            if await button.is_visible() and await button.is_enabled():
                                # Get parent context to avoid filter "View more" buttons
                                parent = await button.query_selector('xpath=..')
                                parent_class = await parent.get_attribute('class') if parent else ""
                                
                                # Skip if it's likely a filter button
                                if any(term in parent_class.lower() for term in ['filter', 'sidebar', 'nav', 'menu']):
                                    logger.debug(f"Skipping filter View more button {i}")
                                    continue
                                
                                logger.info(f"Clicking generic View more button {i}")
                                await button.scroll_into_view_if_needed()
                                await button.click()
                                await page.wait_for_timeout(4000)
                                button_clicked = True
                                break
                except Exception as e:
                    logger.debug(f"Error clicking View more: {e}")
                
                # If no View more button worked, try other selectors
                if not button_clicked:
                    for selector in load_more_selectors:
                        try:
                            button = await page.query_selector(selector)
                            if button and await button.is_visible() and await button.is_enabled():
                                logger.info(f"Clicking load more button: {selector}")
                                await button.scroll_into_view_if_needed()
                                await button.click()
                                await page.wait_for_timeout(4000)
                                button_clicked = True
                                break
                        except Exception as e:
                            logger.debug(f"Error clicking {selector}: {e}")
                            continue
                
                if not button_clicked:
                    logger.info("No more clickable load buttons found")
                    break
                
                # Count products after clicking - use generic approach
                current_products = await page.query_selector_all('a[href]')
                current_product_links = []
                for link in current_products:
                    href = await link.get_attribute('href')
                    if href and ('samsung.com' in href or href.startswith('/')):
                        if any(pattern in href for pattern in ['-sm-', '-qe', '-ls', '-hw-', '-np', '-vs', 'galaxy-', 'neo-qled', 'oled']):
                            current_product_links.append(href)
                
                logger.info(f"Product-like links after load attempt {load_attempts + 1}: {len(current_product_links)}")
                
                load_attempts += 1
            
            # Final count and extraction
            final_products = await page.query_selector_all('a[href]')
            all_urls = set()
            for product in final_products:
                href = await product.get_attribute('href')
                if href:
                    if href.startswith('/'):
                        href = f"https://www.samsung.com{href}"
                    
                    # Check if it looks like a Samsung product URL
                    if ('samsung.com' in href and 
                        any(pattern in href for pattern in ['-sm-', '-qe', '-ls', '-hw-', '-np', '-vs', 'galaxy-', 'neo-qled', 'oled'])):
                        # Remove anchor fragments
                        if '#' in href:
                            href = href.split('#')[0]
                        all_urls.add(href)
            
            logger.info(f"Total unique product URLs found: {len(all_urls)}")
            
            # Show sample URLs
            sample_urls = list(all_urls)[:10]
            logger.info("Sample product URLs found:")
            for url in sample_urls:
                logger.info(f"  {url}")
            
        except Exception as e:
            logger.error(f"Error testing pagination: {e}")
        finally:
            await browser.close()

async def main():
    """Test multiple category pages"""
    test_urls = [
        "https://www.samsung.com/uk/smartphones/galaxy-s/",
        "https://www.samsung.com/uk/tvs/qled-tv/",
        "https://www.samsung.com/uk/tablets/all-tablets/",
    ]
    
    for url in test_urls:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing URL: {url}")
        logger.info('='*60)
        await test_category_pagination(url)
        await asyncio.sleep(2)  # Small delay between tests

if __name__ == "__main__":
    asyncio.run(main())