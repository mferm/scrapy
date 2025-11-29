import scrapy
import json
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

class GsaSpider(scrapy.Spider):
    name = 'gsa'
    allowed_domains = ['gsaadvantage.gov']
    
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,  # Lower for browser automation
        'DOWNLOAD_DELAY': 2,  # Longer delay for JavaScript pages
        'ROBOTSTXT_OBEY': True,
    }
    
    def __init__(self, part_numbers_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Load part numbers from file
        if part_numbers_file:
            with open(part_numbers_file, 'r') as f:
                self.part_numbers = [line.strip() for line in f if line.strip()]
        else:
            # Default test with your example
            self.part_numbers = ['BR32CCP07', 'UNCBR32CCP07']
    
    def start_requests(self):
        """Step 1: Visit homepage to establish session"""
        yield scrapy.Request(
            'https://www.gsaadvantage.gov/',
            callback=self.after_homepage,
            meta={
                'playwright': True,
                'playwright_include_page': True,
            },
            dont_filter=True,
        )

    async def after_homepage(self, response):
        """Step 2: Search for each part number using the search form"""
        page = response.meta.get("playwright_page")
        
        if not page:
            self.logger.error("No Playwright page available")
            return
        
        for pn in self.part_numbers:
            self.logger.info(f"Searching for: {pn}")
            
            try:
                # Navigate to homepage to ensure we're on the search page
                await page.goto('https://www.gsaadvantage.gov/', wait_until='networkidle', timeout=60000)
                await page.wait_for_timeout(2000)  # Wait for Angular to load
                
                # Find and fill the search input (id="globalSearch")
                search_input = await page.wait_for_selector('#globalSearch', timeout=10000, state='visible')
                await search_input.fill('')
                await search_input.fill(pn)
                await page.wait_for_timeout(500)  # Small delay after typing
                
                # Find and click the search button (type="submit" with name="GO")
                search_button = await page.wait_for_selector('button[type="submit"][name="GO"]', timeout=5000, state='visible')
                await search_button.click()
                
                # Wait for search results to load
                await page.wait_for_load_state('networkidle', timeout=60000)
                await page.wait_for_selector('app-ux-product-display-inline', timeout=10000)
                await page.wait_for_timeout(2000)  # Extra wait for dynamic content
                
                # Get the page content after search
                content = await page.content()
                
                # Create a new response object with the updated content
                from scrapy.http import HtmlResponse
                search_response = HtmlResponse(
                    url=page.url,
                    body=content.encode('utf-8'),
                    encoding='utf-8'
                )
                # Store part_number on the response object for parsing
                search_response._part_number = pn
                
                # Parse the search results - pass part_number directly
                for item in self.parse_search_results(search_response, part_number=pn):
                    yield item
                
            except Exception as e:
                self.logger.error(f"Error searching for {pn}: {e}", exc_info=True)
                continue
        
        # Close the browser page when done
        if page:
            await page.close()
    
    async def errback_close_page(self, failure):
        """Handle errors and close browser page"""
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed: {failure.value}")
    
    async def errback_detail_page(self, failure):
        """Handle errors on detail page requests"""
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Detail page request failed: {failure.request.url} - {failure.value}")
    
    def parse_search_results(self, response, part_number=None):
        """Step 2: Extract listing links from search results - ONLY exact matches"""
        # Get part_number from parameter or response attribute
        if not part_number:
            part_number = getattr(response, '_part_number', None)
        if not part_number:
            self.logger.error("No part_number provided")
            return
        
        # Find all product cards in search results
        product_cards = response.css('app-ux-product-display-inline')
        
        if not product_cards:
            self.logger.warning(f"No listings found for part number: {part_number}")
            return
        
        exact_matches = 0
        
        # Check each product card
        for card in product_cards:
            # Extract the Mfr Part Number (the field right above product name)
            displayed_part_number = card.css('div.mfrPartNumber::text').get()
            
            if displayed_part_number:
                displayed_part_number = displayed_part_number.strip()
                
                # EXACT MATCH CHECK (case-insensitive)
                if displayed_part_number.upper() == part_number.upper():
                    exact_matches += 1
                    
                    # Extract the link to detail page
                    detail_link = card.css('div.itemName a::attr(href)').get()
                    
                    if detail_link:
                        # Make sure we have an absolute URL
                        if detail_link.startswith('/'):
                            full_url = f"https://www.gsaadvantage.gov{detail_link}"
                        elif detail_link.startswith('http'):
                            full_url = detail_link
                        else:
                            full_url = urljoin(response.url, detail_link)
                        
                        # Ensure we're requesting classic design by adding pdNewDesign=false if not present
                        if 'pdNewDesign' not in full_url:
                            separator = '&' if '?' in full_url else '?'
                            full_url = f"{full_url}{separator}pdNewDesign=false"
                        
                        self.logger.info(f"✓ Exact match found: {displayed_part_number} for search {part_number}")
                        self.logger.info(f"Requesting detail page (classic design): {full_url}")
                        
                        yield scrapy.Request(
                            full_url,
                            callback=self.parse_listing,
                            meta={
                                'part_number': part_number,
                                'displayed_part_number': displayed_part_number,
                                'playwright': True,
                                'playwright_include_page': True,
                                'playwright_page_goto_kwargs': {
                                    'wait_until': 'networkidle',
                                    'timeout': 60000,
                                },
                            },
                            errback=self.errback_detail_page,
                            dont_filter=True,
                        )
                else:
                    self.logger.debug(f"✗ Skipping non-match: {displayed_part_number} (searched for {part_number})")
        
        if exact_matches == 0:
            self.logger.warning(f"No exact matches found for: {part_number}")
        else:
            self.logger.info(f"Found {exact_matches} exact match(es) for {part_number}")
    
    async def parse_listing(self, response):
        """Step 3: Extract price, vendor, and product details from listing page"""
        part_number = response.meta['part_number']
        displayed_part_number = response.meta.get('displayed_part_number', '')
        
        # Get Playwright page if available
        page = response.meta.get("playwright_page")
        
        if page:
            # Check if we need to switch to classic design
            try:
                # Wait for page to fully load
                await page.wait_for_load_state('networkidle', timeout=30000)
                await page.wait_for_timeout(2000)  # Extra wait for Angular
                
                # Check if we're on the new design by looking for the classic design link or alert
                classic_link = await page.query_selector('a[href*="pdNewDesign=false"]')
                
                if classic_link:
                    self.logger.info("Found new design page, switching to classic design...")
                    # Click the classic design link
                    await classic_link.click()
                    # Wait for the classic design to load
                    await page.wait_for_load_state('networkidle', timeout=60000)
                    await page.wait_for_timeout(3000)  # Extra wait for classic design to render
                    
                    # Update response with new content
                    content = await page.content()
                    from scrapy.http import HtmlResponse
                    response = HtmlResponse(
                        url=page.url,
                        body=content.encode('utf-8'),
                        encoding='utf-8'
                    )
                    self.logger.info("Switched to classic design successfully")
                else:
                    # Try alternative: modify URL directly
                    current_url = page.url
                    if 'pdNewDesign=false' not in current_url:
                        # Check if we're missing the classic design table
                        table_check = await page.query_selector('tr.selectedItem, tr.otherItem')
                        if not table_check:
                            # Try adding the parameter to the URL
                            parsed = urlparse(current_url)
                            query_params = parse_qs(parsed.query)
                            query_params['pdNewDesign'] = ['false']
                            new_query = urlencode(query_params, doseq=True)
                            new_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
                            
                            self.logger.info(f"Modifying URL to switch to classic design: {new_url}")
                            await page.goto(new_url, wait_until='networkidle', timeout=60000)
                            await page.wait_for_timeout(3000)
                            
                            # Update response with new content
                            content = await page.content()
                            from scrapy.http import HtmlResponse
                            response = HtmlResponse(
                                url=page.url,
                                body=content.encode('utf-8'),
                                encoding='utf-8'
                            )
            except Exception as e:
                self.logger.warning(f"Error switching to classic design (will try to parse anyway): {e}")
        
        # Debug: Check if table exists
        table_rows = response.css('tr.selectedItem, tr.otherItem')
        if not table_rows:
            self.logger.warning(f"No pricing table rows found on page: {response.url}")
            # Save HTML for debugging
            with open('/Users/michael/Documents/Visions/scraper/debug_detail_page.html', 'w', encoding='utf-8') as f:
                f.write(response.text[:50000])  # First 50k chars
            self.logger.info("Saved debug HTML to debug_detail_page.html")
        else:
            self.logger.info(f"Found {len(table_rows)} pricing table rows")
        
        # Extract Manufacturer Part Number
        mfr_part_number = None
        mfr_part_rows = response.xpath('//div[contains(@class, "row") and .//strong[contains(text(), "Manufacturer Part Number")]]')
        if mfr_part_rows:
            mfr_part_number = mfr_part_rows.xpath('.//div[contains(@class, "col-lg-8")]/text()').get()
            if mfr_part_number:
                mfr_part_number = mfr_part_number.strip()
        
        # Extract Contractor Part Number
        contractor_part_number = None
        contractor_part_rows = response.xpath('//div[contains(@class, "row") and .//strong[contains(text(), "Contractor Part Number")]]')
        if contractor_part_rows:
            contractor_part_number = contractor_part_rows.xpath('.//div[contains(@class, "col-lg-8")]/text()').get()
            if contractor_part_number:
                contractor_part_number = contractor_part_number.strip()
        
        # Extract Manufacturer
        manufacturer = None
        mfr_rows = response.xpath('//div[contains(@class, "row") and .//strong[text()="Manufacturer"]]')
        if mfr_rows:
            manufacturer = mfr_rows.xpath('.//div[contains(@class, "col-lg-8")]/text()').get()
            if manufacturer:
                manufacturer = manufacturer.strip()
        
        # Product name
        product_name = response.css('h1.product-title span::text').get()
        if product_name:
            product_name = product_name.strip()
        
        # Extract up to 10 prices from all pricing table rows
        # Each row represents a different contractor/vendor offering
        pricing_rows = response.xpath('//tr[contains(@class, "selectedItem") or contains(@class, "otherItem")]')
        max_prices = 10
        prices_extracted = 0
        
        for row in pricing_rows[:max_prices]:
            # Extract Price from 2nd <td> as <strong>$106.82</strong>
            price_raw = row.xpath('.//td[2]//strong/text()').get()
            price = None
            if price_raw:
                price_raw = price_raw.strip()
                # Remove $ and clean up
                price = price_raw.replace('$', '').strip()
            
            # Skip if no price found
            if not price:
                continue
            
            # Extract Unit of Measure from 3rd <td>
            unit = None
            unit_link = row.xpath('.//td[3]//a[contains(@href, "UNIT_DEFINITIONS")]/text()').get()
            if unit_link:
                unit = unit_link.strip()
            
            # Extract Contractor/Vendor Name from 5th <td>
            # selectedItem rows have contractor in <span><b>NAME</b></span>
            # otherItem rows have contractor in <a href="contractor_detail">NAME</a>
            contractor_name = None
            # First try bold text (for selectedItem)
            contractor_bold = row.xpath('.//td[5]//b/text()').get()
            if contractor_bold:
                contractor_name = contractor_bold.strip()
            # If not found, try contractor link (for otherItem)
            if not contractor_name:
                contractor_link = row.xpath('.//td[5]//a[contains(@href, "contractor_detail")]/text()').get()
                if contractor_link:
                    contractor_name = contractor_link.strip()
            
            # Extract Contract Number from contractor link URL in 5th <td>
            contract_number = None
            contractor_url = row.xpath('.//td[5]//a[contains(@href, "contractor_detail")]/@href').get()
            if contractor_url and 'contractNumber=' in contractor_url:
                # Extract contract number from URL like: /advantage/ws/catalog/contractor_detail?contractNumber=GS-35F-402GA
                match = re.search(r'contractNumber=([^&]+)', contractor_url)
                if match:
                    contract_number = match.group(1)
            
            # If we have a price, yield this pricing option
            prices_extracted += 1
            self.logger.info(f"Scraped price {prices_extracted}: {part_number} - {contractor_name} - ${price}")
            
            yield {
                'searched_part_number': part_number,
                'displayed_part_number': displayed_part_number,
                'mfr_part_number': mfr_part_number,
                'contractor_part_number': contractor_part_number,
                'manufacturer': manufacturer,
                'price': price,
                'unit': unit,
                'contractor_name': contractor_name,
                'contract_number': contract_number,
                'product_name': product_name,
                'url': response.url,
            }
        
        if prices_extracted == 0:
            self.logger.warning(f"No valid prices found for {part_number} on page: {response.url}")