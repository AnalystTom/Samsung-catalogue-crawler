### 📄 Prompt for the AI coding assistant

**Role**  
You are a senior Python scraping engineer.

**Task**  
Produce a single‑file Python 3.11 script that reliably scrapes _every_ publicly listed Samsung product sold on the UK web‑shop and outputs a validated, structured newline‑delimited JSON file plus an optional Parquet snapshot of the same data.

**Required high‑level behaviour**

1. **Reasoning first, code second.**  
       _Begin your response with a short “Reasoning” section that explains, step‑by‑step, how you will achieve full coverage, fault‑tolerance and validation. After that section, output the complete script._
    
2. **Site discovery logic**  
       _Primary seed_ `https://www.samsung.com/uk/info/sitemap/` — parse every descendant link under the “Shop” branch and enqueue only URLs that reside on the domain **samsung.com/uk** and lead to a **category or listing** page. [samsung.com](https://www.samsung.com/uk/info/sitemap/)  
       _Hard‑coded fall‑backs_    (if the sitemap changes or a category is missing, crawl these “all‑products” hubs):  
       • `/smartphones/all-smartphones/` [samsung.com](https://www.samsung.com/uk/smartphones/all-smartphones/?utm_source=chatgpt.com)  
       • `/tablets/all-tablets/` [samsung.com](https://www.samsung.com/uk/tablets/all-tablets/?utm_source=chatgpt.com)  
       • `/watches/all-watches/` [samsung.com](https://www.samsung.com/uk/watches/all-watches/?galaxy-watch=&utm_source=chatgpt.com)  
       • `/tvs/all-tvs/` [samsung.com](https://www.samsung.com/uk/tvs/all-tvs/?utm_source=chatgpt.com)  
       • `/monitors/all-monitors/` [samsung.com](https://www.samsung.com/uk/monitors/all-monitors/?utm_source=chatgpt.com)  
       • `/audio-sound/all-audio-sound/` (for Buds & head‑phones) [samsung.com](https://www.samsung.com/uk/audio-sound/all-audio-sound/?galaxy-buds=&utm_source=chatgpt.com)  
       • `/refrigerators/all-refrigerators/` [samsung.com](https://www.samsung.com/uk/refrigerators/all-refrigerators/?utm_source=chatgpt.com)  
       • `/washers-and-dryers/all-washers-and-dryers/` [samsung.com](https://www.samsung.com/uk/washers-and-dryers/all-washers-and-dryers/?washing-machines=&utm_source=chatgpt.com)  
       • `/vacuum-cleaners/all-vacuum-cleaners/` [samsung.com](https://www.samsung.com/uk/vacuum-cleaners/all-vacuum-cleaners/?utm_source=chatgpt.com)  
       Your discovery code **must** de‑duplicate URLs and detect paginated “Load more” XHRs to reach every PDP (product‑detail page).
    
3. **Extraction rules**  
       _Preferred path_ Parse the `<script type="application/ld+json">` block to obtain  
       `name, sku, price, priceCurrency, availability, category, image, url`.  
       _Fallback path_ If JSON‑LD is absent or incomplete, fall back to CSS/XPath selectors (document these selectors in comments).
    
4. **Data model**  
       Create a `pydantic.BaseModel` named `ProductSchema` with the fields:  
       `url, sku, name, category, sub_category, price_gbp, currency, availability, image_url, timestamp_utc`.  
       Validation errors should trigger an immediate retry (see §6).
    
5. **Fetcher stack (layered for reliability & cost control)**  
       _Layer 1 – Static_ `aiohttp` GET; bail out with HTML only if the page already contains full JSON‑LD.  
       _Layer 2 – Dynamic_ Async Playwright (Chromium, headless, `--lang=en-GB`); wait for `networkidle`; intercept in‑page JSON if available.
    
6. **Fault‑tolerance & self‑healing**  
       Decorate the fetcher with `tenacity.retry` (exponential back‑off, max 4 attempts).  
       If all retries fail, move the URL to `failed_urls.txt`.  
       Design the script so that the failed list can later be passed to an external “AI‑agent” extractor without code changes.
    
7. **Concurrency & rate‑limits**  
       Use `asyncio.Semaphore(CONCURRENCY=10)` in local runs; make the value configurable via CLI arg or ENV.  
       Respect Samsung’s `robots.txt` crawl‑delay and include a descriptive `User‑Agent` with contact email.
    
8. **Outputs**  
       • Write every validated `ProductSchema` instance as one line of JSON into `products.ndjson`.  
       • After crawling finishes, convert the NDJSON file to `products.parquet` (via `pandas`) to ease analytics.
    
9. **Observability**  
       Add `logging` at INFO level for milestones and DEBUG for per‑URL events.  
       Print an end‑of‑run summary with counts of succeeded, retried, permanently failed URLs and total runtime.
    
10. **Run‑ability**  
        The script must run with  
        `python3 -m pip install playwright aiohttp pydantic tenacity pandas pyarrow bs4 lxml && playwright install`  
        and then  
        `python samsung_uk_scraper.py`.  
        Avoid external services so that a reviewer can test locally without credentials.
    
11. **Deliverable format**  
        After the “Reasoning” section, output the full script.  
        When you show the code, wrap it in a fenced markdown block with the `python` language identifier (the reviewer will copy‑paste).  
        Do **not** output anything else after the code block.