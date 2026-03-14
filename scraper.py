import os
import requests
import json
import traceback
import gspread
import re
import concurrent.futures
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

# Default Configure environment variables
DEFAULT_GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
DEFAULT_SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

def send_sse(event: str, data: dict):
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

def extract_sheet_id(url: str):
    if not url:
        return None
    # match /d/(.*?)/
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if match:
        return match.group(1)
    return url # Fallback to using the whole string as the ID/Name if it's not a URL

def append_to_google_sheet(leads_data: list, sheet_url: str = None, sheet_tab: str = "Sheet1"):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds_path = os.path.join(os.path.dirname(__file__), "service_account.json")
        
        if not os.path.exists(creds_path):
            print(f"\\n[Warning] Google Sheets integration skipped. Missing: {creds_path}")
            return False
            
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)
        
        target_sheet = sheet_url if sheet_url else DEFAULT_GOOGLE_SHEET_NAME
        sheet_id = extract_sheet_id(target_sheet)
        
        try:
            if target_sheet and "docs.google.com" in target_sheet:
                spreadsheet = client.open_by_key(sheet_id)
            else:
                spreadsheet = client.open(target_sheet)
                
            print(f"\\n[INFO] Successfully mapped to Google Sheet: '{target_sheet}'")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"\\n[ERROR] Could not find a Google Sheet with ID/Name: '{target_sheet}'")
            return False
        except Exception as e:
            print(f"\\n[ERROR] Authentication or Access error for Google Sheet: {e}")
            return False
            
        try:
            sheet = spreadsheet.worksheet(sheet_tab or "Sheet1")
        except gspread.exceptions.WorksheetNotFound:
            print(f"\\n[ERROR] Could not find the tab name '{sheet_tab}' in the specific sheet.")
            return False
        
        for lead in leads_data:
            row = [
                lead.get("name", "Unknown"), 
                lead.get("url", "NO WEBSITE"), 
                lead.get("status", "Pending"), 
                lead.get("phone_number", "N/A"),
                lead.get("address", "N/A"),
                lead.get("email", "N/A"),
                lead.get("rating", "N/A")
            ]
            sheet.append_row(row)
            
        return True
    except Exception as e:
        traceback.print_exc()
        return False

import time
import random
from playwright.sync_api import sync_playwright

# List of realistic User-Agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
]

def check_website(url):
    if not url or url == "NO WEBSITE":
        return "Pending"
        
    if not url.startswith('http'):
        url = 'http://' + url
        
    max_retries = 2
    for attempt in range(max_retries):
        try:
            with sync_playwright() as p:
                # Launch headless browser
                browser = p.chromium.launch(headless=True)
                
                # Create context with a random User-Agent to evade basic bot detection
                context = browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True # Ignore self-signed certs
                )
                
                page = context.new_page()
                
                # Navigate to URL, wait for DOM content (allows JS to execute)
                # 20s timeout for the page to load
                response = page.goto(url, wait_until='domcontentloaded', timeout=20000)
                
                if not response:
                    browser.close()
                    time.sleep(2)
                    continue
                    
                status_code = response.status
                
                # Wait an additional brief moment for Cloudflare/Vercel JS redirects or challenges to trigger
                try:
                    page.wait_for_timeout(3000)
                except:
                    pass
                
                content_lower = page.content().lower()
                
                # Cleanup
                browser.close()
                
                # 1. Check for parked/expired domains -> Broken
                parked_keywords = [
                    "domain has expired",
                    "domain is expired",
                    "this domain is for sale",
                    "buy this domain",
                    "parked domain",
                    "domain parked",
                    "account suspended",
                    "this site is currently unavailable",
                    "website expired",
                    "default webpage",
                    "future home of something quite cool"
                ]
                for kw in parked_keywords:
                    if kw in content_lower:
                        return "Broken Link"
                        
                # 2. Check for Cloudflare/Protection/Vercel -> Protected
                protection_keywords = [
                    "just a moment...",
                    "access denied",
                    "cloudflare",
                    "attention required!",
                    "security measure",
                    "please turn javascript on",
                    "vercel security checkpoint",
                    "checking if the site connection is secure"
                ]
                
                if status_code in [401, 403, 405, 406] or any(kw in content_lower for kw in protection_keywords):
                    if attempt < max_retries - 1 and status_code == 403:
                        time.sleep(2)
                        continue # Retry on 403
                    return "Protected"
                    
                # 3. >= 400 means Broken
                if status_code >= 400:
                    return "Broken Link"
                    
                return "Verified Active"
                
        except Exception as e:
            # Catch timeouts and connection errors from Playwright
            if attempt < max_retries - 1:
                time.sleep(3) # Wait before retry
                continue
            return "Broken Link"
            
    return "Broken Link"


def run_serper_scan(keyword: str, location: str, radius: int, filter_option: str = "all", search_type: str = "places", api_key: str = None):
    # Fix: Appending 'within Xkm' natively breaks Google Maps Places API pagination for international queries.
    if location == "Global":
        if search_type == "search":
            base_query = f"{keyword} worldwide forum OR community"
        else:
            base_query = f"{keyword} worldwide companies"
    else:
        if search_type == "search":
            base_query = f"{keyword} in {location} forum OR community"
        else:
            base_query = f"{keyword} in {location}"
    
    active_api_key = api_key if api_key else DEFAULT_SERPER_API_KEY
    if not active_api_key:
        yield send_sse("log", {"time": "", "type": "error", "text": "No Serper API Key provided in backend or settings."})
        yield send_sse("done", {"count": 0})
        return
        
    url = "https://google.serper.dev/search" if search_type == "search" else "https://google.serper.dev/places"
    headers = {
      'X-API-KEY': active_api_key,
      'Content-Type': 'application/json'
    }
    
    yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Initializing scan for {keyword} in {location}"})
    yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Filter active: {filter_option.replace('_', ' ').upper()}"})
    yield send_sse("log", {"time": "", "type": "warning", "text": f"[PROCESS] Engine primed for Auto-Pagination (Goal: 50+ results)..."})

    scraped_data = []
    seen_names = set()
    page = 1

    # Intelligent Auto-Pagination (Fetch until at least 99 unique results)
    while len(seen_names) < 99 and page <= 10:
        yield send_sse("log", {"time": "", "type": "warning", "text": f"[PROCESS] Fetching Search Page {page} (Currently {len(seen_names)} unique leads)..."})
        try:
            if search_type == "places":
                payload = json.dumps({
                    "q": base_query,
                    "page": page,
                    "search_type": "places" # Explicit lock
                })
                data_key = "places"
            else:
                 payload = json.dumps({
                    "q": base_query,
                    "page": page,
                    "num": 100
                })
                 data_key = "organic"

            response = requests.request("POST", url, headers=headers, data=payload)
            response.raise_for_status()
            data = response.json()
            
            if data_key not in data or not data[data_key]:
                yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Boundaries reached. No more results found on page {page}."})
                break
                
            new_places_this_page = 0
            
            for item in data[data_key]:
                name = item.get("title", "Unknown")
                
                if name in seen_names:
                    continue
                    
                seen_names.add(name)
                new_places_this_page += 1
                
                if search_type == "places":
                    website = item.get("website", "NO WEBSITE")
                    
                    if filter_option == "no_website" and website != "NO WEBSITE":
                        continue
                        
                    # Extract email if available in places or from secondary links
                    emails = item.get("emails", [])
                    email = emails[0] if emails else "N/A"
                    
                    rating = item.get("rating", "N/A")
                    phone_number = item.get("phoneNumber", "N/A")
                    
                    # Split address into components if possible
                    address = item.get("address", "N/A")
                    city_str = "N/A"
                    country_str = "N/A"
                    
                    if address != "N/A":
                        raw_parts = [p.strip() for p in address.split(",")]
                        parts = [p for p in raw_parts if not p.lower().startswith("opens ") and not p.lower().startswith("closes ")]
                        
                        if len(parts) >= 2:
                            country_str = parts[-1]
                            city_str = parts[-2]
                            import re
                            city_str = re.sub(r'[\d\-]', '', city_str).strip()
                        elif len(parts) == 1:
                            country_str = parts[0]
                else: # Search/Forums path
                    website = item.get("link", "NO WEBSITE")
                    if filter_option == "no_website" and website != "NO WEBSITE":
                        continue
                        
                    email = "N/A"
                    rating = "N/A"
                    address = "N/A"
                    city_str = "N/A"
                    country_str = "N/A"
                    snippet = item.get("snippet", "N/A")
                    phone_number = (snippet[:47] + "...") if snippet != "N/A" else "N/A"

                scraped_data.append({
                    "name": name,
                    "url": website,
                    "status": "Verified" if website != "NO WEBSITE" else "Pending",
                    "phone_number": phone_number,
                    "address": address,
                    "city": city_str,
                    "country": country_str,
                    "email": email,
                    "rating": rating
                })
                
                # Send live result back
                yield send_sse("result", scraped_data[-1])
            
            if new_places_this_page == 0:
                yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Only duplicate map pins found on page {page}. Halting fetch loop."})
                break
                
            page += 1
            
        except Exception as e:
            yield send_sse("log", {"time": "", "type": "error", "text": f"[ERROR] Serper API failure on page {page}: {e}"})
            break

    # If the user wants ONLY Broken websites or ALL, filter/verify them
    if filter_option in ["broken_website", "all", "no_or_broken_website"]:
        yield send_sse("log", {"time": "", "type": "warning", "text": f"[PROCESS] Launching High-Performance Verification Engine..."})
        
        verified_data = []
        
        def process_lead(lead):
            if lead['url'] == "NO WEBSITE":
                return lead
                
            status_result = check_website(lead['url'])
            lead['status'] = status_result
                
            return lead
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_lead = {executor.submit(process_lead, lead): lead for lead in scraped_data}
            completed = 0
            
            for future in concurrent.futures.as_completed(future_to_lead):
                res = future.result()
                completed += 1
                
                if completed % 10 == 0 or completed == len(scraped_data):
                    yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Engine verified {completed}/{len(scraped_data)} links..."})
                
                if res:
                    # If target was broken only, discard active ones
                    if filter_option == "broken_website" and res['status'] != "Broken Link":
                        continue
                        
                    # If target was no_or_broken, discard active ones
                    if filter_option == "no_or_broken_website" and res['status'] == "Verified Active":
                        continue
                        
                    verified_data.append(res)
                    yield send_sse("result_update", res)
                    
        scraped_data = verified_data

    # Assign IDs sequentially for frontend table rendering
    for idx, lead in enumerate(scraped_data):
        lead["id"] = idx + 1

    # Send final success if there are any
    if scraped_data:
        yield send_sse("log", {"time": "", "type": "success", "text": f"[SUCCESS] Total valid extraction: {len(scraped_data)} leads. Ready to save."})
    else:
        yield send_sse("log", {"time": "", "type": "info", "text": f"[INFO] Scan complete. 0 valid leads found for this specific filter."})

    yield send_sse("done", {"count": len(scraped_data), "final_data": scraped_data})