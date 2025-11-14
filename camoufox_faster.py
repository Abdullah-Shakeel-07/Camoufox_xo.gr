import os
import json
import logging
import pandas as pd
from camoufox.sync_api import Camoufox

####################################################
# CACHE MANAGER CLASS
####################################################
class CacheManager:
    ROOT_PATH = 'cache'
    ERROR_PATH = 'error_cache'

    def __init__(self, base_path=''):
        self.base_folder_name = base_path
        self.base_path = os.path.join(self.ROOT_PATH, base_path)
        self.error_path = os.path.join(self.ERROR_PATH, base_path)
        self.counter = 0
        self.error_counter = 0

        os.makedirs(self.ROOT_PATH, exist_ok=True)
        os.makedirs(self.ERROR_PATH, exist_ok=True)
        os.makedirs(self.base_path, exist_ok=True)
        os.makedirs(self.error_path, exist_ok=True)

    @staticmethod
    def normalize_key(key):
        if not key:
            return key
        return str(key).replace('/', '').replace('\\', '').replace(':', '').strip()

    def save(self, key, data):
        try:
            if not key:
                return
            key = self.normalize_key(key)
            path = os.path.join(self.base_path, f"{key}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.counter += 1
            print(f'[CACHE] Saved OK: "{key}" ({self.counter})')
        except Exception as exc:
            logging.error(f'Exception saving cache for key {key}: {exc}')

    def save_error(self, key, html_content):
        try:
            key = self.normalize_key(key)
            path = os.path.join(self.error_path, f"{key}.html")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.error_counter += 1
            print(f'[CACHE] Saved ERROR: "{key}" ({self.error_counter})')
        except Exception as exc:
            logging.error(f'Exception saving error cache for key {key}: {exc}')

    def exists(self, key):
        key = self.normalize_key(key)
        file_path = os.path.join(self.base_path, f"{key}.json")
        return os.path.exists(file_path)


####################################################
# Process a single URL in a single page
####################################################
def process_single(page, row, cache_manager, max_retries=2):
    key = row["Streets"]
    url = row["URLs"]

    if cache_manager.exists(key):
        print(f"[SKIP] Already cached: {key}")
        return

    for attempt in range(max_retries + 1):
        try:
            page.goto(url, timeout=15000)
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(5000)

            html = page.content()

            # Detect CAPTCHA or block page
            if "captcha" in html.lower() or "Enable JavaScript and cookies to continue" in html:
                cache_manager.save_error(key, html)
                return

            # Save success
            cache_manager.save(key, {"url": url, "html": html})
            return

        except Exception as exc:
            print(f"[ERROR] {url} attempt {attempt+1}: {exc}")
            if attempt == max_retries:
                cache_manager.save_error(key, f"Exception: {exc}")


####################################################
# MAIN (with concurrency)
####################################################
def main():

    input_file_path = "start_urls_181_200.csv"
    df = pd.read_csv(input_file_path, dtype=str, names=["Streets", "URLs"])
    rows = df.to_dict("records")

    basepath = input_file_path.replace("start_urls_", "").split(".csv")[0]
    cache_manager = CacheManager(base_path=basepath)

    # Proxy load
    with open("proxy_cred.json","r",encoding="utf-8") as f:
        proxy = json.load(f)

    NUM_PAGES = 10   # concurrent tabs

    # Camoufox config
    config = {
        'window.outerHeight': 1056,
        'window.outerWidth': 1920,
        'window.innerHeight': 1008,
        'window.innerWidth': 1920,
        'window.history.length': 4,
        'navigator.userAgent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
        'navigator.appCodeName': 'Mozilla',
        'navigator.appName': 'Netscape',
        'navigator.appVersion': '5.0 (Windows)',
        'navigator.oscpu': 'Windows NT 10.0; Win64; x64',
        'navigator.language': 'en-US',
        'navigator.languages': ['en-US'],
        'navigator.platform': 'Win32',
        'navigator.hardwareConcurrency': 12,
        'navigator.product': 'Gecko',
        'navigator.productSub': '20030107',
        'navigator.maxTouchPoints': 10,
    }

    with Camoufox(
        headless=True,                # HEADLESS = FASTER
        persistent_context=True,
        user_data_dir="user-data-dir",
        os=("windows"),
        config=config,
        i_know_what_im_doing=True,
        geoip=True,
        proxy=proxy
    ) as browser:

        # create pages (tabs)
        pages = [browser.new_page() for _ in range(NUM_PAGES)]

        # block heavy resources
        for page in pages:
            page.route("**/*", lambda route, request:
                route.abort() if request.resource_type in ["image","font","stylesheet"] else route.continue_()
            )

        total = len(rows)
        idx = 0

        print(f"\nStarting concurrent scraping: {NUM_PAGES} tabs for {total} URLs")

        while idx < total:

            # create batch of tasks = number of tabs
            active_jobs = []
            for page in pages:
                if idx >= total:
                    break
                row = rows[idx]
                active_jobs.append((page, row))
                idx += 1

            # run all jobs
            for page, row in active_jobs:
                process_single(page, row, cache_manager)

        for p in pages:
            p.close()


if __name__ == "__main__":
    main()
