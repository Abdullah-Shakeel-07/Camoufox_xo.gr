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

        # Ensure directories exist
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
        """Save successful response"""
        try:
            if not key:
                return
            key = self.normalize_key(key)
            path = os.path.join(self.base_path, f"{key}.json")
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                self.counter += 1
                print(f'[CACHE] Saved response for "{key}" (Total saved: {self.counter})')
        except Exception as exc:
            logging.error(f'Exception saving cache for key {key}: {exc}')

    def save_error(self, key, html_content):
        """Save error response (e.g. CAPTCHA or human verification)"""
        try:
            key = self.normalize_key(key)
            path = os.path.join(self.error_path, f"{key}.html")
            with open(path, 'w', encoding='utf-8') as f:
                f.write(html_content)
                self.error_counter += 1
                print(f'[CACHE] Saved ERROR response for "{key}" (Error count: {self.error_counter})')
        except Exception as exc:
            logging.error(f'Exception saving error cache for key {key}: {exc}')

    def exists(self, key):
        """Check if successful response already exists"""
        key = self.normalize_key(key)
        file_path = os.path.join(self.base_path, f"{key}.json")
        return os.path.exists(file_path)

    def get(self, key):
        try:
            if not key:
                return None
            key = self.normalize_key(key)
            file_path = os.path.join(self.base_path, f"{key}.json")
            if not os.path.exists(file_path):
                return None
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as exc:
            logging.error(f'Exception reading cache for key {key}: {exc}')
            return None


####################################################
# PROXY PROVIDER
####################################################



####################################################
# MAIN SCRIPT
####################################################
def main():
    input_file_path = 'start_urls_181_200.csv'
    df = pd.read_csv(input_file_path, dtype=str, names=['Streets', 'URLs'])
    all_urls = df['URLs'].tolist()
    print(f"Total URLs to process: {len(all_urls)}")

    # Cache setup
    basepath = input_file_path.replace('start_urls_', '').split('.csv')[0]
    cache_manager = CacheManager(base_path=basepath)

    # Config for Camoufox
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

    # Proxy optional
    # proxy = get_proxy('geonode')
    proxy_path = 'proxy_cred.json'
    with open(proxy_path, 'r', encoding = 'utf-8') as f:
        data = f.read()
    proxy = json.loads(data)

    with Camoufox(
        headless=False, 
        persistent_context=True,
        user_data_dir='user-data-dir',
        os=('windows'),
        config=config,
        i_know_what_im_doing=True,
        geoip=True,
        proxy=proxy
    ) as browser:
        page = browser.new_page()

        for index, row in df.iterrows():
            key = row['Streets']
            url = row['URLs']

            # kip if already cached successfully
            if cache_manager.exists(key):
                print(f"[SKIP] Already cached: {key}")
                continue

            print(f"\nProcessing [{index + 1}/{len(df)}] => {key}")

            try:
                page.goto(url)
                page.wait_for_load_state('networkidle')
                page.wait_for_timeout(10000)
                
                html = page.content()

                # Check for bot or verification page
                if "Enable JavaScript and cookies to continue" in html or "captcha" in html.lower():
                    cache_manager.save_error(key, html)

                    if cache_manager.error_counter >= 5:
                        print("\nToo many verification errors (5). Stopping script for manual check.")
                        break
                    continue

                # Save successful response
                cache_manager.save(key, {"url": url, "html": html})

            except Exception as e:
                logging.error(f"Error processing {url}: {e}")
                cache_manager.save_error(key, f"Exception: {str(e)}")

                if cache_manager.error_counter >= 5:
                    print("\nToo many errors encountered. Stopping for inspection.")
                    break

        page.close()


if __name__ == '__main__':
    main()
