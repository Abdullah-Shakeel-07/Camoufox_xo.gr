import os
import json
import logging
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

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
    
    def get_data(self, key):
        key = self.normalize_key(key)
        path = os.path.join(self.base_path, f"{key}.json")
        if not os.path.exists(path):
            print(f'[CACHE] Not Found "{key}"')
            return None
        print(f'[CACHE] Found "{key}"')
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
def parse_data(data):
    data = json.loads(data['html'].split('<pre>')[-1].split('</pre>')[0])
    return data["ADDRLIST"]
    
def main():

    error_df = pd.DataFrame(columns=["Streets", "URLs"])
    missing_df = pd.DataFrame(columns=["Streets", "URLs"])
    
    location_rows = []
    
    input_file_path = "start_urls_181_200.csv"
    df = pd.read_csv(input_file_path, dtype=str, names=["Streets", "URLs"])

    basepath = input_file_path.replace("start_urls_", "").split(".csv")[0]
    cache_manager = CacheManager(base_path=basepath)
    
    for index, row in df.iterrows():
        pass_through = {'Query': row["Streets"], 'Url': row["URLs"]}
        
        key = row["Streets"]
        if cache_manager.exists(key):
            data = cache_manager.get_data(key)
            try:
                data = parse_data(data)
                # add pass through fields
                for item in data:
                    item.update(pass_through)
                
                location_rows.extend(data)
            except:
                # write to error dataframe
                error_df = error_df.append(row, ignore_index=True)
        else:
            print(f'No cached data for key "{key}".')
            # write to missing dataframe
            missing_df = missing_df.append(row, ignore_index=True)
    
    location_df = pd.DataFrame(location_rows)
    location_df.to_csv(f"feed_{basepath}.csv", index=False)
    error_df.to_csv(f"errors_{basepath}.csv", index=False, header=False)
    missing_df.to_csv(f"missing_{basepath}.csv", index=False, header=False)
    
if __name__ == "__main__":
    main()