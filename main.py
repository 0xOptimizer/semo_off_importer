import gzip
import json
import logging
import os
import sys
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

try:
    DB_CONFIG = {
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "options": f"-c search_path={os.getenv('DB_SCHEMA', 'public')}"
    }
    FILE_PATH = os.environ["DATA_FILE_PATH"]
    STATE_FILE = os.environ.get("STATE_FILE", "import_state.json")
    ERROR_LOG_FILE = os.environ.get("ERROR_LOG_FILE", "import_errors.jsonl")
    LOG_FILE = os.environ.get("LOG_FILE", "process_output.log")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 2000))
except KeyError as e:
    sys.exit(f"Critical Error: Missing environment variable {e}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error("State file corrupted.")
    return {"lines_processed": 0, "success_count": 0, "fail_count": 0}

def save_state(state):
    try:
        temp_file = f"{STATE_FILE}.tmp"
        with open(temp_file, 'w') as f:
            json.dump(state, f)
        os.replace(temp_file, STATE_FILE)
    except IOError as e:
        logging.error(f"Failed to save state: {e}")

def log_batch_error(batch_data, error_reason):
    try:
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "error": str(error_reason),
            "batch_size": len(batch_data),
            "failed_codes": [item[0] for item in batch_data] 
        }
        with open(ERROR_LOG_FILE, 'a') as f:
            f.write(json.dumps(error_entry) + "\n")
    except Exception as e:
        logging.error(f"Failed to write to error log: {e}")

def clean_str(val): return val.strip() if isinstance(val, str) and val.strip() else None

def clean_int(val): 
    try: return int(val)
    except (ValueError, TypeError): return None

def clean_real(val):
    try: return float(val)
    except (ValueError, TypeError): return None

def parse_list(val):
    if isinstance(val, list): return [clean_str(x) for x in val if x]
    if isinstance(val, str): return [clean_str(x) for x in val.split(',') if x]
    return []

def process_import():
    state = load_state()
    lines_processed = state["lines_processed"]
    success_count = state["success_count"]
    fail_count = state["fail_count"]
    
    if not os.path.exists(FILE_PATH):
        logging.critical(f"Data file not found: {FILE_PATH}")
        sys.exit(1)
        
    total_size = os.path.getsize(FILE_PATH)
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False 
        cursor = conn.cursor()
        
        logging.info(f"Resuming. Processed: {lines_processed} | Success: {success_count} | Failures: {fail_count}")

        buffers = {
            "products": [], "brands": [], "categories": [], "nutriments": [],
            "labels": [], "allergens": [], "countries": [], "additives": []
        }

        with gzip.open(FILE_PATH, 'rt', encoding='utf-8') as f:
            if lines_processed > 0:
                for _ in range(lines_processed):
                    next(f)

            for line in f:
                lines_processed += 1
                
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue 

                code = clean_str(row.get('code'))
                if not code: continue

                buffers["products"].append((
                    code, clean_str(row.get('url')), clean_str(row.get('creator')),
                    clean_int(row.get('created_t')), clean_int(row.get('last_modified_t')),
                    clean_str(row.get('product_name')), clean_str(row.get('generic_name')),
                    clean_str(row.get('quantity')), clean_str(row.get('packaging')),
                    clean_str(row.get('brands')), clean_str(row.get('categories')),
                    clean_str(row.get('origins')), clean_str(row.get('labels')),
                    clean_str(row.get('countries')), clean_str(row.get('ingredients_text')),
                    clean_str(row.get('allergens')), clean_str(row.get('traces')),
                    clean_str(row.get('serving_size')), clean_int(row.get('nutriscore_score')),
                    clean_str(row.get('nutriscore_grade')), clean_int(row.get('nova_group')),
                    clean_str(row.get('image_url'))
                ))

                for b in parse_list(row.get('brands_tags')): buffers["brands"].append((code, b))
                for c in parse_list(row.get('categories_tags')): buffers["categories"].append((code, c))
                for l in parse_list(row.get('labels_tags')): buffers["labels"].append((code, l))
                for a in parse_list(row.get('allergens_tags')): buffers["allergens"].append((code, a))
                for cnt in parse_list(row.get('countries_tags')): buffers["countries"].append((code, cnt))
                for add in parse_list(row.get('additives_tags')): buffers["additives"].append((code, add))

                nutriments = row.get('nutriments', {})
                if isinstance(nutriments, dict):
                    for k, v in nutriments.items():
                        if k.endswith('_100g'):
                            val = clean_real(v)
                            if val is not None:
                                buffers["nutriments"].append((code, k.replace('_100g', ''), val))

                if len(buffers["products"]) >= BATCH_SIZE:
                    try:
                        execute_values(cursor, """
                            INSERT INTO off_data_products 
                            (code, url, creator, created_t, last_modified_t, product_name, generic_name, quantity, packaging, brands, categories, origins, labels, countries, ingredients_text, allergens, traces, serving_size, nutriscore_score, nutriscore_grade, nova_group, image_url)
                            VALUES %s ON CONFLICT (code) DO NOTHING
                        """, buffers["products"])
                        
                        if buffers["brands"]: execute_values(cursor, "INSERT INTO off_data_brands (product_code, brand) VALUES %s", buffers["brands"])
                        if buffers["categories"]: execute_values(cursor, "INSERT INTO off_data_categories (product_code, category) VALUES %s", buffers["categories"])
                        if buffers["labels"]: execute_values(cursor, "INSERT INTO off_data_labels (product_code, label) VALUES %s", buffers["labels"])
                        if buffers["allergens"]: execute_values(cursor, "INSERT INTO off_data_allergens (product_code, allergen) VALUES %s", buffers["allergens"])
                        if buffers["countries"]: execute_values(cursor, "INSERT INTO off_data_countries (product_code, country) VALUES %s", buffers["countries"])
                        if buffers["additives"]: execute_values(cursor, "INSERT INTO off_data_additives (product_code, additive) VALUES %s", buffers["additives"])
                        if buffers["nutriments"]: execute_values(cursor, "INSERT INTO off_data_nutriments (product_code, nutrient_id, value_100g) VALUES %s", buffers["nutriments"])
                        
                        conn.commit()
                        success_count += len(buffers["products"])
                        
                        state.update({"lines_processed": lines_processed, "success_count": success_count, "fail_count": fail_count})
                        save_state(state)

                        current_bytes = f.fileobj.tell()
                        progress = (current_bytes / total_size) * 100
                        logging.info(f"Progress: {progress:.2f}% | Processed: {lines_processed} | Success: {success_count} | Fail: {fail_count}")

                    except psycopg2.Error as db_err:
                        conn.rollback()
                        fail_count += len(buffers["products"])
                        logging.error(f"Batch failed: {db_err}")
                        log_batch_error(buffers["products"], db_err)
                    
                    for k in buffers: buffers[k].clear()

            if buffers["products"]:
                try:
                    execute_values(cursor, """
                            INSERT INTO off_data_products 
                            (code, url, creator, created_t, last_modified_t, product_name, generic_name, quantity, packaging, brands, categories, origins, labels, countries, ingredients_text, allergens, traces, serving_size, nutriscore_score, nutriscore_grade, nova_group, image_url)
                            VALUES %s ON CONFLICT (code) DO NOTHING
                    """, buffers["products"])
                    if buffers["brands"]: execute_values(cursor, "INSERT INTO off_data_brands (product_code, brand) VALUES %s", buffers["brands"])
                    if buffers["categories"]: execute_values(cursor, "INSERT INTO off_data_categories (product_code, category) VALUES %s", buffers["categories"])
                    if buffers["labels"]: execute_values(cursor, "INSERT INTO off_data_labels (product_code, label) VALUES %s", buffers["labels"])
                    if buffers["allergens"]: execute_values(cursor, "INSERT INTO off_data_allergens (product_code, allergen) VALUES %s", buffers["allergens"])
                    if buffers["countries"]: execute_values(cursor, "INSERT INTO off_data_countries (product_code, country) VALUES %s", buffers["countries"])
                    if buffers["additives"]: execute_values(cursor, "INSERT INTO off_data_additives (product_code, additive) VALUES %s", buffers["additives"])
                    if buffers["nutriments"]: execute_values(cursor, "INSERT INTO off_data_nutriments (product_code, nutrient_id, value_100g) VALUES %s", buffers["nutriments"])
                    
                    conn.commit()
                    success_count += len(buffers["products"])
                except psycopg2.Error as e:
                    conn.rollback()
                    log_batch_error(buffers["products"], e)

            state.update({"lines_processed": lines_processed, "success_count": success_count, "fail_count": fail_count})
            save_state(state)

    except KeyboardInterrupt:
        logging.warning("Interrupted.")
    except Exception as e:
        logging.critical(f"Failure: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    process_import()