import gzip
import json
import logging
import os
import sys
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
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)

def run_preflight(conn):
    logging.debug("Verifying environment and database schema...")
    
    if not os.path.exists(FILE_PATH):
        logging.critical(f"Data file missing at {FILE_PATH}")
        return False

    cursor = conn.cursor()
    schema = os.getenv('DB_SCHEMA', 'public')
    check_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = %s 
        AND table_name = 'off_data_products' 
        AND column_name IN ('nutriscore_grade', 'ecoscore_grade');
    """
    cursor.execute(check_query, (schema,))
    columns = cursor.fetchall()
    
    for col_name, dtype in columns:
        if 'char' in dtype.lower() and 'varying' not in dtype.lower() and 'text' not in dtype.lower():
            logging.critical(f"Column {col_name} is {dtype}. Run ALTER TABLE to change to TEXT.")
            return False
    
    logging.info("PRE-FLIGHT PASSED: Database schema is ready.")
    
    if sys.stdin.isatty():
        try:
            confirm = input("Proceed with 15GB import? (y/n): ")
            return confirm.lower() == 'y'
        except EOFError:
            return True
    return True

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

def log_single_error(code, error_reason):
    try:
        error_entry = {
            "timestamp": datetime.now().isoformat(),
            "error": str(error_reason),
            "product_code": code
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

def insert_individual_rows(cursor, conn, buffers):
    local_success = 0
    local_fail = 0
    logging.debug("Switching to individual insertion mode for failed batch...")
    conn.rollback() 
    
    for i in range(len(buffers["products"])):
        try:
            p = buffers["products"][i]
            code = p[0]
            cursor.execute("""
                INSERT INTO off_data_products 
                (code, url, creator, created_t, last_modified_t, product_name, generic_name, quantity, packaging, brands, categories, origins, labels, countries, ingredients_text, allergens, traces, serving_size, nutriscore_score, nutriscore_grade, nova_group, image_url)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (code) DO NOTHING
            """, p)
            
            if buffers["brands"]:
                b_data = [b for b in buffers["brands"] if b[0] == code]
                if b_data: execute_values(cursor, "INSERT INTO off_data_brands (product_code, brand) VALUES %s", b_data)
            
            if buffers["categories"]:
                c_data = [c for c in buffers["categories"] if c[0] == code]
                if c_data: execute_values(cursor, "INSERT INTO off_data_categories (product_code, category) VALUES %s", c_data)

            if buffers["nutriments"]:
                n_data = [n for n in buffers["nutriments"] if n[0] == code]
                if n_data: execute_values(cursor, "INSERT INTO off_data_nutriments (product_code, nutrient_id, value_100g) VALUES %s", n_data)
            
            conn.commit()
            local_success += 1
        except psycopg2.Error as e:
            conn.rollback()
            local_fail += 1
            log_single_error(code, e)
            
    return local_success, local_fail

def process_import():
    state = load_state()
    lines_processed = state["lines_processed"]
    success_count = state["success_count"]
    fail_count = state["fail_count"]
    total_size = os.path.getsize(FILE_PATH)
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        if not run_preflight(conn):
            return

        cursor = conn.cursor()
        buffers = {k: [] for k in ["products", "brands", "categories", "nutriments"]}

        with gzip.open(FILE_PATH, 'rt', encoding='utf-8') as f:
            if lines_processed > 0:
                logging.info(f"Fast-forwarding to line {lines_processed}...")
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
                        if buffers["nutriments"]: execute_values(cursor, "INSERT INTO off_data_nutriments (product_code, nutrient_id, value_100g) VALUES %s", buffers["nutriments"])
                        conn.commit()
                        success_count += len(buffers["products"])
                    except psycopg2.Error:
                        s, f_c = insert_individual_rows(cursor, conn, buffers)
                        success_count += s
                        fail_count += f_c

                    state.update({"lines_processed": lines_processed, "success_count": success_count, "fail_count": fail_count})
                    save_state(state)
                    for k in buffers: buffers[k].clear()
                    
                    current_bytes = f.buffer.tell() if hasattr(f, 'buffer') else 0
                    progress = (current_bytes / total_size) * 100
                    logging.info(f"Progress: {progress:.2f}% | Processed: {lines_processed} | Success: {success_count} | Fail: {fail_count}")

    except KeyboardInterrupt:
        logging.warning("User interrupted.")
    except Exception as e:
        logging.critical(f"FATAL: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    process_import()