# config.py

API_URL = "https://app1bhunakshaodisha.nic.in/bhunaksha/ScalarDatahandler"
BASE_PARAMS = {"OP": "5", "state": "21"}
SHEET_PARAMS = {"OP": "2", "level": "5", "state": "21"}

# Scraping Configuration
MAX_CONSECUTIVE_EMPTY = 1000
TIMEOUT_SECONDS = 10
BATCH_SIZE = 200
MAX_WORKERS = 200

# File and Directory Configuration
OUTPUT_DIR = "village_data"
DP_STATE_FILE = "scraper_state.json"  # Added this line
LOG_FILE = "scraper.log"