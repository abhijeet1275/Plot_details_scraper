# utils.py

import json
import os
import logging
from datetime import datetime
from config import OUTPUT_DIR, DP_STATE_FILE, LOG_FILE

def setup_logging():
    """Configure logging for the scraper"""
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'  # This will overwrite the log file each time
    )
    # Also log to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def ensure_directories():
    """Create necessary directories if they don't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def load_state():
    """Load the scraper state from file or create new if doesn't exist"""
    try:
        if os.path.exists(DP_STATE_FILE):
            with open(DP_STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading state file: {e}")
    
    # Return fresh state if file doesn't exist or error occurs
    return {
        "processed_sheets": {},
        "last_village": None,
        "last_sheet": None
    }

def save_state(state_data):
    """Save the current scraper state"""
    try:
        with open(DP_STATE_FILE, 'w') as f:
            json.dump(state_data, f, indent=4)
        logging.info("State saved successfully")
    except Exception as e:
        logging.error(f"Error saving state: {e}")

def save_village_data(village_no, data):
    """Save village data with metadata to JSON file"""
    filename = os.path.join(OUTPUT_DIR, f"village_{village_no}.json")
    try:
        output_data = {
            "metadata": {
                "village_number": village_no,
                "timestamp": datetime.now().isoformat(),
                "total_plots": len(data),
                "source": "BhuNaksha Odisha"
            },
            "plots": data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Saved data for village {village_no}")
        return True
    except Exception as e:
        logging.error(f"Error saving village data: {e}")
        return False

def load_village_data(village_no):
    """Load existing village data if available"""
    filename = os.path.join(OUTPUT_DIR, f"village_{village_no}.json")
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                data = json.load(f)
                return data.get("plots", {})
    except Exception as e:
        logging.error(f"Error loading village data: {e}")
    return {}