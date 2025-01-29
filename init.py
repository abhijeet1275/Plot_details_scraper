# init.py

import os
import json
from config import OUTPUT_DIR, DP_STATE_FILE, LOG_FILE

def initialize_scraper():
    """
    Initialize fresh files and directories for the scraper.
    Creates new state file and village data directory.
    """
    # Create fresh output directory
    if os.path.exists(OUTPUT_DIR):
        # Remove old village data files
        for file in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
    else:
        os.makedirs(OUTPUT_DIR)

    # Create fresh state file
    initial_state = {
        "processed_sheets": {},
        "last_village": None,
        "last_sheet": None
    }
    
    with open(DP_STATE_FILE, 'w') as f:
        json.dump(initial_state, f, indent=4)

    # Create fresh log file
    with open(LOG_FILE, 'w') as f:
        f.write("")

    print("Initialized fresh scraper files and directories.")