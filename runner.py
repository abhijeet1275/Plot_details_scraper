# runner.py

from utils import setup_logging, ensure_directories
from scraper import VillageScraper
from init import initialize_scraper
import logging

def extract_village_data(village_number, fresh_start=True):
    """
    Extract plot details for a specific village and save to JSON file.
    
    Args:
        village_number (str): The village number to scrape
        fresh_start (bool): Whether to initialize fresh files before starting
        
    Returns:
        dict: The scraped data if successful, None if failed
    """
    if fresh_start:
        # Initialize fresh files
        initialize_scraper()
    
    # Setup logging and directories
    setup_logging()
    ensure_directories()
    
    # Initialize scraper
    scraper = VillageScraper()
    logging.info(f"Starting scrape for village {village_number}")
    
    try:
        # Scrape village data
        data = scraper.scrape_village(village_number)
        
        if data:
            logging.info(f"Successfully scraped village {village_number}")
            logging.info(f"Total plots found: {len(data)}")
            return data
        else:
            logging.error(f"Failed to scrape village {village_number}")
            return None
            
    except Exception as e:
        logging.error(f"Error while scraping village {village_number}: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    village_no = "38"  # Replace with your village number
    result = extract_village_data(village_no, fresh_start=True)
    if result:
        print(f"Successfully extracted data for village {village_no}")
        print(f"Number of plots found: {len(result)}")
    else:
        print(f"Failed to extract data for village {village_no}")