# scraper.py

import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from config import *
from utils import save_village_data, save_state, load_state

class VillageScraper:
    def __init__(self):
        self.visited_plots = {}
        self.state = load_state()

    def fetch_plot_data(self, village_no, sheet_no, plot_no, vil_json):
        """
        Fetch data for a single plot
        
        Args:
            village_no (str): Village number
            sheet_no (str): Sheet number
            plot_no (int): Plot number
            vil_json (dict): Dictionary to store plot data
            
        Returns:
            dict: Result of the fetch operation
        """
        if plot_no in self.visited_plots.get(village_no, set()):
            return {"success": False, "has_data": "N", "plot_no": plot_no}

        params = BASE_PARAMS.copy()
        params["levels"] = f"1,1,2,{village_no},{sheet_no},"
        params["plotno"] = plot_no

        try:
            response = requests.get(API_URL, params=params, timeout=TIMEOUT_SECONDS)
            
            if response.status_code != 200:
                logging.error(f"Request failed for village {village_no}, sheet {sheet_no}, plot {plot_no}: Status {response.status_code}")
                return {"success": False, "has_data": "N", "plot_no": plot_no}
            
            response_data = response.json()
            has_data = response_data.get("has_data", "N")
            
            if has_data == "Y":
                if village_no not in vil_json:
                    vil_json[village_no] = {}

                vil_json[village_no][str(plot_no)] = {
                    "plot_no": plot_no,
                    "xmax": response_data["xmax"],
                    "xmin": response_data["xmin"],
                    "ymin": response_data["ymin"],
                    "ymax": response_data["ymax"],
                    "center_x": response_data["center_x"],
                    "center_y": response_data["center_y"],
                    "gisCode": response_data["gisCode"]
                }
                logging.info(f"Found Plot {plot_no} in Village {village_no}, Sheet {sheet_no}")

                if village_no not in self.visited_plots:
                    self.visited_plots[village_no] = set()
                self.visited_plots[village_no].add(plot_no)

            return {
                "success": True, 
                "has_data": has_data, 
                "plot_no": plot_no, 
                "max_plot": plot_no if has_data == "Y" else 0
            }

        except requests.exceptions.Timeout:
            logging.error(f"Timeout for village {village_no}, sheet {sheet_no}, plot {plot_no}")
        except Exception as e:
            logging.error(f"Error processing plot: {e}")

        return {"success": False, "has_data": "N", "plot_no": plot_no}

    def process_sheet(self, village_no, sheet_no, vil_json):
        """Process all plots in a sheet"""
        if sheet_no in self.state["processed_sheets"].get(village_no, {}):
            logging.info(f"Sheet {sheet_no} in Village {village_no} already processed. Skipping...")
            return

        consecutive_empty = 0
        current_plot = 1
        max_plot_found = 0

        while consecutive_empty < MAX_CONSECUTIVE_EMPTY:
            logging.info(f"Processing plots {current_plot} to {current_plot + BATCH_SIZE - 1}")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self.fetch_plot_data, 
                        village_no, 
                        sheet_no, 
                        plot_no, 
                        vil_json
                    ): plot_no 
                    for plot_no in range(current_plot, current_plot + BATCH_SIZE)
                }

                for future in as_completed(futures):
                    result = future.result()
                    if result["success"]:
                        if result["has_data"] == "Y":
                            consecutive_empty = 0
                            max_plot_found = max(max_plot_found, result["max_plot"])
                        else:
                            consecutive_empty += 1
                    else:
                        consecutive_empty += 1

            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                logging.info(f"Reached {MAX_CONSECUTIVE_EMPTY} consecutive empty plots. Moving to next sheet.")
                break

            current_plot += BATCH_SIZE
            time.sleep(1)  # Rate limiting

        if village_no not in self.state["processed_sheets"]:
            self.state["processed_sheets"][village_no] = {}
        self.state["processed_sheets"][village_no][sheet_no] = max_plot_found
        save_state(self.state)

    def get_sheet_numbers(self, village_no):
        """Get all sheet numbers for a village"""
        params = SHEET_PARAMS.copy()
        params["selections"] = f"1,1,2,{village_no},"
        
        try:
            response = requests.get(API_URL, params=params, timeout=TIMEOUT_SECONDS)
            soup = BeautifulSoup(response.text, "html.parser")
            select_tag = soup.find("select", {"id": "level_5"})
            return [option["value"] for option in select_tag.find_all("option") if option["value"].isdigit()] if select_tag else []
        except Exception as e:
            logging.error(f"Error getting sheet numbers for village {village_no}: {e}")
            return []

    def scrape_village(self, village_no):
        """
        Scrape all plot data for a village
        
        Args:
            village_no (str): Village number to scrape
            
        Returns:
            dict: Scraped village data if successful, None if failed
        """
        vil_json = {village_no: {}}
        
        try:
            sheet_nos = self.get_sheet_numbers(village_no)
            logging.info(f"Found sheets for village {village_no}: {sheet_nos}")

            for sheet_no in sheet_nos:
                logging.info(f"Processing sheet {sheet_no}")
                self.process_sheet(village_no, sheet_no, vil_json)
                save_village_data(village_no, vil_json[village_no])
                time.sleep(2)  # Rate limiting between sheets

            return vil_json[village_no]

        except Exception as e:
            logging.error(f"Error scraping village {village_no}: {e}")
            save_village_data(f"{village_no}_error", vil_json.get(village_no, {}))
            return None