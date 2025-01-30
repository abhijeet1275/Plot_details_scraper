import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from typing import List, Dict, Union, Tuple
from main import find_villages
from collections import defaultdict

def get_sheet_number(gis_code: str) -> str:
    """Extract sheet number from GIS code (last two digits)"""
    try:
        return gis_code[-2:]
    except:
        return "00"  # Default sheet number if extraction fails

def calculate_sheet_bboxes(village_data: Dict) -> Dict[str, str]:
    """
    Calculate BBOX parameters for each sheet in the village.
    Returns dict of sheet numbers to BBOX strings.
    """
    # Group plots by sheet number
    sheet_plots = defaultdict(list)
    
    for plot in village_data['plots'].values():
        sheet_num = get_sheet_number(plot['gisCode'])
        sheet_plots[sheet_num].append(plot)
    
    # Calculate BBOX for each sheet
    sheet_bboxes = {}
    for sheet_num, plots in sheet_plots.items():
        if not plots:
            continue
            
        # Initialize with first plot's values
        xmin = plots[0]['xmin']
        ymin = plots[0]['ymin']
        xmax = plots[0]['xmax']
        ymax = plots[0]['ymax']
        
        # Find min/max coordinates for this sheet
        for plot in plots:
            xmin = min(xmin, plot['xmin'])
            ymin = min(ymin, plot['ymin'])
            xmax = max(xmax, plot['xmax'])
            ymax = max(ymax, plot['ymax'])
        
        sheet_bboxes[sheet_num] = f"{xmin},{ymin},{xmax},{ymax}"
    
    return sheet_bboxes

def download_village_plots(village_no: Union[str, int], max_workers: int = 20) -> Dict[str, int]:
    """
    Downloads plot images for a given village number using sheet-specific BBOXes.
    
    Args:
        village_no: Village number to process
        max_workers: Maximum number of concurrent downloads (default: 20)
    
    Returns:
        Dict with counts of successful and failed downloads
    """
    # Initialize session and parameters
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    # Load and process village data
    try:
        with open(f"village_data/village_{village_no}.json") as f:
            village_data = json.load(f)
            
        # Calculate BBOXes for each sheet
        sheet_bboxes = calculate_sheet_bboxes(village_data)
        print(f"Calculated BBOXes for {len(sheet_bboxes)} sheets in village {village_no}")
        
        # Group GIS codes by sheet
        sheet_gis_codes = defaultdict(list)
        for plot in village_data['plots'].values():
            sheet_num = get_sheet_number(plot['gisCode'])
            sheet_gis_codes[sheet_num].append(plot['gisCode'])
        
    except Exception as e:
        print(f"Error processing village data: {e}")
        return {"successful": 0, "failed": 0, "total_plots": 0}

    base_params = {
        "SERVICE": "WMS",
        "VERSION": "1.3.0",
        "REQUEST": "GetMap",
        "FORMAT": "image/png",
        "TRANSPARENT": "true",
        "LAYERS": "VILLAGE_MAP",
        "transparent": "true",
        "state": "21",
        "overlay_codes": "",
        "CRS": "EPSG:3857",
        "STYLES": "VILLAGE_MAP",
        "FORMAT_OPTIONS": "dpi:180",
        "WIDTH": "4698",
        "HEIGHT": "4086"
    }
    
    # Prepare output directory
    output_dir = f"images/village_{village_no}"
    os.makedirs(output_dir, exist_ok=True)
    
    def download_single_plot(gis_code: str, bbox: str) -> bool:
        """Helper function to download a single plot image with specific BBOX"""
        try:
            # Skip if file exists
            output_file = f"{output_dir}/{gis_code}.png"
            if os.path.exists(output_file):
                print(f"File exists: {gis_code}.png")
                return True
                
            # Prepare parameters and download
            params = base_params.copy()
            params['gis_code'] = gis_code
            params['BBOX'] = bbox
            
            time.sleep(0.2)  # Rate limiting
            response = session.get(
                "https://app1bhunakshaodisha.nic.in/bhunaksha/WMS",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            # Save image
            with open(output_file, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {gis_code}.png (Sheet: {get_sheet_number(gis_code)})")
            return True
            
        except Exception as e:
            print(f"Failed to download {gis_code}: {e}")
            return False
    
    # Download plots using thread pool, grouped by sheet
    successful = 0
    failed = 0
    total_plots = sum(len(codes) for codes in sheet_gis_codes.values())
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        # Submit downloads for each sheet with its specific BBOX
        for sheet_num, gis_codes in sheet_gis_codes.items():
            if sheet_num not in sheet_bboxes:
                print(f"Warning: No BBOX found for sheet {sheet_num}")
                continue
                
            bbox = sheet_bboxes[sheet_num]
            print(f"Processing sheet {sheet_num} with BBOX: {bbox}")
            
            for gis_code in gis_codes:
                futures.append(
                    executor.submit(download_single_plot, gis_code, bbox)
                )
        
        for future in as_completed(futures):
            if future.result():
                successful += 1
            else:
                failed += 1
    
    return {
        "successful": successful,
        "failed": failed,
        "total_plots": total_plots
    }

if __name__ == "__main__":
    village_list = find_villages(1,1,2)
    for village in village_list:
        print(f"\nProcessing village {village}")
        results = download_village_plots(village)
        print(f"Results for village {village}:")
        print(f"- Successful downloads: {results['successful']}")
        print(f"- Failed downloads: {results['failed']}")
        print(f"- Total plots: {results['total_plots']}")