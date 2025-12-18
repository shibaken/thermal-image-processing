# Third-Party
import os
import logging
import subprocess
from datetime import datetime
# Local
from tipapp import settings
from thermalimageprocessing.thermal_image_processing import unzip_and_prepare, run_thermal_processing

logger = logging.getLogger(__name__)

class ImportsProcessor():

    def __init__(self, source_path, dest_path):
        self.source_path = source_path
        self.history_path = dest_path

    def process_files(self):
        logger.info(f"Processing pending Imports from : {self.source_path}")
        
        try:
            for entry in os.scandir(self.source_path):
                filename = entry.name

                # log watch
                logger.info ("File to be processed: " + str(entry.path))   

                # Case-insensitive check
                lower_filename = filename.lower()
                if lower_filename.endswith(('.7z', '.zip')):
                    try:
                        # =========================================================
                        # Call Python functions directly instead of .sh
                        # =========================================================
                        logger.info(f"Starting direct Python processing for: {filename}")
                        
                        # 1. Unzip and Prepare (Replaces shell script logic)
                        # entry.path: The full path to the pending .7z file
                        # dest_path: Where to move the original .7z file after extraction
                        processed_dir_path = unzip_and_prepare(entry.path)
                        
                        logger.info(f"Unzipped and prepared at: {processed_dir_path}")
                        
                        # 2. Run Main Thermal Processing
                        # This runs the GDAL/PostGIS/GeoServer pipeline
                        run_thermal_processing(processed_dir_path)
                        
                        logger.info(f"Successfully finished processing: {filename}")

                    # Since we are running python code directly, we catch standard Exceptions
                    except Exception as e:
                        logger.error(f"Error processing file {filename}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"A critical error occurred in ImportsProcessor: {e}", exc_info=True)
