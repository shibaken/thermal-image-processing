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
        print(f"Starting to process pending imports from: {self.source_path}")

        try:
            # --- Get a list of files to process first for better feedback ---
            # We check for files ending with .7z or .zip, case-insensitively.
            files_to_process = [
                entry for entry in os.scandir(self.source_path) 
                if entry.is_file() and entry.name.lower().endswith(('.7z', '.zip'))
            ]
            
            if not files_to_process:
                print("\nNo new .7z or .zip files found in the import directory. Nothing to do.")
                logger.info("No new files found to process.")
                return

            print(f"\nFound {len(files_to_process)} file(s) to process.")
            print("-" * 50) # A separator for readability

            for entry in files_to_process:
                filename = entry.name

                # log watch
                print(f"\nProcessing file: {filename}")
                logger.info ("File to be processed: " + str(entry.path))   

                try:
                    # =========================================================
                    # Call Python functions directly instead of .sh
                    # =========================================================
                    print(f"  -> Starting file preparation (unzip and move)...")
                    logger.info(f"Starting direct Python processing for: {filename}")
                    
                    # 1. Unzip and Prepare (Replaces shell script logic)
                    # entry.path: The full path to the pending .7z file
                    # dest_path: Where to move the original .7z file after extraction
                    processed_dir_path = unzip_and_prepare(entry.path)
                    
                    print(f"  -> File successfully unzipped to: {processed_dir_path}")
                    logger.info(f"Unzipped and prepared at: {processed_dir_path}")
                    
                    # 2. Run Main Thermal Processing
                    # This runs the GDAL/PostGIS/GeoServer pipeline
                    print(f"  -> Starting main thermal processing pipeline...")
                    run_thermal_processing(processed_dir_path)
                    
                    print(f"  -> Thermal processing pipeline completed successfully.")
                    logger.info(f"Successfully finished processing for: {filename}")
                    
                    print(f"  => SUCCESS: Finished processing {filename}")

                # Since we are running python code directly, we catch standard Exceptions
                except Exception as e:
                    print(f"  => ERROR: An error occurred while processing {filename}: {e}")
                    logger.error(f"Error processing file {filename}: {e}", exc_info=True)

            print("-" * 50)
            print("All pending files have been processed.")

        except Exception as e:
            print(f"\nA critical error occurred: {e}")
            logger.error(f"A critical error occurred in ImportsProcessor: {e}", exc_info=True)
