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
        self.path = source_path
        self.dest_path = dest_path

    def process_files(self):
        logger.info(f"Processing pending Imports from : {self.path}")
        
        BASE_DIR = settings.BASE_DIR
        try:
            if (not os.path.isabs(self.path)):
                self.path = os.path.join(BASE_DIR, self.path)
                
            for entry in os.scandir(self.path):
                filename = entry.name

                # log watch
                logger.info ("File to be processed: " + str(entry.path))   

                # Case-insensitive check
                lower_filename = filename.lower()
                if lower_filename.endswith('.7z') or lower_filename.endswith('.zip'):
                    # try:
                    #     script_path = os.path.join(BASE_DIR, 'thermalimageprocessing/thermal_image_processing.sh')
                    #     dest_path = os.path.join(BASE_DIR, self.dest_path)
                    #     dest_path = os.path.normpath(dest_path)
                    #     logger.info("Destination folder "+str(dest_path))
                    #     result = subprocess.run(["/bin/bash", script_path, entry.path, dest_path], capture_output=True, text=True, check=True)
                    #     if result.stdout:
                    #         logger.info("--- STDOUT ---\n" + result.stdout)
                    #     if result.stderr:
                    #         logger.error("--- STDERR ---\n" + result.stderr)
                    # except subprocess.CalledProcessError as e:
                    #     # Capture specific error output from the subprocess
                    #     logger.error(f"Command failed with return code {e.returncode}")
                    #     logger.error(f"Stdout: {e.stdout}")
                    #     logger.error(f"Stderr: {e.stderr}") 
                    # except Exception as e:
                    #     logger.error(f"Unexpected error: {e}")
                    try:
                        # =========================================================
                        # REFACTOR: Call Python functions directly instead of .sh
                        # =========================================================
                        
                        # Prepare destination path for the history/uploads folder
                        dest_path = os.path.join(BASE_DIR, self.dest_path)
                        dest_path = os.path.normpath(dest_path)
                        
                        logger.info(f"Starting direct Python processing for: {filename}")
                        
                        # 1. Unzip and Prepare (Replaces shell script logic)
                        # entry.path: The full path to the pending .7z file
                        # dest_path: Where to move the original .7z file after extraction
                        processed_dir_path = unzip_and_prepare(entry.path, dest_path)
                        
                        logger.info(f"Unzipped and prepared at: {processed_dir_path}")
                        
                        # 2. Run Main Thermal Processing
                        # This runs the GDAL/PostGIS/GeoServer pipeline
                        run_thermal_processing(processed_dir_path)
                        
                        logger.info(f"Successfully finished processing: {filename}")

                    # Since we are running python code directly, we catch standard Exceptions
                    except Exception as e:
                        logger.error(f"Error processing file {filename}: {e}", exc_info=True)
        except Exception as e:
            logger.error(e)
