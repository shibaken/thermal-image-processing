# Third-Party
import os
import logging
import subprocess
from datetime import datetime
# Local
from tipapp import settings

logger = logging.getLogger(__name__)

class ImportsProcessor():

    def __init__(self, source_path, dest_path):
        self.path = source_path
        self.dest_path = dest_path

    def process_files(self):
        logger.info(f"Processing pending Imports from : {self.path}")
        
        current_datetime = datetime.now().astimezone()
        seen_datetime = datetime.strftime(current_datetime, '%Y-%m-%d %H:%M:%S')
        BASE_DIR = settings.BASE_DIR
        try:
            if (not os.path.isabs(self.path)):
                self.path = os.path.join(BASE_DIR, self.path)
                
            for entry in os.scandir(self.path):
                filename = entry.name
                current_datetime = datetime.now().astimezone()
                seen_datetime = datetime.strftime(current_datetime, '%Y-%m-%d %H:%M:%S')

                # log watch
                logger.info (seen_datetime+" File to be processed: "+ str(entry.path))   

                # Case-insensitive check
                lower_filename = filename.lower()
                if lower_filename.endswith('.7z') or lower_filename.endswith('.zip'):
                    try:
                        script_path = os.path.join(BASE_DIR, 'thermalimageprocessing/thermal_image_processing.sh')
                        dest_path = os.path.join(BASE_DIR, self.dest_path)
                        logger.info("Destination folder "+str(dest_path))
                        result = subprocess.run(["/bin/bash", script_path, entry.path, dest_path], capture_output=True, text=True, check=True)
                        logger.info(result)
                    except subprocess.CalledProcessError as e:
                        # Capture specific error output from the subprocess
                        logger.error(f"Command failed with return code {e.returncode}")
                        logger.error(f"Stdout: {e.stdout}")
                        logger.error(f"Stderr: {e.stderr}") 
                    except Exception as e:
                        logger.error(f"Unexpected error: {e}")
        except Exception as e:
            logger.error(e)
