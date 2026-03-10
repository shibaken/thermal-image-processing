"""
Management command to download the latest DBCA districts spatial file from
Kaartdijin Boodja (KB) and replace the local copy used for district name lookups.

The download URL can be overridden via the environment variable
`general_districts_kb_url`. The destination path is read from
`general_districts_dataset_name`.

Intended to be run from cron, e.g. once daily.
"""

import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path

import requests
from django.core.management import base

from tipapp import settings

logger = logging.getLogger(__name__)


class DistrictsLayerSync:
    """Downloads the DBCA districts GeoPackage from Kaartdijin Boodja (KB)
    and replaces the local copy atomically."""

    def __init__(self, url, dest_path):
        self.url = url
        self.dest_path = dest_path

    def run_sync(self):
        current_datetime = datetime.now().astimezone()
        logger.info(f"Syncing DBCA districts layer from KB {datetime.strftime(current_datetime, '%Y-%m-%d %H:%M:%S')}")
        try:
            tmp_path, error = self.download_layer(self.url)
            if error:
                logger.error(f"Error downloading layer {self.url}: {error}")
                return False

            # Validate: a valid GeoPackage (SQLite) starts with these magic bytes
            with open(tmp_path, "rb") as f:
                header = f.read(16)
            if not header.startswith(b"SQLite format 3"):
                os.remove(tmp_path)
                logger.error(
                    f"Downloaded file does not appear to be a valid GeoPackage "
                    f"(unexpected header: {header[:16]!r})"
                )
                return False

            size_kb = os.path.getsize(tmp_path) // 1024
            os.replace(tmp_path, self.dest_path)  # atomic replace on POSIX
            logger.info(f"Districts layer updated successfully ({size_kb} KB): {self.dest_path}")
            return True

        except Exception as e:
            logger.error(e)
            return False

    def download_layer(self, url):
        dest_dir = Path(self.dest_path).parent
        try:
            dest_dir.mkdir(parents=True, exist_ok=True)
            tmp_fd, tmp_path = tempfile.mkstemp(dir=dest_dir, suffix=".gpkg.tmp")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with os.fdopen(tmp_fd, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(f"Layer downloaded and saved as {tmp_path}")
            return tmp_path, None
        except Exception as e:
            return None, f"Error downloading layer: {str(e)}"


class Command(base.BaseCommand):
    help = "Download the latest DBCA districts gpkg from KB and update the local file."

    def handle(self, *args, **kwargs):
        url = settings.DISTRICTS_KB_URL
        dest_path = settings.DISTRICTS_GPKG_PATH

        if not dest_path:
            msg = (
                "general_districts_dataset_name is not set. "
                "Cannot determine destination path for district file."
            )
            logger.error(msg)
            self.stderr.write(self.style.ERROR(msg))
            return

        self.stdout.write(f"Syncing districts file from KB...")
        self.stdout.write(f"  URL:  {url}")
        self.stdout.write(f"  Dest: {dest_path}")

        success = DistrictsLayerSync(url=url, dest_path=dest_path).run_sync()
        if success:
            self.stdout.write(self.style.SUCCESS("Districts file updated successfully."))
        else:
            self.stderr.write(self.style.ERROR("Failed to sync districts file. Check the logs for details."))
