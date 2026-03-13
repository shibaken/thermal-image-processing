"""
Management command to process jobs queued for retirement.

Picks up jobs in RETIRE_QUEUED status one at a time and performs the full
retire sequence:
  1. Move the processed data folder to the retired archive
  2. Delete GeoServer Coverage Stores (mosaic + individual images)
  3. Delete TIF files from GeoServer storage (rclone mount)
  4. Delete PostGIS records (footprints, boundaries, centroids)
  5. Mark the job as RETIRED

If any unrecoverable error occurs the job is marked RETIRE_FAILED so the
operator can investigate and retry.

Registered as a django-cron job (ProcessRetireQueueCronJob) running every
minute. Can also be invoked manually via:
  python manage.py process_retire_queue_command
"""

import logging
import os
import shutil

import requests as http_requests
from django.core.management import base
from django.utils import timezone
from django_cron import CronJobBase, Schedule
from sqlalchemy import create_engine, text

from tipapp import settings

logger = logging.getLogger(__name__)


def _process_retire_queue(stdout=None):
    """Query all RETIRE_QUEUED jobs and retire them one by one."""
    from tipapp.models import ThermalProcessingJob

    queued_jobs = ThermalProcessingJob.objects.filter(status='RETIRE_QUEUED').order_by('updated_at')

    count = queued_jobs.count()
    if count == 0:
        msg = "No jobs queued for retirement."
        if stdout:
            stdout.write(msg)
        logger.info("process_retire_queue: no jobs queued.")
        return

    msg = f"Found {count} job(s) queued for retirement."
    if stdout:
        stdout.write(msg)
    logger.info(f"process_retire_queue: found {count} queued job(s).")

    for job in queued_jobs:
        _retire_job(job, stdout)


def _retire_job(job, stdout=None):
    """Perform the full retire sequence for a single job."""
    flight_name = job.flight_name
    # flight_timestamp is used for GeoServer store names and PostGIS flight_datetime column
    flight_timestamp = flight_name.replace("FireFlight_", "")
    now = timezone.now()

    if stdout:
        stdout.write(f"Retiring job {job.id} ({flight_name})...")
    logger.info(f"process_retire_queue: starting retirement of job {job.id} ({flight_name}).")

    # Mark as RETIRING so the UI reflects active progress
    job.status = 'RETIRING'
    job.current_step = 'Retiring: moving data folder'
    job.save(update_fields=['status', 'current_step', 'updated_at'])

    errors = []

    # ------------------------------------------------------------------
    # Step 1: Move the processed data folder to the retired archive
    # ------------------------------------------------------------------
    original_folder = os.path.join(settings.DATA_STORAGE, flight_name)
    retired_dest = os.path.join(settings.RETIRED_STORAGE, flight_name)

    # If the destination already exists (re-retirement of the same flight),
    # append a timestamp to avoid shutil.move placing the folder inside it.
    if os.path.exists(retired_dest):
        retired_dest = f"{retired_dest}.RETIRED_{now.strftime('%Y%m%d_%H%M%S')}"
        logger.warning(
            f"Retired destination already exists; using timestamped path: {retired_dest}"
        )

    if os.path.exists(original_folder):
        try:
            shutil.move(original_folder, retired_dest)
            logger.info(f"Retired folder moved: {original_folder} -> {retired_dest}")
        except Exception as e:
            error_msg = f"Failed to move folder to retired archive: {e}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
    else:
        logger.warning(
            f"Processed data folder not found (may have been moved already): {original_folder}"
        )

    # ------------------------------------------------------------------
    # Step 2: Delete GeoServer Coverage Stores
    # ------------------------------------------------------------------
    job.current_step = 'Retiring: removing GeoServer stores'
    job.save(update_fields=['current_step', 'updated_at'])

    gs_user = os.environ.get('geoserver_user')
    gs_pwd = os.environ.get('geoserver_password')
    gs_url_base = os.environ.get(
        'general_gs_url_base',
        'https://hotspots.dbca.wa.gov.au/geoserver/rest/workspaces/hotspots/coveragestores/',
    )

    if gs_user and gs_pwd:
        try:
            # List all coverage stores to find individual image stores for this flight
            list_response = http_requests.get(
                gs_url_base,
                headers={'Accept': 'application/json'},
                auth=(gs_user, gs_pwd),
                timeout=30,
            )
            stores_to_delete = []

            if list_response.status_code == 200:
                store_list = list_response.json().get('coverageStores', {}).get('coverageStore', [])
                img_prefix = f"{flight_timestamp}_img_"
                for store in store_list:
                    name = store.get('name', '')
                    if name == f"{flight_name}.tif" or name.startswith(img_prefix):
                        stores_to_delete.append(name)
                logger.info(f"GeoServer stores to delete for {flight_name}: {stores_to_delete}")
            else:
                logger.warning(
                    f"Could not list GeoServer stores (status {list_response.status_code}). "
                    "Will attempt mosaic store deletion directly."
                )
                stores_to_delete = [f"{flight_name}.tif"]

            for store_name in stores_to_delete:
                # GeoServer REST API treats the last dot-separated segment as a format
                # specifier, so a store named "foo.tif" accessed as ".../foo.tif" is
                # parsed as store="foo", format="tif" -> 404.
                # Appending ".json" makes GeoServer parse it as store="foo.tif", format="json".
                delete_url = f"{gs_url_base}{store_name}.json?recurse=true"
                del_response = http_requests.delete(
                    delete_url,
                    auth=(gs_user, gs_pwd),
                    timeout=30,
                )
                if del_response.status_code in [200, 404]:
                    logger.info(
                        f"GeoServer store deleted (or not found): {store_name} "
                        f"(status {del_response.status_code})"
                    )
                else:
                    error_msg = (
                        f"Failed to delete GeoServer store '{store_name}': "
                        f"status {del_response.status_code}"
                    )
                    logger.error(error_msg)
                    errors.append(error_msg)
        except Exception as e:
            error_msg = f"GeoServer deletion error: {e}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
    else:
        logger.warning("GeoServer credentials not set; skipping GeoServer deletion.")

    # ------------------------------------------------------------------
    # Step 3: Delete TIF files from GeoServer storage (rclone mount)
    # ------------------------------------------------------------------
    job.current_step = 'Retiring: removing GeoServer storage files'
    job.save(update_fields=['current_step', 'updated_at'])

    gs_storage_base = "/rclone-mounts/thermalimaging-flightmosaics"
    mosaic_tif = os.path.join(gs_storage_base, f"{flight_name}.tif")
    images_dir = os.path.join(gs_storage_base, f"{flight_name}_images")

    for path, label in [(mosaic_tif, "mosaic TIF"), (images_dir, "images directory")]:
        if os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                logger.info(f"Deleted GeoServer storage {label}: {path}")
            except Exception as e:
                error_msg = f"Failed to delete GeoServer storage {label} '{path}': {e}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
        else:
            logger.warning(
                f"GeoServer storage {label} not found (may have been removed already): {path}"
            )

    # ------------------------------------------------------------------
    # Step 4: Delete PostGIS records
    # ------------------------------------------------------------------
    job.current_step = 'Retiring: removing PostGIS records'
    job.save(update_fields=['current_step', 'updated_at'])

    raw_postgis_url = os.environ.get('general_postgis_table', '')
    if raw_postgis_url:
        postgis_url = raw_postgis_url.replace('postgis://', 'postgresql://')
        try:
            engine = create_engine(postgis_url)
            with engine.connect() as conn:
                for table in ['hotspot_flight_footprints', 'hotspot_boundaries', 'hotspot_centroids']:
                    result = conn.execute(
                        text(f"DELETE FROM {table} WHERE flight_datetime = :ts"),
                        {"ts": flight_timestamp},
                    )
                    conn.commit()
                    logger.info(
                        f"Deleted {result.rowcount} rows from {table} "
                        f"for flight_datetime={flight_timestamp}"
                    )
        except Exception as e:
            error_msg = f"PostGIS deletion error: {e}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
    else:
        logger.warning("general_postgis_table not set; skipping PostGIS deletion.")

    # ------------------------------------------------------------------
    # Step 5: Finalise the job record
    # ------------------------------------------------------------------
    if errors:
        job.status = 'RETIRE_FAILED'
        job.error_message = '\n'.join(errors)
        job.current_step = 'Retire failed — check error message for details'
        job.save(update_fields=['status', 'error_message', 'current_step', 'updated_at'])
        logger.error(
            f"process_retire_queue: job {job.id} ({flight_name}) RETIRE_FAILED. "
            f"Errors: {errors}"
        )
        if stdout:
            stdout.write(f"  -> Job {job.id} ({flight_name}) RETIRE_FAILED.")
    else:
        # Rename flight_name to free up the unique slot for future re-uploads.
        # Format: FireFlight_20231214_093139.RETIRED_20260313_143022
        retired_flight_name = f"{flight_name}.RETIRED_{now.strftime('%Y%m%d_%H%M%S')}"
        job.flight_name = retired_flight_name
        job.status = 'RETIRED'
        job.retired_at = now
        job.current_step = 'Retired'
        job.save(update_fields=['flight_name', 'status', 'retired_at', 'current_step', 'updated_at'])
        logger.info(f"process_retire_queue: job {job.id} ({flight_name}) RETIRED successfully. flight_name renamed to '{retired_flight_name}'.")
        if stdout:
            stdout.write(f"  -> Job {job.id} ({flight_name}) RETIRED.")


class Command(base.BaseCommand):
    help = "Process jobs that are queued for retirement (RETIRE_QUEUED status)."

    def handle(self, *args, **kwargs):
        _process_retire_queue(self.stdout)


class ProcessRetireQueueCronJob(CronJobBase):
    """django-cron job: process RETIRE_QUEUED jobs every minute."""
    RUN_EVERY_MINS = 1
    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'tipapp.ProcessRetireQueueCronJob'

    def do(self):
        _process_retire_queue()
