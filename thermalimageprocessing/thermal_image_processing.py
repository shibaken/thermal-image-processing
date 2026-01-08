import os
import shutil
import subprocess
import sys
import requests
import time
import decouple
import logging
import django
#import gdal
from osgeo import gdal
from thermalimageprocessing import gdal_edit
import fiona
from osgeo import ogr
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads
from shapely.geometry import LineString, Polygon
from sqlalchemy import create_engine
from postmarker.core import PostmarkClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from shapely.geometry import shape, mapping
from tipapp import settings
from tipapp import emails


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tipapp.settings')
django.setup()

logger = logging.getLogger(__name__)

# Enable GDAL exceptions to catch errors like disk space failure in Python try-except blocks
gdal.UseExceptions()
# Disable disk space check to prevent "ERROR 3" (Not recommended for production but fixes the immediate issue)
gdal.SetConfigOption('CHECK_DISK_FREE_SPACE', 'FALSE')
# FIX: Automatically close open polygons in KML files to prevent "LinearRing do not form a closed linestring" error
gdal.SetConfigOption('OGR_GEOMETRY_ACCEPT_UNCLOSED_RING', 'YES')

input_image_file_ext = ".png"
output_image_file_ext = ".tif"

raw_url = decouple.config("general_postgis_table", default="NO DATABASE URL FOUND FOR THERMAL IMAGE PROCESSING.")
if raw_url:
    # SQLAlchemy requires 'postgresql://' protocol, but Django often uses 'postgis://'.
    # Replace 'postgis://' with 'postgresql://' to avoid NoSuchModuleError.
    postgis_table = raw_url.replace('postgis://', 'postgresql://')
else:
    logger.error("ERROR: general_postgis_table environment variable is not set.")
    sys.exit(1) 

logger.debug(f'postgis_table: {postgis_table}')
# azure_conn_string = os.environ.get('general_azure_conn_string') # config.get('general', 'azure_conn_string') 
container_name = os.environ.get('general_container_name') # config.get('general', 'container_name')
# blob_service_client = BlobServiceClient.from_connection_string(azure_conn_string)
districts_dataset_name = os.environ.get('general_districts_dataset_name') # config.get('general', 'districts_dataset_name')
districts_gpkg = os.path.join(os.path.dirname(__file__), districts_dataset_name)
districts_layer_name = os.environ.get('general_districts_layer_name') #config.get('general', 'districts_layer_name')
user = os.environ.get('geoserver_user') #config.get('geoserver', 'user')
gs_pwd = os.environ.get('geoserver_password') #config.get('geoserver', 'gs_pwd')

class Footprint:
    def __init__(self):
        self.as_line = None
        self.as_poly = None
        self.districts = []

def check_first_two_images_overlap(files):
    overlap = False
    if len(files) >= 2:
        first_image = gdal.Open(files[0])
        second_image = gdal.Open(files[1])
        ulx1, uly1, lrx1, lry1 = get_corners(first_image)
        ulx2, uly2, lrx2, lry2 = get_corners(second_image)
        first_image = None
        second_image = None
        # If second_image is to the right of first_image
        if ulx2 >= ulx1:
            # Is it too far right?
            if ulx2 <= lrx1: # No, not too far right
                # Is it too high?
                if uly2 >= uly1:
                    if lry2 <= uly1: # No, not too high
                        overlap = True
                # Is it too low?
                elif uly2 < uly1:
                    if uly2 >= lry1: # No, not too low
                        overlap = True
        else: # If second_image is to the left of first_image
            # Is it too far left?
            if ulx1 <= lrx2: # No, not too far right
                # Is it too high?
                if uly2 >= uly1:
                    if lry2 <= uly1: # No, not too high
                        overlap = True
                # Is it too low?
                elif uly2 < uly1:
                    if uly2 >= lry1: # No, not too low
                        overlap = True
    return overlap

def check_timediff_first_two_images(files):
    first_file = files[0]
    second_file = files[1]
    first_time = os.path.getmtime(first_file)
    second_time = os.path.getmtime(second_file)
    timediff = second_time - first_time
    return timediff

def get_corners(image):
    ulx, xres, xskew, uly, yskew, yres = image.GetGeoTransform()
    lrx = ulx + (image.RasterXSize * xres)
    lry = uly + (image.RasterYSize * yres)
    return (ulx, uly, lrx, lry)

def get_exclude_first(files):
    # Check if first photo should be excluded - need time diff btw first two > 180s AND no overlap in first two images
    overlap = check_first_two_images_overlap(files)
    timediff = check_timediff_first_two_images(files)
    if (not overlap) and timediff > 180:
        exclude_first = True
    else:
        exclude_first = False
    return exclude_first

def merge(files, output_path):
    # Merges PNGs and saves the output to the specified mosaic_image path.
    # Using gdal.Warp instead of gdal_merge.main ensures better compatibility 
    # between different GDAL versions (e.g., 3.0 vs 3.8) and offers better performance.
    file_count = len(files)
    output_name = os.path.basename(output_path)
    msg = f"Merging {file_count} input files into: {output_name}..."
    logger.info(msg)

    try:
        gdal.Warp(
            output_path,       # Output file path
            files,              # List of input files
            format="GTiff",
            srcNodata=0,        # Equivalent to -n 0
            dstNodata=0,         # Equivalent to -a_nodata 0
            options=["-co", "COMPRESS=DEFLATE"] # Compresses the output to save disk space
        )
    except Exception as e:
        # Log the error with stack trace for debugging
        logger.error(f"Merge failed: {e}", exc_info=True)
        # Also print to stdout so it appears in the subprocess runner's log
        raise

    # Assign the projection (EPSG:28350) to the output image
    gdal_edit_args = ["", "-a_srs", "EPSG:28350", output_path]
    # gdal_edit_args = ["", "-a_srs", "EPSG:4326", mosaic_image]
    gdal_edit.main(gdal_edit_args)

def translate_png2tif(input_png, short_file, flight_name):
    # Translates png to tif
    output_tif = input_png.replace(".png", ".tif")
    tif_filename = short_file.replace(".png", ".tif")
    gdal.Translate(output_tif, input_png, outputSRS="EPSG:28350")
    relative_path = flight_name + "_images/" + tif_filename
    copy_to_geoserver_storage(output_tif, relative_path)
    #publish_image_on_geoserver(flight_name, tif_filename)

def copy_to_geoserver_storage(source_file, relative_dest_path):
    """
    Copies the processed image file to the shared storage mount for GeoServer.
    """
    try:
        # Define the target base path
        mount_base_path = "/rclone-mounts/thermalimaging-flightmosaics"
        
        # Construct the full destination path
        # blob_name is used here as the relative path (e.g., 'FlightName.tif' or 'FlightName_images/xxx.tif')
        dest_path = os.path.join(mount_base_path, relative_dest_path)

        try:
            file_size = os.path.getsize(source_file)
            file_size_mb = file_size / (1024 * 1024) # Convert to MB
            logger.info(f"Copying file ({file_size_mb:.2f} MB) to GeoServer storage...")
        except OSError:
            # Handle cases where file might not exist yet (though unlikely here)
            logger.warning(f"Copying file to GeoServer storage (Size unknown).")

        logger.info(f"Source: {source_file}")
        logger.info(f"Destination: {dest_path}")

        # Extract the directory path
        dest_dir = os.path.dirname(dest_path)
        
        # Create the directory if it does not exist
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
        
        # Copy the image file to the destination
        shutil.copyfile(source_file, dest_path)

        logger.info(f"Copy complete.") 
        
    except Exception as e:
        error_msg = f"Failed to copy file to rclone mount: {e}"
        # Log the error with full stack trace
        logger.error(error_msg, exc_info=True)


def create_mosaic_footprint_as_line(files, raw_img_folder, flight_timestamp, image, engine, footprint, output_geopackage):
    bboxes = create_img_bounding_boxes(files, raw_img_folder)
    minx, miny, maxx, maxy = bboxes.geometry.total_bounds
    # Create linestring
    points = [(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny)]
    footprint.as_line = LineString(points)
    footprint.as_poly = Polygon(points)
    line_geom = gpd.geoseries.GeoSeries([footprint.as_line])
    poly_geom = gpd.geoseries.GeoSeries([footprint.as_poly])
    data_dictionary = {"flight_datetime": [flight_timestamp]}
    footprint_line_layer = gpd.geodataframe.GeoDataFrame(data_dictionary, crs="EPSG:4326", geometry=line_geom)
    footprint_line_layer.to_postgis("hotspot_flight_footprints", engine, if_exists="append")
    footprint_poly_layer = gpd.geodataframe.GeoDataFrame(data_dictionary, crs="EPSG:4326", geometry=poly_geom)
    footprint_poly_layer.to_file(output_geopackage, layer='footprint', driver="GPKG")

def get_footprint_districts(footprint, output_geopackage):
    districts_gdf = gpd.read_file(districts_gpkg, layer=districts_layer_name)
    footprint_gdf = gpd.read_file(output_geopackage, layer='footprint')
    for index, footprint_feature in footprint_gdf.iterrows():
        # Should only be one footprint in the layer
        footprint_geom = footprint_feature['geometry']
    for index, district in districts_gdf.iterrows():
        if footprint_geom.intersects(district['geometry']):
            footprint.districts.append(district['ADMIN_ZONE'].strip().replace(" ", "_"))

def create_img_bbox_as_poly(image):
    # Get coords of diagonal corners of output raster
    working_image = gdal.Open(image)
    ulx, uly, lrx, lry = get_corners(working_image)
    working_image = None
    # Create linestring
    points = [(ulx, uly), (lrx, uly), (lrx, lry), (ulx, lry)]
    poly = Polygon(points)
    return str(poly)

def create_img_bounding_boxes(files, raw_img_folder):
    bbox_polys = []
    files_columns = []
    for file in files:
        filename = os.path.basename(file)
        #short_file = os.path.join(raw_img_folder, filename) files_columns.append([file, short_file])
        files_columns.append([file, filename])
        poly = create_img_bbox_as_poly(file)
        bbox_polys.append(poly)
    crs = "EPSG:28350"
    geom = gpd.geoseries.GeoSeries([loads(poly) for poly in bbox_polys])
    geom.crs = crs
    bboxes = gpd.geodataframe.GeoDataFrame(files_columns, crs=crs, geometry=geom, columns=['file', 'short_file'])
    bboxes = bboxes.to_crs('EPSG:4326')
    return bboxes 

def create_boundaries_and_centroids(flight_timestamp, kml_boundaries_file, bboxes, engine, output_geopackage):
    all_images_with_hotspots = []
    try:
        # NB bboxes is a set of bounding boxes for each image (excluding the first, if exclude_first is True) gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw' # Enables 
        # fiona KML driver
        # gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # Enables fiona KML driver
        # Use fiona directly to enable LIBKML driver support
        fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'    
        fiona.drvsupport.supported_drivers['KML'] = 'rw'
        # kml_boundaries = gpd.read_file(kml_boundaries_file)
        # kml_boundaries['geometry'] = kml_boundaries.geometry.buffer(0)
        # kml_boundaries['geometry'] = kml_boundaries.geometry.make_valid()

        # =====================================================================
        # FIX: Implement the robust feature-by-feature reading and cleaning process.
        # This avoids the crash by validating and fixing each geometry individually.

        clean_features = []
        geom_types_to_keep = ['Polygon', 'MultiPolygon']

        logger.info(f"Reading and cleaning KML file: {kml_boundaries_file}")
        with fiona.open(kml_boundaries_file, 'r') as source:
            for feature in source:
                # Skip features with no geometry
                if not feature.get('geometry'):
                    continue

                # Filter to keep only polygon types
                if feature['geometry']['type'] in geom_types_to_keep:
                    try:
                        # Convert the Fiona geometry to a Shapely geometry object
                        geom = shape(feature['geometry'])

                        # Check for validity. If not valid, try to fix it with buffer(0).
                        # buffer(0) is a powerful trick that fixes many issues, including non-closed rings.
                        if not geom.is_valid:
                            fixed_geom = geom.buffer(0)
                            # If buffer(0) resulted in an empty geometry, skip it
                            if fixed_geom.is_empty:
                                logger.warning(f"Invalid geometry found and could not be fixed (resulted in empty geom). Skipping feature.")
                                continue

                            # Create a NEW feature dictionary instead of modifying the existing one.
                            clean_feature = {
                                'type': feature.get('type', 'Feature'),
                                'properties': feature.get('properties', {}),
                                'geometry': mapping(fixed_geom)
                            }
                            clean_features.append(clean_feature)
                        else: 
                            # If geometry is valid as is, just append the original feature
                            clean_features.append(feature)

                    except Exception as e:
                        logger.error(f"Could not process a feature due to an error: {e}. Skipping.", exc_info=False)
        
        # If no valid polygons were found after cleaning, exit the function.
        if not clean_features:
            logger.warning(f"No valid Polygon or MultiPolygon features found in KML file after cleaning.")
            return []

        # Create a clean GeoDataFrame from the list of validated features.
        kml_boundaries = gpd.GeoDataFrame.from_features(clean_features, crs=source.crs)
        logger.info(f"Successfully loaded {len(kml_boundaries)} valid polygon features.")
        # =====================================================================

        boundary_geometries = []
        try:
            # boundary_geometries = [geom for geom in kml_boundaries.unary_union.geoms]
            # The unary_union might result in a single geometry, not a list of geoms
            # It's safer to handle both cases
            union_geom = kml_boundaries.union_all()
            if union_geom.geom_type in ('MultiPolygon', 'GeometryCollection'):
                boundary_geometries = list(union_geom.geoms)
            else:
                boundary_geometries = [union_geom]
        except:
            logger.error(f"Error during unary_union: {e}", exc_info=True)
            return []
        included_geometries = []
        centroid_geometries = []
        images_column = []
        flight_datetime_column = []
        hotspot_no_column = []
        i = 1
        for geom in boundary_geometries:
            envelope = geom.envelope
            images = []
            for index, row in bboxes.iterrows():
                box = row['geometry']
                file = row['file']
                short_file = row['short_file']
                if envelope.intersects(box):
                    images.append(short_file)
                    if short_file not in all_images_with_hotspots:
                        all_images_with_hotspots.append(short_file)
            if len(images) > 0:
                included_geometries.append(geom)
                centroid_geometries.append(geom.centroid)
                images_column.append(str(images)[1:-1].replace("'", "").replace(".png", ""))
                flight_datetime_column.append(flight_timestamp)
                hotspot_no_column.append(i)
                i += 1
        data_dictionary = {"flight_datetime": flight_datetime_column, "hotspot_no": hotspot_no_column, "images": images_column}
        crs="EPSG:4283"
        boundaries = gpd.geodataframe.GeoDataFrame(data_dictionary, crs=crs, geometry=included_geometries)
        boundaries = boundaries.to_crs('EPSG:4326')
        boundaries.to_file(output_geopackage, layer='boundaries', driver="GPKG")
        boundaries.to_postgis("hotspot_boundaries", engine, if_exists="append")
        centroids = gpd.geodataframe.GeoDataFrame(data_dictionary, crs=crs, geometry=centroid_geometries)
        centroids = centroids.to_crs('EPSG:4326')
        centroids.to_file(output_geopackage, layer='centroids', driver="GPKG")
        centroids.to_postgis("hotspot_centroids", engine, if_exists="append")
    except Exception as e:
        logger.error(f"An unexpected error occurred in create_boundaries_and_centroids: {e}", exc_info=True)
    finally:
        return sorted(all_images_with_hotspots)

def publish_image_on_geoserver(flight_name, image_name=None):
    logger.info(f'Publishing to GeoServer... Flight: {flight_name}, Image: {image_name}')

    flight_timestamp = flight_name.replace("FireFlight_", "")
    headers = {'Content-type': 'application/xml'}
    file_url_base = os.environ.get('general_file_url_base', 'file:///rclone-mounts/thermalimaging-flightmosaics/')
    gs_url_base = os.environ.get('general_gs_url_base','https://hotspots.dbca.wa.gov.au/geoserver/rest/workspaces/hotspots/coveragestores/')

    logger.info(f'gs_url_base: {gs_url_base}')

    if image_name is None:
        gs_layer_url = gs_url_base + flight_name + '.tif/coverages'
    else:
        gs_layer_url = gs_url_base + flight_timestamp + '_img_' + image_name + '/coverages'

    # Create data store on geoserver
    if image_name is None:
        store_data = '<coverageStore><name>{flight_name}.tif</name><workspace>hotspots</workspace><enabled>true</enabled><type>GeoTIFF</type><url>{file_url_base}{flight_name}.tif</url></coverageStore>'.format(flight_name=flight_name, file_url_base=file_url_base)
    else:
        store_data = '<coverageStore><name>{flight_timestamp}_img_{image}</name><workspace>hotspots</workspace><enabled>true</enabled><type>GeoTIFF</type><url>{file_url_base}{flight_name}_images/{image_name}</url></coverageStore>'.format(flight_name=flight_name, flight_timestamp=flight_timestamp, file_url_base=file_url_base, image=image_name, image_name=image_name)
    
    # --- Create Coverage Store ---
    try:
        response = requests.post(gs_url_base, headers=headers, data=store_data, auth=(user, gs_pwd))
        if response.status_code in [200, 201]:
            logger.info(f"Coverage Store created successfully. (Status {response.status_code})")
        elif response.status_code == 500 and "already exists" in response.text:
            # Not strictly an error if we are updating or reprocessing
            logger.info(f"Coverage Store already exists. Status: {response.status_code}")
        else:
            logger.error(f"Failed to create Coverage Store. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Exception during Coverage Store creation: {e}", exc_info=True)
    
    if image_name is None:
        target_layer_name = flight_name
    else:
        # e.g. 20231214_..._img_00001
        target_layer_name = f"{flight_timestamp}_img_{image_name[:-4]}"

    layer_data = f'<coverage><name>{target_layer_name}</name><title>{target_layer_name}</title><srs>EPSG:28350</srs></coverage>'

    # --- Create/Publish Layer ---
    try:
        response = requests.post(gs_layer_url, headers=headers, data=layer_data, auth=(user, gs_pwd))

        if response.status_code == 201:
            success_message = f'Great success! Layer published on GeoServer: {target_layer_name}.'
            logger.info(success_message)
        elif response.status_code == 500 and "already exists" in response.text:
            success_message = f'Layer already exists on GeoServer (Status 500). Skipping: {target_layer_name}.'
            logger.info(success_message)
        else:
            error_msg = f"Error GeoServer Layer Publish for {target_layer_name}. Status: {response.status_code}"
            logger.error(error_msg)
            logger.error(f"Response: {response.text}")
            print(f"Response: {response.text}")
    except Exception as e:
        error_msg = f"Exception during Layer publication: {e}"
        logger.error(error_msg, exc_info=True)

def unzip_and_prepare(full_filename_path):
    """
    Handles file preparation: copying, moving, and unzipping.
    Replaces the functionality of the shell script.
    """
    # Get the base directory of the project
    # base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    filename = os.path.basename(full_filename_path)
    
    # Logic to determine directory name (Removing extension and timestamp)
    basename_without_ext = os.path.splitext(filename)[0]
    # Assuming format Name.timestamp -> Name
    dirname = os.path.splitext(basename_without_ext)[0]

    logger.info(f"Preparing to process: {filename}")
    logger.info(f"Target directory name: {dirname}")

    # Define processing directory
    # processing_base_folder = os.path.join(base_dir, 'thermal_data_processing')
    processing_base_folder = settings.DATA_STORAGE

    if not os.path.exists(processing_base_folder):
        os.makedirs(processing_base_folder)

    # Path for the temporary 7z file in processing folder
    target_7z_path = os.path.join(processing_base_folder, filename)

    # 1. Copy file to processing folder
    logger.info(f"Copying {full_filename_path} to {target_7z_path}")
    shutil.copy2(full_filename_path, target_7z_path)

    # 2. Move original file to uploads history folder
    # Ensure uploads_folder_path is absolute
    # if not os.path.isabs(uploads_folder_path):
    #     uploads_folder_path = os.path.join(base_dir, uploads_folder_path)
    
    dest_move_path = os.path.join(settings.UPLOADS_HISTORY_PATH, filename)
    logger.info(f"Moving original file to {dest_move_path}")    

    if not os.path.exists(settings.UPLOADS_HISTORY_PATH):
         os.makedirs(settings.UPLOADS_HISTORY_PATH, exist_ok=True)
         
    shutil.move(full_filename_path, dest_move_path)

    # 3. Unzip using 7z
    logger.info(f"Uncompressing {filename}...")
    try:
        # -aoa: Overwrite All existing files without prompt
        subprocess.run(
            ['7z', 'x', target_7z_path, '-aoa'],
            cwd=processing_base_folder, # Execute inside thermal_data_processing
            check=True,
            capture_output=True
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"7z extraction failed: {e.stderr.decode()}")
        raise

    # 4. Remove the temporary .7z file
    os.remove(target_7z_path)

    # Return the full path to the extracted directory
    return os.path.join(processing_base_folder, dirname)


# =========================================================
# Main processing logic wrapper
# =========================================================
def run_thermal_processing(flight_path_arg):
    """
    Main entry point for thermal image processing.
    """
    # Argument is now the full path
    flight_name = os.path.basename(flight_path_arg)
    flight_timestamp = flight_name.replace("FireFlight_", "")
    main_folder = flight_path_arg

    # Set filepaths
    raw_img_folder = os.path.join(main_folder, "PNGs/CAMERA1")
    output_folder = os.path.join(main_folder, "Processed")
    mosaic_image = os.path.join(output_folder, flight_name + "_mosaic" + output_image_file_ext)
    footprint = Footprint()
    kml_boundaries_folder = os.path.join(main_folder, "KML Boundaries/CAMERA1")
    kml_boundaries_file =""
    output_geopackage = os.path.join(output_folder, "output.gpkg")

    # =========================================================
    # FIX: Dynamically add a FileHandler for this specific flight
    # =========================================================
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_folder = os.path.join(base_dir, 'logs')
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    log_file_path = os.path.join(logs_folder, flight_name + '.txt')

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s %(asctime)s %(name)s [Line:%(lineno)s][%(funcName)s] %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Send the "Processing Started" notification immediately.
    # We wrap this in a try/except block so that a failure in sending this email
    # does not prevent the main processing from running.
    try:
        logger.info(">>> Sending 'Processing Started' notification email...")
        # email_sender.send_processing_started_notification(flight_name)
        emails.send_processing_started_notification(flight_name)
    except Exception as e:
        logger.error(f"Failed to send 'started' notification email: {e}", exc_info=True)

    # --- Log: Start Process ---
    start_msg = f"=== STARTING PROCESSING FOR: {flight_name} ==="
    logger.info(start_msg)

    # These variables will act as a "scoreboard" to track the final outcome.
    success = True  # We start by assuming the process will be successful.
    msg = ""        # This will accumulate messages for the success email.
    error_details = "" # This will store the specific error for the failure email.

    try:
        logger.info(f"Looking for KML in: {kml_boundaries_folder}")
        if os.path.exists(kml_boundaries_folder):
            for filename in os.listdir(kml_boundaries_folder):
                if "supermosaic_" in filename.lower() and filename.lower().endswith("bnd.kml"):
                    kml_boundaries_file = os.path.join(kml_boundaries_folder, filename)
                    break
            if kml_boundaries_file == "":
                for filename in os.listdir(kml_boundaries_folder):
                    if filename.lower() == "mosaic_0_0_bnd.kml":
                        kml_boundaries_file = os.path.join(kml_boundaries_folder, filename)
                        break
        
        if kml_boundaries_file == "":
            raise FileNotFoundError("Critical Error: No file named *SuperMosaic*BND.kml found in KML Boundaries folder.")

        engine = create_engine(postgis_table)
        
        exclude_first = None
        if os.path.exists(raw_img_folder):
            files = [os.path.join(raw_img_folder, f) for f in os.listdir(raw_img_folder) if f.endswith(input_image_file_ext)]
            files.sort()
            exclude_first = get_exclude_first(files)
            if exclude_first:
                files.remove(files[0])
        else:
            # Raise an error if the image folder is missing.
            raise FileNotFoundError(f"Raw image folder not found: {raw_img_folder}")

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        all_images_with_hotspots = []
        mosaic_stored_ok = False

        # --- Log: Mosaic Creation ---
        logger.info(">>> Step 1/8: Creating Mosaic Image (gdal.Warp)...")
        # Pass output path explicitly
        merge(files, mosaic_image)
        msg += "\nMosaic produced OK"
        logger.info("Mosaic produced OK")

        # Wait a bit
        time.sleep(10)

        # --- Log: File Copy/Upload ---
        logger.info(">>> Step 2/8: Copying Mosaic to GeoServer Storage...")
        copy_to_geoserver_storage(mosaic_image, flight_name + ".tif")
        msg += "\nMosaic pushed to GeoServer storage OK"
        logger.info("Mosaic pushed to GeoServer storage OK")
        mosaic_stored_ok = True

        # --- Log: Footprint Creation ---
        logger.info(">>> Step 3/8: Creating Footprint and pushing to PostGIS...")
        create_mosaic_footprint_as_line(files, raw_img_folder, flight_timestamp, mosaic_image, engine, footprint, output_geopackage)
        msg += "\nFootprint produced and pushed to PostGIS OK"
        logger.info('Footprint produced and pushed to PostGIS OK') 

        # TEST MODE: Intentional failure for email testing
        if decouple.config("TEST_THERMAL_PROCESSING_FAILURE", default=False, cast=bool):
            raise Exception("TEST MODE: Intentional failure for email testing")

        # --- Log: District Check ---
        logger.info(">>> Step 4/8: Checking Districts...")
        get_footprint_districts(footprint, output_geopackage)
        msg += "\nFootprint lies in district(s) " + str(footprint.districts)
        logger.info("Footprint lies in district(s) " + str(footprint.districts))

        # --- Log: Bounding Boxes ---
        logger.info(">>> Step 5/8: Creating Image Bounding Boxes...")
        bboxes = create_img_bounding_boxes(files, raw_img_folder)
        msg += "\nBounding box creation for images OK"
        logger.info('Bounding box creation for images OK')

        # --- Log: Hotspot Analysis ---
        logger.info(">>> Step 6/8: Analyzing Hotspots (Intersects)...")
        all_images_with_hotspots = create_boundaries_and_centroids(flight_timestamp, kml_boundaries_file, bboxes, engine, output_geopackage)
        if not all_images_with_hotspots:
            msg += "\nNO HOTSPOTS FOUND!!!"
            logger.info("NO HOTSPOTS FOUND!!!")
        else:
            msg += "\nBoundaries and centroids creation and push to PostGIS OK"
            logger.info("Boundaries and centroids creation and push to PostGIS OK")

        # --- Log: Image Conversion ---
        count = len(all_images_with_hotspots)
        logger.info(f">>> Step 7/8: Converting {count} Hotspot Images (PNG to TIF)...")
        if len(all_images_with_hotspots) > 0:
            for img in all_images_with_hotspots:
                full_path = os.path.join(raw_img_folder, img)
                translate_png2tif(full_path, img, flight_name)
            msg += "\nProduction of tif images OK"
            logger.info("Production of tif images OK")

        # Wait for storage sync
        time.sleep(60)

        # --- Log: GeoServer Publishing ---
        logger.info(">>> Step 8/8: Publishing to GeoServer...")
        if mosaic_stored_ok:
            publish_image_on_geoserver(flight_name)
            msg += "\nMosaic published on geoserver OK"
            logger.info("Mosaic published on geoserver OK")
        else:
            # This is not an exception, but a known non-critical issue.
            # We log it and add it to the message, but let the process continue.
            msg += "\nWarning: Mosaic was not stored, so it could not be published on geoserver."
            logger.warning("Mosaic was not stored, so it could not be published on geoserver.")

        for img in all_images_with_hotspots:
            img = img.replace(".png", ".tif")
            publish_image_on_geoserver(flight_name, img)

        if all_images_with_hotspots:
            msg += f"\nPublished {len(all_images_with_hotspots)} individual hotspot images to geoserver OK."
            logger.info(f"Published {len(all_images_with_hotspots)} individual hotspot images to geoserver OK.")

    except Exception as e:
        # If ANY of the steps in the 'try' block fails, the code will jump directly here.
        success = False  # Mark the entire process as failed.
        error_details = f"A failure occurred during processing. Error: {str(e)}"
        msg += f"\n\n--- PROCESS FAILED ---\n{error_details}"
        logger.error(error_details, exc_info=True)
    finally:
        logger.info(">>> Preparing to send final completion notification email...")
        try:
            # Check the "scoreboard" variable to decide which email to send.
            if success:
                # If the 'success' flag is still True, send the success email.
                emails.send_success_notification(
                    flight_name=flight_name, 
                    details_message=msg
                )
            else:
                # If the 'success' flag was set to False in the 'except' block, send the failure email.
                emails.send_failure_notification(
                    flight_name=flight_name, 
                    error_message=error_details
                )
        except Exception as e:
            logger.error(f"FATAL: Could not send the final notification email. Error: {e}", exc_info=True)

        # This ensures it's always the last thing logged for the process.
        end_msg = f"=== FINISHED PROCESSING FOR: {flight_name} (Success: {success}) ==="
        logger.info(end_msg)
        
        # Add cleanup for the logger's file handler.
        if 'file_handler' in locals() and file_handler in logger.handlers:
            logger.removeHandler(file_handler)
            file_handler.close()

# =========================================================
# Legacy Support: Allows running from command line (like .sh)
# =========================================================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python thermal_image_processing.py <flight_data_path>")
        sys.exit(1)
        
    # If run directly from command line (legacy .sh style), 
    # we assume the path provided is already prepared/unzipped.
    run_thermal_processing(sys.argv[1])
