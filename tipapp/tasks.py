import os
import re
import logging
from datetime import datetime, timezone
from pathlib import Path
import py7zr
from zoneinfo import ZoneInfo

from tipapp import settings

logger = logging.getLogger(__name__)


def convert_date(timestamp):
    d = datetime.fromtimestamp(timestamp, timezone.utc)
    local_tz = ZoneInfo(settings.TIME_ZONE)
    d_local = d.astimezone(local_tz)
    formatted_date = d_local.strftime('%d %b %Y %H:%M:%S')
    return formatted_date

def get_files_list(dir_path, extensions = []):
    files_list = []
    with os.scandir(dir_path) as dir_entries:
        for entry in dir_entries:
            if entry.is_file():
                info = entry.stat()
                file_name = entry.name
                if len(extensions) > 0:
                    _, file_extension = os.path.splitext(file_name)
                    if file_extension.lower() in extensions:
                        files_list.append({"name": file_name, "path" : entry.path , "created_at": convert_date(info.st_mtime)})
    return files_list

def get_dir_size(dir_path):
    try:
        root_directory = Path(dir_path)
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())
    except Exception as e:
        logger.error(f"Error getting size of directory: {dir_path}")
        logger.error(e)
        return 0

def get_thermal_files(dir_path, page, offset, search = "", sort_by = "name", sort_order = "asc"):
    all_items = []
    try:
        # First, collect all entries with their metadata
        for entry in os.scandir(dir_path):
            entry_name = entry.name
            if search != "" and not re.search(str.lower(search), str.lower(entry_name)):
                continue
            
            info = entry.stat()
            is_dir = not entry.is_file()
            item = {
                "name": entry_name, 
                "path": entry.path, 
                "created_at": convert_date(info.st_mtime),
                "created_at_timestamp": info.st_mtime,
                "is_dir": is_dir
            }
            if is_dir:
                item['size'] = get_dir_size(entry.path)
            else:
                item['size'] = info.st_size
            all_items.append(item)
        
        # Sort the items
        reverse = (sort_order.lower() == "desc")
        if sort_by == "name":
            # Sort folders first, then by name
            all_items.sort(key=lambda x: (x['is_dir'] == False, x['name'].lower()), reverse=reverse)
        elif sort_by == "created_at":
            # Sort folders first, then by creation date
            all_items.sort(key=lambda x: (x['is_dir'] == False, x['created_at_timestamp']), reverse=reverse)
        elif sort_by == "size":
            # Sort folders first, then by size
            all_items.sort(key=lambda x: (x['is_dir'] == False, x['size']), reverse=reverse)
        
        # Remove the temporary timestamp field
        for item in all_items:
            item.pop('created_at_timestamp', None)
        
        # Paginate the results
        start_index = page * offset
        end_index = (page + 1) * offset
        items = all_items[start_index:end_index]
        
    except Exception as e:
        logger.error(f"Error getting thermal files from directory: {dir_path}")
        logger.error(e)
        return []
            
    return all_items

def get_file_record(dir_path, file_name):
    file_path = os.path.join(dir_path, file_name)
    info = os.stat(file_path)
    return {"name": file_name, "path" : file_path , "created_at": convert_date(info.st_mtime)}
