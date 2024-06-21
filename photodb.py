from concurrent.futures import ThreadPoolExecutor, as_completed
import dbm
from geopy.geocoders import Nominatim
from datetime import datetime
from functools import partial
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import pickle
import PIL.Image
from pymediainfo import MediaInfo
import re
import shutil
import sys
from wand.image import Image
import xxhash


def setup_logger(log_file):
    """
    Set up the logger to write to a file with rotation and also log INFO and higher messages to the console.

    :param log_file: The path to the log file.
    :return: Configured logger.
    """
    # Create a custom logger
    logger = logging.getLogger('photosdb')
    
    # Set the logging level for the logger itself
    logger.setLevel(logging.DEBUG)  # Capture all levels, control via handlers

    # Create a file handler that writes to a file and rotates logs
    handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)  # 5MB per file, keep 3 backups
    handler.setLevel(logging.DEBUG)  # Capture all levels to file
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Create a console handler to log INFO and higher messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Log INFO and higher to console
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(handler)
    logger.addHandler(console_handler)

    return logger


def get_coords(exif_data):
    if not exif_data:
        return None
    
    gps_tag_id = 34853
    if gps_tag_id not in exif_data:
        return None
    
    north = exif_data.get(gps_tag_id, {}).get(2, ())
    east = exif_data.get(gps_tag_id, {}).get(4, ())
    if not north or not east:
        return None
    
    lat = float(((((north[0] * 60) + north[1]) * 60) + north[2]) / 60 / 60)
    long = float(((((east[0] * 60) + east[1]) * 60) + east[2]) / 60 / 60)
    if exif_data.get(gps_tag_id, {}).get(1, "") == "S":
        lat *= -1.0
    if exif_data.get(gps_tag_id, {}).get(3, "") == "W":
        long *= -1.0
    return (lat, long)


def get_timestamp(exif_data):
    if not exif_data:
        return None

    creation_time_tag_id = 36867
    creation_time_str = exif_data.get(creation_time_tag_id)
    if not creation_time_str:
        return None
    
    timestamp = datetime.strptime(creation_time_str, "%Y:%m:%d %H:%M:%S")
    return timestamp


def get_date(timestamp):
    if not timestamp:
        return None
    return timestamp.strftime('%Y-%m-%d')


def get_year(timestamp):
    if not timestamp:
        return None
    return timestamp.strftime('%Y')


def get_location(coords):
    if not coords:
        return None
    loc = geo.reverse(f"{coords[0]},{coords[1]}")
    address = loc.raw.get("address", {})
    house_number = address.get("house_number")
    road = address.get("road")
    city = address.get("city")
    if not city:
        city = address.get("town")
    if not city:
        city = address.get("county")

    state = address.get("ISO3166-2-lvl4")
    if state.startswith("US-"):
        state = state[3:]
    lookup = f"{house_number} {road}, {city}, {state}"
    if lookup in cfg.get("locations"):
        return cfg.get("locations").get(lookup)
    if road:
        return (f"{road}, {city}, {state}")
    return(f"{city}, {state}")


def get_hash(file_path, chunk_size=8192):
    """Generate xxhash of the specified file."""
    hasher = xxhash.xxh64()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
    except Exception as e:
        return None
    return hasher.hexdigest()


def get_creation_timestamp(file_path):
    try:
        # Get the creation time
        creation_time = os.path.getctime(file_path)
        # Get the modification time
        modification_time = os.path.getmtime(file_path)
        
        # Compare and get the earliest timestamp
        earliest_timestamp = min(creation_time, modification_time)
        
        # Convert the earliest timestamp to a datetime object
        timestamp = datetime.fromtimestamp(earliest_timestamp)
        return timestamp
    except Exception as e:
        logger.error(e)
        return None


def get_media_info(file_path):
    try:
        media_info = MediaInfo.parse(file_path)
        metadata = {
            'creation_date': None,
            'general': {},
            'video': {},
            'audio': {},
            'image': {},
            'menu': {}
        }

        for track in media_info.tracks:
            if track.track_type == 'General':
                metadata['general']['format'] = track.format
                metadata['general']['file_size'] = track.file_size
                metadata['general']['duration'] = track.duration
                metadata['general']['bit_rate'] = track.overall_bit_rate
                creation_date = track.encoded_date
                if creation_date:
                    date_str = creation_date.replace('UTC ', '')
                    metadata['creation_date'] = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

            elif track.track_type == 'Video':
                metadata['video']['video_codec'] = track.codec_id
                metadata['video']['width'] = track.width
                metadata['video']['height'] = track.height
                metadata['video']['frame_rate'] = track.frame_rate
                metadata['video']['aspect_ratio'] = track.display_aspect_ratio
                metadata['video']['bit_rate'] = track.bit_rate

            elif track.track_type == 'Audio':
                metadata['audio']['audio_codec'] = track.codec_id
                metadata['audio']['sampling_rate'] = track.sampling_rate
                metadata['audio']['channels'] = track.channel_s
                metadata['audio']['bit_rate'] = track.bit_rate
                metadata['audio']['language'] = track.language

            elif track.track_type == 'Image':
                metadata['image']['image_codec'] = track.codec_id
                metadata['image']['image_width'] = track.width
                metadata['image']['image_height'] = track.height

            elif track.track_type == 'Menu':
                metadata['menu']['menu_format'] = track.format

            # Check for GPS data (if available)
            if hasattr(track, 'latitude') and hasattr(track, 'longitude'):
                metadata['gps_latitude'] = track.latitude
                metadata['gps_longitude'] = track.longitude
                if hasattr(track, 'altitude'):
                    metadata['gps_altitude'] = track.altitude

        return metadata
    except Exception as e:
        logger.error(e)
        return None


def convert_heic_to_jpeg(heic_file):
    try:
        # Extract directory path and filename without extension
        directory = os.path.dirname(heic_file)
        filename_no_ext = os.path.splitext(os.path.basename(heic_file))[0]

        # Generate JPEG file path in the same directory
        jpeg_file_path = os.path.join(directory, filename_no_ext + '.jpg')

        # Use Wand to convert HEIC to JPEG
        with Image(filename=heic_file) as img:
            img.format = 'jpeg'
            img.save(filename=jpeg_file_path)

        return jpeg_file_path

    except Exception as e:
        logger.error(e)
        return None


def generate_unique_filename(file_path):
    if not os.path.exists(file_path):
        return file_path  # If file doesn't exist, no collision, return original path
    
    file_name, file_ext = os.path.splitext(file_path)
    counter = 1
    
    while True:
        new_file_path = f"{file_name}_{counter:03d}{file_ext}"
        if not os.path.exists(new_file_path):
            return new_file_path  # Return the new path if it doesn't exist
        
        counter += 1


def move_file(path, new_path):
    new_path = generate_unique_filename(new_path)

    logger.info(f"Moving {path} to {new_path}")

    folder = os.path.dirname(new_path)
    os.makedirs(folder, exist_ok=True)
    
    try:
        shutil.move(path, new_path)
    except Exception as e:
        logger.error(e)
    return True


def move_to_duplicate(path):
    stripped_path = path.lstrip(cfg.get("main_dir"))
    new_path = os.path.join(cfg.get("duplicate_dir"), stripped_path)
    return move_file(path, new_path)
    

def process_file(path, import_files=False, move_duplicates=False):
    logger.debug(f"Processing file {path}")
    exif_data = None
    try:
        img = PIL.Image.open(path)
        exif_data = img._getexif()
    except PIL.UnidentifiedImageError:
        filename = os.path.basename(path)
        if filename in cfg.get("skip_files", []):
            logger.debug(f"Skipping {filename} because it is in skip_files")
            return
        if filename.upper().endswith(".HEIC"):
            if import_files:
                logger.info(f"Converting {path} to JPEG")
                jpeg_path = convert_heic_to_jpeg(path)
                if jpeg_path:
                    if (process_file(jpeg_path)):
                        return move_to_duplicate(path)
            return

    # Get timestamp, date and year
    timestamp = None
    if exif_data:
        timestamp = get_timestamp(exif_data)
    else:
        media_info = get_media_info(path)
        timestamp = media_info.get("creation_date")

    # If neither EXIF or media info is available, resort to using the creation timestamp
    if not timestamp:
        timestamp = get_creation_timestamp(path)

    date = get_date(timestamp)
    year = get_year(timestamp)
    if not date:
        logger.warning("Unknown timestamp, skipping")
        return

    # Get location
    coords = None
    location = None
    if exif_data:
        coords = get_coords(exif_data)
        location = get_location(coords)

    # Get hash
    hash = get_hash(path)

    new_folder = date
    if location:
        new_folder += f" - {location}"    
    
    filename = os.path.basename(path)
    new_path = os.path.join(cfg.get("main_dir"), year, new_folder, filename)
    
    stripped_path = path.lstrip(cfg.get("main_dir"))

    obj = {
        "filename": stripped_path,
        "coords": coords,
        "location": location,
        "date": date
    }
    
    db_obj = load_value(hash)
    if db_obj:
        full_path = os.path.join(cfg.get("main_dir"), db_obj.get("filename"))
        if not os.path.isfile(full_path):
            logger.debug(f"Deleting record for {full_path}")
            delete_value(hash)
            db_obj = None
    
    if db_obj:
        if stripped_path == db_obj.get("filename"):
            logger.debug(f"Updating record for {stripped_path}")
            save_value(hash, obj, overwrite=True)
        else:
            logger.warning(f"Duplicate found for {hash} ({stripped_path})")
            if move_duplicates:
                return move_to_duplicate(path)
    else:
        logger.debug(f"Creating record for {stripped_path}")
        save_value(hash, obj, overwrite=False)

    if import_files:
        return move_file(path, new_path)

def process_dir(folder, max_workers=4, recurse=True, import_files=False, move_duplicates=False):
    logger.debug(f"Processing directory {folder}")
    paths = os.listdir(folder)
    files = list()
    dirs = list()
    for file in paths:
        path = os.path.join(folder, file)
        if os.path.isfile(path):
            files.append(path)
        elif os.path.isdir(path):
            dirs.append(path)

    if recurse:
        for folder in dirs:
            process_dir(folder, max_workers, recurse, import_files, move_duplicates)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        partial_process_file = partial(process_file, import_files=import_files, move_duplicates=move_duplicates)
        futures = [executor.submit(partial_process_file, file) for file in files]
        
        for future in as_completed(futures):
            future.result()  # Wait for each future to complete
    
    logger.debug(f"Completed processing directory {folder}")


def save_value(key, value, overwrite=True):
    serialized = pickle.dumps(value)
    if key in db and not overwrite:
        return False
    db[key] = serialized
    return True


def load_value(key):
    try:
        serialized = db[key]
        return pickle.loads(serialized)
    except KeyError:
        return None


def delete_value(key):
    del db[key]


logger = setup_logger("photodb.log")

geo = Nominatim(user_agent="PhotoDB")

with open("photodb.cfg.json", "r") as cfg_file:
    cfg = json.load(cfg_file)

db = dbm.open("photos.gdbm", "c")

# previous_dir_state = load_value("dir_state")
# if not previous_dir_state:
#     previous_dir_state = dict()

# current_dir_state = get_directory_state(cfg.get("main_dir"))
# added, modified, deleted = detect_dir_changes(previous_dir_state, current_dir_state)

# for folder in added:
#     print(f"added {folder}")
#     process_dir(folder, recurse=True, import_files=False, move_duplicates=True)

# for folder in modified:
#     print(f"modified {folder}")
#     process_dir(folder, recurse=True, import_files=False, move_duplicates=True)

# save_value("dir_state", current_dir_state)


# Import process
#folder = cfg.get("incoming_dir")
#process_dir(folder, recurse=True)

# Update process
# Open main_dir
main_dir = cfg.get("main_dir")
process_dir(main_dir, max_workers=4, recurse=True, import_files=False, move_duplicates=True)

# for key in db.keys():
#     print(f"{key} = {pickle.loads(db[key])}")

# Go through the files
# Get metadata
# Get md5
# Store all of the above in the database
# If there's already an entry:
  # exit


