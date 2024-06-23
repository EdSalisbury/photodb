import argparse
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dbmwrapper import DBMWrapper
from geopy.geocoders import Nominatim
from datetime import datetime
from functools import partial
import json
import logging
import os
from pathlib import Path
import PIL.Image
from pymediainfo import MediaInfo
from ratelimit import limits, sleep_and_retry
import re
import shutil
import sys
import tempfile
import traceback
from wand.image import Image
import xxhash


def setup_logger():
    """
    Set up the logger to write to a file with rotation and also log INFO and higher messages to the console.

    :param log_file: The path to the log file.
    :return: Configured logger.
    """
    # Create a custom logger
    logger = logging.getLogger('photosdb')
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = f"photosdb_{timestamp}.log"
    console_log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

    # Set the logging level for the logger itself
    logger.setLevel(logging.DEBUG)  # Capture all levels, control via handlers

    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)  # Capture all levels to file
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Create a console handler to log INFO and higher messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)  # Log INFO and higher to console
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Add the handlers to the logger
    logger.addHandler(handler)
    logger.addHandler(console_handler)

    return logger


def parse_arguments():
    """
    Parse command line arguments.

    :return: Namespace with parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Process files with various options.")

    parser.add_argument(
        '--move-duplicates',
        action='store_true',
        help='Move duplicate files to a specified directory.'
    )
    parser.add_argument(
        '--import-files',
        type=str,
        help='Path to import media from.'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of worker threads to use for processing files. Default is 4.'
    )

    return parser.parse_args()


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


def round_coordinates(coordinate_tuple, precision=6):
    """
    Round the coordinates in the tuple to a specified precision.

    :param coordinate_tuple: A tuple of coordinates (e.g., (x, y)).
    :param precision: Number of decimal places to round to.
    :return: A new tuple with rounded coordinates.
    """
    return tuple(round(coord, precision) for coord in coordinate_tuple)


def tuple_to_dbm_key(coordinate_tuple, precision=6):
    """
    Convert a rounded coordinate tuple to a suitable key for dbm.

    :param coordinate_tuple: A tuple of coordinates (e.g., (x, y)).
    :param precision: Number of decimal places to round to before converting to key.
    :return: Encoded byte string suitable for dbm keys.
    """
    # Round the coordinates
    rounded_tuple = round_coordinates(coordinate_tuple, precision)
    
    # Convert tuple to string
    tuple_str = str(rounded_tuple)
    
    # Encode the string to bytes
    key_bytes = tuple_str.encode('utf-8')
    
    return key_bytes


@sleep_and_retry
@limits(calls=1, period=5)
def get_location(coords):
    logger.info(f"Getting location for {coords}")
    try:
        loc = geo.reverse(f"{coords[0]},{coords[1]}")
        return loc.raw.get("address", {})
    except Exception:
        return dict()


def get_address(coords):
    if not coords:
        return None
    
    coords = round_coordinates(coords)
    key = tuple_to_dbm_key(coords)
    address = db.load_value(key)

    if not address:
        address = dict()
        try:
            address = get_location(coords)
            db.save_value(key, address)
        except Exception:
            pass

    house_number = address.get("house_number")
    road = address.get("road")
    city = address.get("city")
    if not city:
        city = address.get("town")
    if not city:
        city = address.get("county")

    state = address.get("ISO3166-2-lvl4", "")
    if state.startswith("US-"):
        state = state[3:]
    lookup = f"{house_number} {road}, {city}, {state}"
    if lookup in cfg.get("locations"):
        return cfg.get("locations").get(lookup)
    if road:
        return (f"{road}, {city}, {state}")
    return(f"{city}, {state}")


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

        # Generate JPEG file path in a temp dir
        temp_dir = tempfile.mkdtemp()
        jpeg_file_path = os.path.join(temp_dir, filename_no_ext + '.jpg')

        # Use Wand to convert HEIC to JPEG
        with Image(filename=heic_file) as img:
            img.format = 'jpeg'
            img.save(filename=jpeg_file_path)

        return jpeg_file_path

    except Exception as e:
        logger.error(e)
        sys.exit(1)
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


def get_metadata(path):
    exif_data = None
    try:
        img = PIL.Image.open(path)
        exif_data = img._getexif()
    except PIL.UnidentifiedImageError:
        pass
    except AttributeError:
        pass

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
    
    # Get location
    coords = None
    address = None
    if exif_data:
        coords = get_coords(exif_data)
        address = get_address(coords)

    return {
        "path": path,
        "coords": coords,
        "location": address,
        "date": date,
        "year": year
    }


def get_hash(file_path, chunk_size=65536):
    """Generate xxhash of the specified file."""
    hasher = xxhash.xxh3_64()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
    except Exception as e:
        return None
    return hasher.hexdigest()


def process_file(path, move_duplicates=False):
    try:
        # Skip files
        filename = os.path.basename(path)
        if filename in cfg.get("skip_files"):
            logger.debug(f"Skipping {path}")
            return True
        
        logger.debug(f"Processing file {path}")
        hash = get_hash(path)
        db_path = db.load_value(hash)
        if db_path:
            logger.debug(f"Record found for {path} ({hash})")
            if os.path.isfile(db_path):
                if path == db_path:
                    return True
                else:
                    logger.debug(f"Duplicate detected for {path} (original is {db_path})")
                    if move_duplicates:
                        return move_to_duplicate(path)
                    else:
                        return True
            else:
                logger.debug(f"Replacing record for {db_path} ({hash})")
                return db.save_value(hash, path)
        else:
            logger.debug(f"Adding record for {path} ({hash})")
            return db.save_value(hash, path)
    except Exception as e:
        logger.error(f"Error processing {path}: {e}")
        logger.error(traceback.format_exc())  # Log the full traceback for debugging
        return False


def copy_file(path):
    metadata = get_metadata(path)
    
    new_folder = metadata.get("date")
    if metadata.get("location"):
        new_folder += f" - {metadata.get('location')}"    
    
    filename = os.path.basename(path)
    new_path = os.path.join(cfg.get("main_dir"), metadata.get('year'), new_folder, filename)    
    new_path = generate_unique_filename(new_path)

    folder = os.path.dirname(new_path)

    os.makedirs(folder, exist_ok=True)

    logger.info(f"Copying {path} to {new_path}")
    try:
        shutil.copy(path, new_path)
        return new_path
    except Exception as e:
        logger.error(e)
        return None


def import_file(path):
    try:
        # Skip files
        filename = os.path.basename(path)
        if filename in cfg.get("skip_files"):
            logger.debug(f"Skipping {path}")
            return True
        
        # Convert HEIC to JPEG
        if filename.upper().endswith(".HEIC"):
            logger.info(f"Converting {path} to JPEG")
            path = convert_heic_to_jpeg(path)
            if path:
                return import_file(path)
            else:
                return True

        logger.debug(f"Analyzing file {path}")
        hash = get_hash(path)
        db_path = db.load_value(hash)
        if db_path and os.path.isfile(db_path):
            logger.info(f"File has already been imported {path} ({hash}) {db_path}")
            return True
        else:
            logger.info(f"Importing file {path} ({hash})")
            new_path = copy_file(path)
            #db.save_value(hash, new_path)
            #sys.exit()
            return db.save_value(hash, new_path)
    except Exception as e:
        logger.error(f"Error processing {path}: {e}")
        logger.error(traceback.format_exc())  # Log the full traceback for debugging
        return False
    

def process_dir(folder, max_workers=4, import_files=False, move_duplicates=False):
    logger.info(f"Analyzing directory {folder}.")
    paths = os.listdir(folder)
    files = list()
    dirs = list()
    for file in paths:
        path = os.path.join(folder, file)
        if os.path.isfile(path):
            files.append(path)
        elif os.path.isdir(path):
            dirs.append(path)

    for folder in dirs:
        process_dir(folder, max_workers=max_workers, import_files=import_files, move_duplicates=move_duplicates)

    mtime = os.path.getmtime(folder)
    last_modified = db.load_value(folder)
    if import_files or not last_modified or mtime > float(last_modified):
        logger.info(f"Processing files in {folder}.")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            if import_files:
                partial_process_file = partial(import_file)
            else:
                partial_process_file = partial(process_file, import_files=import_files, move_duplicates=move_duplicates)
            
            futures = [executor.submit(partial_process_file, file) for file in files]
            
            for future in as_completed(futures):
                try:
                    result = future.result()  # Wait for each future to complete
                    if not result:
                        logger.error(f"Processing failed for file: {future.exception()}")
                except Exception as e:
                    logger.error(f"Error processing file: {e}")
                    logger.error(traceback.format_exc())

        db.save_value(folder, str(mtime))
        logger.info(f"Completed processing directory {folder}.")
    else:
        logger.debug(f"Skipping processing for {folder}.")


logger = setup_logger()
geo = Nominatim(user_agent="PhotoDB")
with open("photodb.cfg.json", "r") as cfg_file:
    cfg = json.load(cfg_file)
args = parse_arguments()
main_dir = cfg.get("main_dir")

with DBMWrapper("photos.gdbm", logger=logger) as db:
    if args.import_files:
        process_dir(args.import_files, max_workers=args.max_workers, import_files=True)
    else:
        process_dir(cfg.get("main_dir"), max_workers=args.max_workers, import_files=False, move_duplicates=args.move_duplicates)
