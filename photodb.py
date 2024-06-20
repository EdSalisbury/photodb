import dbm
from geopy.geocoders import Nominatim
from datetime import datetime
import functools
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import PIL.Image
import shutil
import sys


def setup_logger(log_file):
    """
    Set up the logger to write to a file with rotation.

    :param log_file: The path to the log file.
    :return: Configured logger.
    """
    # Create a custom logger
    logger = logging.getLogger('photosdb')
    
    # Set the logging level
    logger.setLevel(logging.DEBUG)  # Change this to logging.INFO or another level if needed

    # Create handlers
    # File handler that writes to a file and rotates logs
    handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)  # 5MB per file, keep 3 backups
    
    # Create a console handler if you also want to log to console
    console_handler = logging.StreamHandler()

    # Create formatters and add them to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(handler)
    logger.addHandler(console_handler)

    return logger


def log_operation(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting '{func.__name__}' with args: {args}, kwargs: {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Completed '{func.__name__}' successfully with result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in '{func.__name__}': {e}")
            raise
    return wrapper


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
    timestamp = datetime.strptime(creation_time_str, "%Y:%m:%d %H:%M:%S")
    return timestamp


def get_date(timestamp):
    if not timestamp:
        return "Unknown"
    return timestamp.strftime('%Y-%m-%d')


def get_year(timestamp):
    if not timestamp:
        return "Unknown"
    return timestamp.strftime('%Y')


def get_location(coords):
    if not coords:
        return ""
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
    return (f"{road}, {city}, {state}")


def get_md5_hash(file_path, chunk_size=1048576):
    """
    Calculate the MD5 hash of a file.

    :param file_path: Path to the file
    :param chunk_size: Size of the chunk to read at a time. Default is 8192 bytes.
    :return: MD5 hash of the file as a hexadecimal string
    """
    md5 = hashlib.md5()
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            md5.update(chunk)
    
    return md5.hexdigest()

@log_operation
def process_file(path):
    logger.debug(f"Processing file {path}")
    try:
        img = PIL.Image.open(path)
    except PIL.UnidentifiedImageError:
        logger.warning("Unknown image type, skipping")
        return

    exif_data = img._getexif()
    coords = get_coords(exif_data)
    timestamp = get_timestamp(exif_data)
    location = get_location(coords)
    md5 = get_md5_hash(path)
    date = get_date(timestamp)
    year = get_year(timestamp)

    new_folder = f"{date} - {location}"
    if date == "Unknown":
        logger.warning("Unknown timestamp, skipping")
        return

    if location == "":
        new_folder = f"{date}"

    new_path = os.path.join(cfg.get("main_dir"), year, new_folder)

    try:
        os.makedirs(new_path)
    except FileExistsError:
        pass

    logger.debug(f"New path = {new_path}")
    try:
        shutil.move(path, new_path)
    except Exception as e:
        logger.error(e)


def process_dir(folder):
    logger.debug(f"Processing directory {folder}")
    files = os.listdir(folder)

    for file in files:
        path = os.path.join(folder, file)
        if os.path.isdir(path):
            process_dir(path)
        else:
            process_file(path)


logger = setup_logger("photodb.log")

geo = Nominatim(user_agent="PhotoDB")

with open("photodb.cfg.json", "r") as cfg_file:
    cfg = json.load(cfg_file)

db = dbm.open("photos.gdbm", "c")

folder = cfg.get("incoming_dir")
process_dir(folder)

    





