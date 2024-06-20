import dbm
from geopy.geocoders import Nominatim
from datetime import datetime
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import PIL.Image
from pymediainfo import MediaInfo
import re
import shutil
import sys
from wand.image import Image


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
        return None
    return timestamp.strftime('%Y-%m-%d')


def get_year(timestamp):
    if not timestamp:
        return None
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
    if road:
        return (f"{road}, {city}, {state}")
    return(f"{city}, {state}")


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


def process_file(path):
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
            logger.info(f"Converting {path} to JPEG")
            path = convert_heic_to_jpeg(path)
            if path:
                process_file(path)
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
        sys.exit()
        return

    # Get location
    location = None
    if exif_data:
        coords = get_coords(exif_data)
        location = get_location(coords)

    # Get hash
    md5 = get_md5_hash(path)

    new_folder = date
    if location:
        new_folder += f" - {location}"    
    
    new_path = os.path.join(cfg.get("main_dir"), year, new_folder)

    try:
        os.makedirs(new_path)
    except FileExistsError:
        pass

    logger.info(f"Moving {path} to {new_path}")
    try:
        shutil.move(path, new_path)
    except Exception as e:
        logger.error(e)
    sys.exit()


def process_dir(folder, recurse=True):
    logger.debug(f"Processing directory {folder}")
    files = os.listdir(folder)

    for file in files:
        path = os.path.join(folder, file)
        if os.path.isdir(path) and recurse:
            process_dir(path)
        elif os.path.isfile(path):
            process_file(path)


logger = setup_logger("photodb.log")

geo = Nominatim(user_agent="PhotoDB")

with open("photodb.cfg.json", "r") as cfg_file:
    cfg = json.load(cfg_file)

db = dbm.open("photos.gdbm", "c")

folder = cfg.get("incoming_dir")
process_dir(folder, recurse=False)

    





