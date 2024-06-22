import dbm
import logging
import pickle
import threading
import traceback


class DBMWrapper:
    def __init__(self, db_path, logger):
        self.db_path = db_path
        self.db = None
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger(__name__)

    def __enter__(self):
        try:
            self.db = dbm.open(self.db_path, 'c')
            return self
        except Exception as e:
            self.logger.critical(f"Error opening database: {e}")
            self.logger.critical(traceback.format_exc())  # Log the full traceback for debugging
            raise
            

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db is not None:
            self.db.close()

    def _serialize(self, value):
        return pickle.dumps(value)

    def _deserialize(self, value):
        return pickle.loads(value)

    def load_value(self, key):
        with self.lock:
            try:
                serialized_key = self._serialize(key)
                serialized_value = self.db.get(serialized_key)
                return self._deserialize(serialized_value) if serialized_value else None
            except Exception as e:
                self.logger.error(f"Error loading value for {key}: {e}")
                return None

    def save_value(self, key, value):
        #self.logger.debug(f"Saving {key} = {value}")
        with self.lock:
            try:
                serialized_key = self._serialize(key)
                serialized_value = self._serialize(value)
                self.db[serialized_key] = serialized_value
                return True
            except Exception as e:
                self.logger.error(f"Error saving value for {key}: {e}")
                return False