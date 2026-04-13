# server/ttl.py

import time
import threading
from server.store import store

def ttl_worker():
    while True:
        store.cleanup_expired()
        time.sleep(2)  # runs every 2 seconds

def start_ttl_thread():
    thread = threading.Thread(target=ttl_worker, daemon=True)
    thread.start()