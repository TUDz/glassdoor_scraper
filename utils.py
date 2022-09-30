from pathlib import Path
NUM_RETRIES = 3
NUM_THREADS = 5
current_url = []
benefits_html = []

DB_CSV = Path('./files/db.csv')
TMP_DB_PATH = Path('./files/tmp_db.csv')

HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)"
}

