import requests
import urllib.parse

NUM_RETRIES = 3
NUM_THREADS = 5
current_url = []
benefits_html = []

HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Thunder Client (https://www.thunderclient.com)"
}

