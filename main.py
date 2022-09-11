from scrapy import Selector
import pandas as pd
import requests
import json
import time
import sys
from pathlib import Path
import urllib.parse
import concurrent.futures
from utils import NUM_THREADS, NUM_RETRIES

from utils import HEADERS

employer_name = []
employer_website = []
employer_sectorName = []
employer_ceo = []
employer_main_review_url = []
benefits_url = []
current_url = []
benefits_html = []

def scrape_url(url):
    params = {'api_key': '216c169f3e335ca9ccb47b538e4ae044', 'url': url}

    print('Function is called!')
    try:
        response = requests.get(url, headers=HEADERS, params=urllib.parse.urlencode(params))
        if response.status_code in (200, 404):
            print(f'Success scraping -> {url}')
    except requests.exceptions.ConnectionError:
        print('Except!!')
        response = ''
        current_url.append(url)
        benefits_html.append('')

    if response.status_code == 200:
        current_url.append(url)
        benefits_html.append(response.text)

def recover_db() -> pd.DataFrame:
    db_csv = Path('./db.csv')
    if db_csv.is_file():
        print("Loading database!")
        db = pd.read_csv('db.csv')
    else:
        html_source_code_list = glassdoor_api()
        if not html_source_code_list:
            print('No urls to scrape. Exit program')
            sys.exit(0)
        else:
            db = iterate_html_source(html_source_code_list)
            db.to_csv('db.csv')
    
    return db

def glassdoor_api():
    HTML_source_code_list = []
    for page in range(1,56):
        url_string = f"http://api.glassdoor.com/api/api.htm?t.p=102567&t.k=bEJk395y8Qe&userip=0.0.0.0&useragent&format=json&v=1&action=employers&l=MX&pn={page}"
        try:
            res = requests.get(url_string, headers=HEADERS)
        except requests.exceptions.ConnectionError:
            res = ""

        if res.status_code == 200:
            HTML_source_code_list.append(res.text)
        else:
            #log
            print(f"URL -> {url_string} | cannot be scraped!")
        
        time.sleep(2)
    
    return HTML_source_code_list

def iterate_html_source(html_source_code_list):
    for html_source in html_source_code_list:
        try:
            json_data = json.loads(html_source)
            for employer in json_data['response']['employers']:
                employer_name.append(employer['name'])
                employer_website.append(employer['website'])
    
                if  'sectorName'  in employer.keys():
                    employer_sectorName.append(employer['sectorName'])
                else:
                    employer_sectorName.append('Sin clasificar')
    
                if 'ceo' in employer.keys():
                    employer_ceo.append(employer['ceo']['name'])
                else:
                    employer_ceo.append('Sin información')
    
                employer_main_review_url.append(employer['featuredReview']['attributionURL'])
        except:
            print('cannot obtain data!!')
        
    db = pd.DataFrame({
        'Empresa': employer_name,
        'URL_Empresa': employer_website,
        'Sector': employer_sectorName,
        'CEO': employer_ceo,
        'Review URL': employer_main_review_url,
        'Benefits URL': ''
    })

    return db

def run():
    print('STARTS PROGRAM!!')
    db = recover_db()
    print('DATABASE RECOVERED!')
    urls_to_scrape = db['Review URL'].head(n=1).to_list()
    
    print('START ASYNC SCRAPING!')
    print(urls_to_scrape)


    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        executor.map(scrape_url, urls_to_scrape)

    tmp_db = pd.DataFrame({
        'current_url': current_url,
        'benefits_html': benefits_html
    })

    print('SAVING TMP DB!')
    tmp_db.to_csv('tmp_db.csv')
    print('EXIT PROGRAM!')

if __name__ == "__main__":
    run()