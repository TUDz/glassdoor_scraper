""""Glassdoor web scraper

This script allows to scrape the results of 500 companies in glassdoor.com.mx. It creates a data
frame an finally exports the results to an Excel file.

This script mainly depends on 'pandas' and 'scrapy' modules. They can be installed with the 
requirements.txt file and the pip command. 

This script uses csv files to keep information 'live'. This, in case of interruption of the main program
loop. The consumption of scraperapi API is limited, so in order to optimize the consumption of account credits
the information should be saved.
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from scrapy import Selector
import requests
import json
import time
import sys
from pathlib import Path
from urllib.parse import urlencode
import concurrent.futures
import utils
import constants

employer_name = []
employer_website = []
employer_sectorName = []
employer_ceo = []
employer_main_review_url = []
benefits_url = []
current_url = []
current_benefit_url = []
HTML_benefits_page = []

def scrape_url(url: str):
    """Scrape the desired url obtaining the HTML source code

    Parameters
    ----------
    url : str
        The url to scrape the html code
    
    Returns
    ------
    None
    """
    try:
        params = {'api_key': constants.SCRAPERAPI_KEY, 'url': url}
        response = requests.get('http://api.scraperapi.com/', params=urlencode(params))

        if response.status_code in (200, 404):
            print(f'Success scraping -> {url}')
            sel = Selector(text=response.text)
            benefit_link = sel.xpath('//a[contains(@class, "benefits")]/@href').extract_first()

    except requests.exceptions.ConnectionError:
        print('Except!!')
        response = ''
        current_url.append(url)
        benefits_url.append('')

    if response.status_code == 200:
        current_url.append(url)
        benefits_url.append(f"https://www.glassdoor.com.mx{benefit_link}")
    
def recover_db() -> pd.DataFrame:
    """ Recovers the main db file. If file not exists, then proceeds to consume the Glassdoor API
        and generates it. The main db column names are: 
        Empresa,URL_Empresa,Sector,CEO,current_url,benefits_url

    Parameters
    ----------
    None

    Returns
    ------
    DataFrame:
        Returns a new dataframe object.
    """
    if utils.DB_CSV.is_file():
        print("Loading database!")
        db = pd.read_csv(utils.DB_CSV)
    else:
        company_info_list = glassdoor_api()
        if len(company_info_list) == 0:
            print('No urls to scrape. Exit program')
            sys.exit(0)
        else:
            db = iterate_html_source(company_info_list)
            db.to_csv(utils.DB_CSV, index=False)
    
    return db

def glassdoor_api(n: int = 10):
    """Consumes the glassdoor API to create the main list of the companies to be scraped

    Parameters
    ----------
    n:
        Number of companies to be scraped. Default value is 10.

    Returns
    ------
    List:
        Returns a list of JSON with the company main info.
    """
    company_info_list = []
    for page in range(1,n):
        url_string = f"http://api.glassdoor.com/api/api.htm?t.p={constants.GLASSDOOR_USER}&t.k={constants.GLASSDOOR_KEY}&userip=0.0.0.0&useragent&format=json&v=1&action=employers&l=MX&pn={page}"
        try:
            res = requests.get(url_string, headers=utils.HEADERS)
            time.sleep(1)
        except requests.exceptions.ConnectionError:
            res = ""

        if res.status_code == 200:
            company_info_list.append(res.text)
        else:
            print(f"URL -> {url_string} | Error while consuming API")
    
    return company_info_list

def iterate_html_source(html_source_code_list):
    for html_source in html_source_code_list:
        try:
            json_data = json.loads(html_source)
            for employer in json_data['response']['employers']:
                employer_name.append(employer['name'])
                employer_website.append(employer['website'])
    
                if 'sectorName' in employer.keys():
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
        'current_url': employer_main_review_url,
        'benefits_url': ''
    })

    return db

def recover_benefits_html(url):
    try:
        print('Starts scraper function!')
        params = {'api_key': '216c169f3e335ca9ccb47b538e4ae044', 'url': url}
        response = requests.get('http://api.scraperapi.com/', params=urlencode(params))

        if response.status_code in (200, 404):
            print(f'Success scraping -> {url}')
            current_benefit_url.append(url)
            HTML_benefits_page.append(response.text)
        else:
            print(f'Error while scraping -> {url}-CODE: {response.status_code}')

    except requests.exceptions.ConnectionError:
        print('Error!')
        current_benefit_url.append('')
        HTML_benefits_page.append('')    

def run():
    db = recover_db()
    urls_to_scrape = db['current_url'].to_list()
    
    if utils.TMP_DB_PATH.is_file():
        print("Loading tmp database!")
        tmp_db = pd.read_csv(utils.TMP_DB_PATH)
    else:
        print('NO TMP DB FOUND!')
        print('START ASYNC SCRAPING!')
        with concurrent.futures.ThreadPoolExecutor(max_workers=utils.NUM_THREADS) as executor:
            executor.map(scrape_url, urls_to_scrape)
        
        tmp_db = pd.DataFrame({'current_url': current_url,
                                'benefits_url': benefits_url
            })

        tmp_db.to_csv('tmp_db.csv', index=False)
        
    benefit_urls_list = tmp_db['benefits_url'].to_list()

    benefits_db_path = Path("./benefits_db.csv") 
    if benefits_db_path.is_file():
        print("Loading benefits_db database!")
        benefits_db = pd.read_csv('benefits_db.csv')
    else:
        print('NO BENEFITS HTML DB FOUND!')
        print('START ASYNC SCRAPING!')
        with concurrent.futures.ThreadPoolExecutor(max_workers=utils.NUM_THREADS) as executor:
            executor.map(recover_benefits_html, benefit_urls_list)

        benefits_db = pd.DataFrame({
            'benefits_url': current_benefit_url,
            'HTML_benefits_page': HTML_benefits_page
        })
        benefits_db.to_csv('./benefits_db.csv', index=False)

    CURRENT_URL_LIST = []

    benefits_details_db = pd.DataFrame(columns=['benefits_url', 'Maternidad y Paternidad','Jubilación y Finanzas'])
    for ind in benefits_db.index:
        try:
            html_source_code = benefits_db['HTML_benefits_page'][ind]
            current_url = benefits_db['benefits_url'][ind]
            CURRENT_URL_LIST.append(current_url)
            print(f"----------------{current_url}----------------")
            sel = Selector(text=html_source_code)

            tmp_dictionary = {'benefits_url': current_url,
                              'Maternidad y Paternidad': 'N/A',
                              'Jubilación y Finanzas': 'N/A',
                              'Seguros, Salud y Bienestar': 'N/A',
                              'Ventajas y Beneficios': 'N/A',
                              'Desarrollo Personal': 'N/A',
                              'Vacaciones y Días Libres': 'N/A'}

            benefits_list = sel.xpath('//div/div[@class="p-std css-14xbtcm ejjgytf0"]//h3[@data-test="benefitsTabNameHeader"]').extract()
            if len(benefits_list) > 0:
                for e in sel.xpath('//div/div[@class="p-std css-14xbtcm ejjgytf0"]'):
                    benefit_name = e.xpath('.//h3[@data-test="benefitsTabNameHeader"]/text()').extract_first()
                    benefit_details = ','.join(e.xpath('.//a/text()').extract())

                    if benefit_name in tmp_dictionary.keys():
                        tmp_dictionary[benefit_name] = benefit_details
                
            benefits_details_db = benefits_details_db.append(tmp_dictionary, ignore_index=True)
        except Exception as e:
            print(e)
    

    benefits_details_db.to_csv('./benefits_details.csv', index=True)
    first_merge = benefits_db.merge(benefits_details_db, on="benefits_url", how='inner')
    second_merge = tmp_db.merge(first_merge, on="benefits_url", how="inner")


if __name__ == "__main__":
    run()