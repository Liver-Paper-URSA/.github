import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
import time 

# 함수: .XPT 파일 다운로드
def download_xpt_file(url, path):
    if not url.startswith('http'):
        url = 'https://wwwn.cdc.gov' + url
    filename = os.path.join(path, url.split('/')[-1])
    with requests.get(url) as r:
        with open(filename, 'wb') as f:
            f.write(r.content)
    print(f'Downloaded {filename}')

# 함수: 상세 페이지에서 .XPT 파일 찾기 및 다운로드
def process_detail_page(detail_page_url, data_path):
    detail_response = requests.get(detail_page_url)
    detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
    xpt_links = [link['href'] for link in detail_soup.find_all('a', href=True) if link['href'].lower().endswith('.xpt')]
    with ThreadPoolExecutor(max_workers=5) as executor:
        for xpt_url in xpt_links:
            executor.submit(download_xpt_file, xpt_url, data_path)

# 메인 코드 시작 시간 기록
start_time = time.time()

# 메인 코드
base_url = 'https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?Cycle=2017-2020'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

ul_tag = soup.find('ul', class_="mb-0 block-list")
a_tags = ul_tag.find_all('a', href=True) if ul_tag else []

parsed_url = urlparse(base_url)
cycle = parse_qs(parsed_url.query).get('Cycle', [''])[0]

base_path = os.path.join(os.getcwd(), cycle)
os.makedirs(base_path, exist_ok=True)

with requests.Session() as session:
    for a_tag in a_tags:
        href = a_tag['href']
        component = parse_qs(urlparse(href).query).get('Component', [''])[0]
        data_path = os.path.join(base_path, component)
        os.makedirs(data_path, exist_ok=True)
        detail_page_url = f"https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component={component}&Cycle={cycle}"
        process_detail_page(detail_page_url, data_path)

# 메인 코드 종료 시간 기록 및 실행 시간 계산
end_time = time.time()
execution_time = end_time - start_time
print(f"Total execution time: {execution_time} seconds")
