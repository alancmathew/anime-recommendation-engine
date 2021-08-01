hard_drive = 'sdb'

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import sys
import time
import random
import pickle
import json
from itertools import cycle
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import sqlalchemy as db
from sqlalchemy import create_engine

sys.path.insert(0, '../tools/')
from specialRequests import specialRequests

with open('../tools/credentials.json') as file:
    credentials = json.load(file)
    
username = credentials["dblogin"]["username"]
password = credentials["dblogin"]["password"]

db_string = f"postgresql://{username}:{password}@192.168.0.3:5432/animeplanet"
db = create_engine(db_string)

def loadData():
    urls = {}
    with db.connect() as con:
        urls['done'] = set(pd.read_sql('SELECT url FROM web_scrape WHERE html_text IS NOT NULL', con)['url'].to_list())
        urls['to_do'] = set(pd.read_sql('SELECT url FROM web_scrape WHERE html_text IS NULL', con)['url'].to_list())
    return urls

urls = loadData()

def saveData(page_data):
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(urls, file)
        
    with open("../data/urls.json", 'w') as file:
        json.dump({'done':list(urls['done']), 'to_do':list(urls['to_do'])}, file, indent=2) 
        
    page_df = pd.DataFrame(page_data)
    
    with db.connect() as con:
        page_df.to_sql('web_scrape', con, index=False, if_exists='append', method='multi')
        
    del page_df

sr = specialRequests()

def getCurrentPageUrls(url):    
    
    time.sleep(random.randint(200, 2000)/1000)
    
    cur_urls = set()

    html_text = sr.get(url)
    soup = BeautifulSoup(html_text, 'html.parser')

    for link in soup.find_all('a'):
        try:
            branch = link.get('href')
            if branch[0] == '/':
                cur_urls.add('https://www.anime-planet.com' + branch)
        except:
            pass
    
    return cur_urls, (url, html_text)

# def getAllUrls():
#     page_data = {'url':[], 'html_text':[]}
#     disallowed_urls = ['https://www.anime-planet.com/search.php', 'https://www.anime-planet.com/login',
#                        'https://www.anime-planet.com/sign-up']
#     while len(urls['novel']) > 0:
#         pop_url = urls['novel'].pop()

#         if pop_url[-1] == '.':
#             pop_url = pop_url.replace('forum/members', 'users')[:-1]

#         if (pop_url not in urls['done']) and (pop_url not in disallowed_urls):
#             cur_urls, html_text = getCurrentPageUrls(pop_url)
#             urls['done'].add(pop_url)
#             page_data['url'].append(pop_url)
#             page_data['html_text'].append(html_text)
        
#             diff = cur_urls.difference(urls['done'])
#             urls['novel'] = urls['novel'].union(diff)
        
#             print(len(urls['novel']), len(urls['done']), 0 if len(urls['novel']) == 0 else len(urls['done'])/len(urls['novel']), pop_url)
        
#             if len(urls['done']) % 100 == 0:
#                 print('saving data...')
#                 saveData(page_data)
#                 page_data = {'url':[], 'html_text':[]}

def getAllUrlsMulti(urls):
    page_data = {'url':[], 'html_text':[]}
    disallowed_urls = ['https://www.anime-planet.com/search.php', 'https://www.anime-planet.com/login',
                       'https://www.anime-planet.com/sign-up']
    start_time = time.time()
    first = True
    while len(urls['to_do']) > 0:
        popped_urls = []
        
        dist_to25 = 25 - (len(urls['done']) % 25)
        
        while len(popped_urls) < dist_to25:
            pop_url = urls['to_do'].pop()

            if pop_url[-1] == '.':
                pop_url = pop_url.replace('forum/members', 'users')[:-1]

            if (pop_url not in urls['done']) and (pop_url not in disallowed_urls):
                popped_urls.append(pop_url)

        with ThreadPoolExecutor(max_workers=25) as executor:
            results = list(executor.map(getCurrentPageUrls, popped_urls))


        urls['done'] = urls['done'].union(popped_urls)

        cur_urls = set().union(*[item[0] for item in results])
        list_of_tuples = [item[1] for item in results]
        url_html_dict = {}
        for tup in list_of_tuples:
            page_data['url'].append(tup[0])
            page_data['html_text'].append(tup[1])

        novel = cur_urls.difference(urls['done'])
        urls['to_do'] = urls['to_do'].union(novel)
        
        for link in novel:
            page_data['url'].append(link)
            page_data['html_text'].append(np.NaN)
        
        print(len(urls['to_do']), len(urls['done']), 0 if len(urls['to_do']) == 0 else len(urls['done'])/len(urls['to_do']))

        len_done = len(urls['done'])
        if len_done % 500 == 0:
            end_time = time.time()
            print('timer: ', end_time-start_time)
            print('saving data...')
            saveData(page_data)
            if len_done % 10000 == 0:
                urls = loadData()
            page_data = {'url':[], 'html_text':[]}
            time.sleep(random.randint(3000, 6000)/1000)
            start_time = time.time()

getAllUrlsMulti(urls)