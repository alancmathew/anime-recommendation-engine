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

sys.path.insert(0, '../tools/')
from specialRequests import specialRequests

# urls = {'novel':{'https://www.anime-planet.com/'}, 'done':set(), 'batch':[]}
with open('../data/urls.pkl','rb') as file:
    urls = pickle.load(file)  

def saveData(page_data):
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(urls, file)
        
    with open("../data/urls_novel.json", 'w') as file:
        json.dump(list(urls['novel']), file, indent=2) 
        
    page_df = pd.DataFrame(page_data)
    page_df.to_csv(f'/mnt/{hard_drive}/data/page_data.csv', mode='a', index=False,
               header=not os.path.exists(f'/mnt/{hard_drive}/data/page_data.csv'))
    del page_df

sr = specialRequests()

def getCurrentPageUrls(url):    
    time.sleep(random.randint(1000, 5000)/1000)
    
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

def getAllUrlsMulti():
    page_data = {'url':[], 'html_text':[]}
    disallowed_urls = ['https://www.anime-planet.com/search.php', 'https://www.anime-planet.com/login',
                       'https://www.anime-planet.com/sign-up']
    start_time = time.time()
    while len(urls['novel']) > 0:
        popped_urls = []
        while len(popped_urls) < 25:
            pop_url = urls['novel'].pop()

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

        diff = cur_urls.difference(urls['done'])
        urls['novel'] = urls['novel'].union(diff)

        print(len(urls['novel']), len(urls['done']), 0 if len(urls['novel']) == 0 else len(urls['done'])/len(urls['novel']))

        if len(urls['done']) % 500 == 0:
            end_time = time.time()
            print('timer: ', end_time-start_time)
            print('saving data...')
            saveData(page_data)
            page_data = {'url':[], 'html_text':[]}
            time.sleep(random.randint(3000, 6000)/1000)
            start_time = time.time()

getAllUrlsMulti()
