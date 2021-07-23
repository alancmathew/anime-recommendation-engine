hard_drive = 'int_drive_0'

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from tqdm import tqdm
import os
import time
import random
import pickle
import json
from itertools import cycle
from multiprocessing import Pool

with open('../tools/key.json', encoding='utf-8') as file:
    key = json.loads(file.read())
    
reqs = requests.get('https://proxy.webshare.io/api/proxy/list/', headers=key)

proxy_list = reqs.json()['results']
proxy_list = list(map(lambda x: f"http://{x['username']}:{x['password']}@{x['proxy_address']}:{x['ports']['http']}/", proxy_list))
random.shuffle(proxy_list)
proxy_cycle = cycle(proxy_list)

user_agent_list = pd.read_csv('../tools/user_agent_list.txt', sep='\t', header=None)
user_agent_list = user_agent_list[0].to_list()

# urls = {'novel':{'https://www.anime-planet.com/'}, 'done':set(), 'batch':[]}
with open('../data/urls.pkl','rb') as file:
    urls = pickle.load(file)  

def saveUrls():
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(urls, file)
        
    with open("../data/urls_novel.json", 'w') as file:
        json.dump(list(urls['novel']), file, indent=2) 

def getCurrentPageUrls(url, try_no=1):
    
    if try_no == 4:
        return set(), {url: ''}
    
    global proxy_cycle
    time.sleep(random.randint(2000,10000)/1000)
    cur_urls = set()
    
    proxy = next(proxy_cycle)
    user_agent = random.choice(user_agent_list)
    headers = {'User-Agent': user_agent}
    try:
        reqs = requests.get(url, proxies={'http': proxy, 'https': proxy}, headers=headers)
    except:
        print(url, proxy)
        return getCurrentPageUrls(url, try_no+1)
    
    if reqs.status_code != 200:
        print(url, proxy)
        return getCurrentPageUrls(url, try_no+1)
        
    
    html_text = reqs.text
    soup = BeautifulSoup(html_text, 'html.parser')

    for link in soup.find_all('a'):
        try:
            branch = link.get('href')
            if branch[0] == '/':
                cur_urls.add('https://www.anime-planet.com' + branch)
        except:
            pass
    
    return cur_urls, {url: html_text}

def getAllUrls():
    counter = 0
    page_data = {'url':[], 'html_text':[]}
    disallowed_urls = ['https://www.anime-planet.com/search.php', 'https://www.anime-planet.com/login',
                       'https://www.anime-planet.com/sign-up']
    while len(urls['novel']) > 0:
        pop_url = urls['novel'].pop()
        if (pop_url not in urls['done']) and (pop_url not in disallowed_urls):
            cur_urls, url_html_text = getCurrentPageUrls(pop_url)
            urls['done'].add(pop_url)
            page_data['url'].append(pop_url)
            page_data['html_text'].append(url_html_text[pop_url])

        
        print(len(urls['novel']), len(urls['done']), 0 if len(urls['novel']) == 0 else len(urls['done'])/len(urls['novel']))

        diff = cur_urls.difference(urls['done'])
        urls['novel'] = urls['novel'].union(diff)
        
        counter += 1; 

        if counter % 10 == 0:
            saveUrls()
            page_df = pd.DataFrame(page_data)
            page_df.to_csv(f'/mnt/{hard_drive}/data/page_data.csv', mode='a', index=False,
                           header=not os.path.exists(f'/mnt/{hard_drive}/data/page_data.csv'))
                
            del page_df
            page_data = {'url':[], 'html_text':[]}

getAllUrls()