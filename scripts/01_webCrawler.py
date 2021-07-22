import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from tqdm import tqdm
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
proxy_cycle = cycle(proxy_list)

# urls = {'novel':{'https://www.anime-planet.com/'}, 'done':set(), 'batch':[]}
with open('../data/urls.pkl','rb') as file:
    urls = pickle.load(file)  
    
def saveUrls():
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(urls, file)

def getCurrentPageUrls(url):
    time.sleep(random.randint(200,1000)/1000)
    cur_urls = set()
    
    proxy = next(proxy_cycle)
    reqs = requests.get(url, proxies={'http': proxy, 'https': proxy})
    soup = BeautifulSoup(reqs.text, 'html.parser')

    for link in soup.find_all('a'):
        try:
            branch = link.get('href')
            if branch[0] == '/':
                cur_urls.add('https://www.anime-planet.com' + branch)
        except:
            pass
    
    if len(cur_urls) == 0:
        urls['novel'] = urls['novel'].union(urls['batch'])
        raise Exception('Scrape Unsucessful')
    
    return cur_urls

def getAllUrls():
    counter = 0
    while len(urls['novel']) > 0:
#        if len(urls['novel']) >= 16:
#            for _ in range(16):
#                pop_url = urls['novel'].pop()
#                if pop_url not in urls['done']:
#                    urls['batch'].append(pop_url)
#
#            with Pool(16) as p:
#                cur_urls = p.map(getCurrentPageUrls, urls['batch'])
#
#            cur_urls = set().union(*cur_urls)
#
#            urls['done'] = urls['done'].union(urls['batch'])
#            
#        else:
        pop_url = urls['novel'].pop()
        if pop_url not in urls['done']:
            cur_urls = getCurrentPageUrls(pop_url)
            urls['done'].add(pop_url)

        
        print(len(urls['novel']), len(urls['done']), 0 if len(urls['novel']) == 0 else len(urls['done'])/len(urls['novel']))

        diff = cur_urls.difference(urls['done'])
        urls['novel'] = urls['novel'].union(diff)
        
        counter += 1; 

        if counter == 10:
            saveUrls()
            counter = 0

getAllUrls()
