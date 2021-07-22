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

user_agent_list = [
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
]

# urls = {'novel':{'https://www.anime-planet.com/'}, 'done':set(), 'batch':[]}
with open('../data/urls.pkl','rb') as file:
    urls = pickle.load(file)  

def saveUrls():
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(urls, file)

def getCurrentPageUrls(url):
    global proxy_cycle
    time.sleep(random.randint(1000,3000)/1000)
    cur_urls = set()
    
    proxy = next(proxy_cycle)
    user_agent = random.choice(user_agent_list)
    headers = {'User-Agent': user_agent}
    try:
        reqs = requests.get(url, proxies={'http': proxy, 'https': proxy}, headers=headers)
    except:
        print(proxy)
        proxy_list.remove(proxy)
        proxy_cycle = cycle(proxy_list)
        return getCurrentPageUrls(url)
    
    if reqs.status_code != 200:
#         urls['novel'] = urls['novel'].union(urls['batch'])
#         urls['novel'].add(url)
        print(proxy)
        proxy_list.remove(proxy)
        proxy_cycle = cycle(proxy_list)
        return getCurrentPageUrls(url)
        
    
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
    while len(urls['novel']) > 0:
        
        # if len(urls['novel']) >= 16:
        #     while len(urls['batch']) < 16:
        #         pop_url = urls['novel'].pop()
        #         if pop_url not in urls['done']:
        #             urls['batch'].append(pop_url)

        #     with Pool(16) as p:
        #         output = p.map(getCurrentPageUrls, urls['batch'])

        #     cur_urls, url_html_text_list = [item[0] for item in output], [item[1] for item in output]
        #     cur_urls = set().union(*cur_urls)

        #     url_html_text = {}
        #     for d in url_html_text_list:
        #         url_html_text.update(d)

        #     for key in url_html_text.keys():
        #         urls['done'].add(key)
        #         page_data['url'].append(key)
        #         page_data['html_text'].append(url_html_text[key])
            
        #     urls['batch'] = []

        # else:
        pop_url = urls['novel'].pop()
        if pop_url not in urls['done']:
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
            page_df.to_csv('../data/page_data.csv', mode='a', index=False,
                           header=not os.path.exists('../data/page_data.csv'))
                
            del page_df
            page_data = {'url':[], 'html_text':[]}

getAllUrls()