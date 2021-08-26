#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from tqdm import tqdm
import sys
import time
import random
import pickle
import json
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import sqlalchemy
from sqlalchemy import create_engine

# sys.path.insert(0, '../tools/')
# from specialRequests import specialRequests


# In[ ]:


class AnimePlanetCrawler:
    def __init__(self):
        with open('../tools/credentials.json') as file:
            credentials = json.load(file)

        username = credentials["dblogin"]["username"]
        password = credentials["dblogin"]["password"]

        db_string = f"postgresql://{username}:{password}@localhost:5432/animeplanet"
        self.db = create_engine(db_string)
        
#         self.sr = specialRequests()


# In[ ]:


def loadData(self):
    print('loading data...')
    with self.db.connect() as con:
        query = """SELECT url 
                    FROM web_scrape 
                    WHERE html_text IS NOT NULL;"""
        self.done = set(pd.read_sql(sqlalchemy.text(query), con)['url'].to_list())
        
        query = """SELECT url 
                    FROM web_scrape 
                    WHERE html_text IS NULL;"""
        self.pending = set(pd.read_sql(sqlalchemy.text(query), con)['url'].to_list())
        
        self.novel = set()
        self.batch = {}


# In[ ]:


def saveData(self):
    print('saving data...')
    
    self.novel = self.novel.difference(set(self.batch.keys()))
    self.pending = self.pending.difference(set(self.batch.keys()))
    
    data_dict = {'done': list(self.done), 
                 'pending': list(self.pending),
                 'novel': list(self.novel),
                 'batch': self.batch}
    
    with open('../data/urls.pkl','wb') as file:
        pickle.dump(data_dict, file)
    
    with open("../data/urls.json", 'w') as file:
        json.dump(data_dict, file, indent=2) 
        
    batch_dict = {'url': list(self.batch.keys()),
                  'html_text': list(self.batch.values())}
    batch_df = pd.DataFrame(batch_dict)
    
    novel_dict = {'url': list(self.novel),
                  'html_text': [np.NaN for _ in range(len(self.novel))]}
    novel_df = pd.DataFrame(novel_dict)
    
    batch_urls = batch_dict['url']
    novel_urls = novel_dict['url']
    
    with self.db.connect() as con:
        print('\tremoving popped pending data...')
        query = f"""DELETE FROM web_scrape 
                    WHERE url in ({str(batch_urls)[1:-1]})"""
        con.execute(sqlalchemy.text(query))
        
        print('\tsaving done data...')
        batch_df.to_sql('web_scrape', con, index=False, if_exists='append', method='multi')

        try:
            print('\tsaving pending data...')
            novel_df.to_sql('web_scrape', con, index=False, if_exists='append', method='multi')
        except Exception as e: 
            print(e)
            query = f"""UPDATE web_scrape 
                        SET html_text = NULL 
                        WHERE url IN ({str(batch_urls)[1:-1]})"""
            con.execute(sqlalchemy.text(query))
            self.done = self.done.difference(batch_urls)
            self.pending = self.pending.difference(novel_urls)
            self.pending = self.pending.union(batch_urls)
        
    self.batch = {}
    self.novel = set()


# In[ ]:


def popBatch(self):
    
    dist_to10 = 10 - (len(self.done) % 10)
    
    popped_urls = set()
    while len(popped_urls) < dist_to10:
        pop_url = self.pending.pop()

        if pop_url[-1] == '.':
            new_url = pop_url.replace('forum/members', 'users')[:-1]
            self.pending.add(new_url)
                
        popped_urls.add(pop_url)
            
    return popped_urls


# In[ ]:


def scrapePage(url):
    
    if ('forum/members' in url) and (url[-1] == '.'):
        return (url, '')

    resp = requests.get(f'http://192.168.0.3:5000/special-requests?url={url}')
    html_text = resp.text
#     html_text = self.sr.get(url)
    
    return (url, html_text)


# In[ ]:


def scrapePages(urls):
    
    resp = requests.post(f'http://192.168.0.3:5000/special-requests', json={'url':urls})
    url_html_dict = resp.json()
#     html_text = self.sr.get(url)
    
    return url_html_dict.items()


# In[ ]:


def parsePage(html_text):
    if html_text == '':
        return set()

    soup = BeautifulSoup(html_text, 'html.parser')
    
    links = [str(a.get('href')) for a in soup.find_all('a')]
    in_domain_links = filter(lambda x: x and x[0] == '/', links)
    cur_urls = set([f'https://www.anime-planet.com{link}' for link in in_domain_links])
    
    return cur_urls


# In[ ]:


def processCrawlResults(self, url_html_tup):
    html_text_list = [x[1] for x in url_html_tup]
    with Pool(4) as p:
        cur_urls_set_list = p.map(parsePage, html_text_list)
    cur_urls = set().union(*cur_urls_set_list)
    for url, html_text in url_html_tup:
        self.done.add(url)
        self.batch[url] = 'failed scrape' if html_text == '' else html_text
    
    cur_urls = (cur_urls.difference(self.pending)).difference(self.done)
    self.novel.update(cur_urls)
    self.pending.update(cur_urls)


# In[ ]:


def printCrawlProgress(self):
    len_done = len(self.done)
    len_pending = len(self.pending)
    print(len_pending, len_done, 0 if len_pending == 0 else len_done/(len_pending+len_done))
    return len_done


# In[ ]:


def waiter(secs):
    print(f'waiting {secs} secs...')
    for _ in tqdm(range(secs)):
        time.sleep(1)


# In[ ]:


def crawl(self):
    self.loadData()
    print('starting crawl...')
    start_time = time.time()
    
    while len(self.pending) > 0:

        popped_urls = self.popBatch()    

        with ThreadPoolExecutor(max_workers=10) as executor:
            url_html_tup = list(executor.map(scrapePage, popped_urls))
        
        self.processCrawlResults(url_html_tup)

        len_done = self.printCrawlProgress()

        
        if len_done % 100 == 0:
            end_time = time.time()
            print('timer: ', end_time-start_time)
            if len_done % 1000 == 0:
                self.saveData()
                if len_done % 500000 == 0:
                    sleep_time = random.randint(3600*3, 3600*5)
                    waiter(sleep_time)
                elif len_done % 100000 == 0:
                    sleep_time = random.randint(1800, 3600)
                    waiter(sleep_time)
                elif len_done % 10000 == 0:
                    sleep_time = random.randint(300, 600)
                    waiter(sleep_time)
                print('starting crawl...')
                
            else:
                time.sleep(random.randint(5, 10))
                
            start_time = time.time()


# In[ ]:


AnimePlanetCrawler.loadData = loadData
AnimePlanetCrawler.saveData = saveData
AnimePlanetCrawler.popBatch = popBatch
AnimePlanetCrawler.processCrawlResults = processCrawlResults
AnimePlanetCrawler.printCrawlProgress = printCrawlProgress
AnimePlanetCrawler.crawl = crawl


# In[ ]:


crawler = AnimePlanetCrawler()


# In[ ]:


crawler.crawl()


# In[ ]:




