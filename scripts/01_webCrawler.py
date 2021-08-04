#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
from sqlalchemy import create_engine

sys.path.insert(0, '../tools/')
from specialRequests import specialRequests


# In[2]:


class AnimePlanetCrawler:
    def __init__(self):
        with open('../tools/credentials.json') as file:
            credentials = json.load(file)

        username = credentials["dblogin"]["username"]
        password = credentials["dblogin"]["password"]

        db_string = f"postgresql://{username}:{password}@192.168.0.3:5432/animeplanet"
        self.db = create_engine(db_string)
        
        self.sr = specialRequests()


# In[3]:


def loadData(self):
    print('loading data...')
    with self.db.connect() as con:
        self.done = set(pd.read_sql('SELECT url FROM web_scrape WHERE html_text IS NOT NULL;', con)['url'].to_list())
        self.pending = set(pd.read_sql('SELECT url FROM web_scrape WHERE html_text IS NULL;', con)['url'].to_list())
        self.novel = set()
        self.batch = {}


# In[4]:


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
    
    inter = set(batch_urls).intersection(set(novel_urls))
    
    if (len(inter) != 0):
        print('inter:', inter)
    
    with self.db.connect() as con:
        print('\tremoving popped pending data...')
        con.execute(f"DELETE FROM web_scrape WHERE url in ({str(batch_urls)[1:-1]})")
        
        print('\tsaving done data...')
        batch_df.to_sql('web_scrape', con, index=False, if_exists='append')

        try:
            print('\tsaving pending data...')
            novel_df.to_sql('web_scrape', con, index=False, if_exists='append')
        except Exception as e: 
            print(e)
            con.execute(f"UPDATE web_scrape SET html_text = NULL WHERE url IN ({str(batch_urls)[1:-1]})")
            self.done = self.done.difference(batch_urls)
            self.pending = self.pending.difference(novel_urls)
            self.pending = self.pending.union(batch_urls)
        
    self.batch = {}
    self.novel = set()


# In[5]:


def scrapePage(self, url):    
 
    cur_urls = set()

    html_text = self.sr.get(url)
    soup = BeautifulSoup(html_text, 'html.parser')

    for link in soup.find_all('a'):
        try:
            branch = link.get('href')
            if branch[0] == '/':
                cur_urls.add('https://www.anime-planet.com' + branch)
        except:
            pass
    
    return cur_urls, (url, html_text)


# In[6]:


def popBatch(self):
    disallowed_urls = ['https://www.anime-planet.com/search.php', 'https://www.anime-planet.com/login', 
                       'https://www.anime-planet.com/sign-up']
    
    dist_to25 = (25 - (len(self.done) % 25))
    
    popped_urls = set()
    while len(popped_urls) < dist_to25:
        pop_url = self.pending.pop()

        if pop_url[-1] == '.':
            new_url = pop_url
            new_url = new_url.replace('forum/members', 'users')[:-1]
            if (new_url not in self.done) and (new_url not in disallowed_urls):
                popped_urls.add(new_url)
                
        if (pop_url not in self.done) and (pop_url not in disallowed_urls):
            popped_urls.add(pop_url)
            
    return popped_urls


# In[7]:


def processCrawlResults(self, results):
    cur_urls = set().union(*map(lambda x: x[0], results))
    url_html_tup = map(lambda x: x[1], results)
    for url, html_text in url_html_tup:
        self.done.add(url)
        self.batch[url] = 'failed scrape' if html_text == '' else html_text
    
    cur_urls = (cur_urls.difference(self.pending)).difference(self.done)
    self.novel = self.novel.union(cur_urls)
    self.pending = self.pending.union(cur_urls)


# In[8]:


def printCrawlProgress(self):
    len_done = len(self.done)
    len_pending = len(self.pending)
    print(len_pending, len_done, 0 if len_pending == 0 else len_done/(len_pending+len_done))
    return len_done


# In[9]:


def crawl(self):
    self.loadData()
    print('starting crawl...')
    start_time = time.time()
    while len(self.pending) > 0:

        
        popped_urls = self.popBatch()    
            
        with ThreadPoolExecutor(max_workers=25) as executor:
            results = list(executor.map(self.scrapePage, popped_urls))
        
        self.processCrawlResults(results)
        
        len_done = self.printCrawlProgress()
        
        if len_done % 500 == 0:
            end_time = time.time()
            print('timer: ', end_time-start_time)
            self.saveData()
            print('starting crawl...')
            start_time = time.time()


# In[10]:


AnimePlanetCrawler.loadData = loadData
AnimePlanetCrawler.saveData = saveData
AnimePlanetCrawler.scrapePage = scrapePage
AnimePlanetCrawler.popBatch = popBatch
AnimePlanetCrawler.processCrawlResults = processCrawlResults
AnimePlanetCrawler.printCrawlProgress = printCrawlProgress
AnimePlanetCrawler.crawl = crawl


# In[11]:


crawler = AnimePlanetCrawler()


# In[12]:


crawler.crawl()


# In[ ]:




