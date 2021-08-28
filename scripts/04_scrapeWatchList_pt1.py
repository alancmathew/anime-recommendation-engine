#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pickle
import numpy as np
import pandas as pd
import json
import sqlalchemy as sql
from sqlalchemy import create_engine
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from io import StringIO
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import time
import random


# In[ ]:


with open('../tools/credentials.json') as file:
    credentials = json.load(file)
    
username = credentials["dblogin"]["username"]
password = credentials["dblogin"]["password"]


# In[ ]:


db_string = f"postgresql://{username}:{password}@localhost:5432/animeplanet"
db = create_engine(db_string)


# In[ ]:


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# ### Scrape User Watch List (Page 1)

# In[ ]:


usernames = pd.read_sql('SELECT * FROM "user" WHERE num_anime_pages IS NULL;', db)['username'].to_list()


# In[ ]:


def getUserFirstPage(username, attempt=1):
    url = f'https://www.anime-planet.com/users/{username}/anime?sort=title&mylist_view=list'
    
    if attempt == 4:
        return (username, url, '')
    
    try:
        resp = requests.get(f'http://192.168.0.3:5000/special-requests?url={quote(url)}')
        if resp.text != '':
            return (username, url, resp.text)
        
        else:
            return getUserFirstPage(username, attempt+1)
            
    except:
        return getUserFirstPage(username, attempt+1)   


# In[ ]:


def findNumAnimePages(uname_url_html_tup):
    try:
        html_text = uname_url_html_tup[2]
        soup = BeautifulSoup(html_text, 'html.parser')
        if soup.find('table') is None:
            result_tup = (*uname_url_html_tup, 0)
            return result_tup
        ul = soup.find('ul', attrs={'class':'nav'})
        page_nums = []
        for tag in ul.find_all('a'):
            try:
                page_nums.append(int(tag.text))
            except:
                continue

        num_anime_pages = max(page_nums)
        result_tup = (*uname_url_html_tup, num_anime_pages)
        return result_tup
    except:
        result_tup = (*uname_url_html_tup, 1)
        return result_tup


# In[ ]:


def saveData():
    global list_of_tups, result_dict
    
    with Pool(4) as p:
        list_of_tups = p.map(findNumAnimePages, list_of_tups)

    for tup in list_of_tups:
        result_dict['username'].append(tup[0])
        result_dict['url'].append(tup[1])
        result_dict['html_text'].append(tup[2])
        result_dict['num_anime_pages'].append(tup[3])

    list_of_tups = []

    df = pd.DataFrame(result_dict)

    with db.connect() as con:
        query = f"""DELETE FROM "user" 
                    WHERE username in ({str(df['username'].to_list())[1:-1]})"""
        con.execute(sql.text(query))

        df[['username', 'num_anime_pages']].to_sql('user', con, if_exists='append', index=False, method='multi')


        query = f"""DELETE FROM web_scrape 
                    WHERE url in ({str(df['url'].to_list())[1:-1]})"""
        con.execute(sql.text(query))

        df[['url', 'html_text']].to_sql('web_scrape', con, if_exists='append', index=False, method='multi')

    del df
    result_dict = {'username':[], 'url':[], 'html_text':[], 'num_anime_pages':[]}


# In[ ]:


chunksize = 25
list_of_tups = []
result_dict = {'username':[], 'url':[], 'html_text':[], 'num_anime_pages':[]}

username_chunks = chunker(usernames, chunksize)

for idx, username_chunk in enumerate(tqdm(username_chunks, total=len(usernames)/chunksize), 1):
    with ThreadPoolExecutor(max_workers=chunksize) as executor:
        list_of_tups.extend(list(executor.map(getUserFirstPage, username_chunk)))
    
    if (idx != 0 and idx % 10 == 0):
          
        saveData()
        
        if idx % 100 == 0:
            time.sleep(random.randint(30, 60))
        elif idx % 1000 == 0:
            time.sleep(random.randint(300, 600))
        else:
            time.sleep(random.randint(5, 10))
            
    else:
        time.sleep(random.randint(2, 5))
        
saveData()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




