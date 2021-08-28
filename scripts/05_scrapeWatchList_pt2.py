#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
import re
import itertools


# In[2]:


with open('../tools/credentials.json') as file:
    credentials = json.load(file)
    
username = credentials["dblogin"]["username"]
password = credentials["dblogin"]["password"]


# In[3]:


db_string = f"postgresql://{username}:{password}@localhost:5432/animeplanet"
db = create_engine(db_string)


# In[4]:


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# ### Scrape User Watch List (Additional Pages)

# In[5]:


query = """
        SELECT * 
        FROM "user" 
        WHERE num_anime_pages > 0;
        """

df = pd.read_sql(sql.text(query), db)
pd.concat([df.head(), df.tail()])


# In[6]:


def generatePageUrls(row):
    username, num_anime_pages = str(row['username']), int(row['num_anime_pages'])
    urls = [f'https://www.anime-planet.com/users/{username}/anime?sort=title&mylist_view=list&page={i}' for i in range(2, num_anime_pages+1)]
    return urls


# In[7]:


urls = set(itertools.chain.from_iterable(df.apply(generatePageUrls, axis=1).to_list()))


# In[8]:


len(urls)


# In[9]:


query = """
        SELECT url 
        FROM web_scrape 
        WHERE html_text IS NOT NULL
        AND url LIKE 'https://www.anime-planet.com/users/%/anime?sort=title&mylist_view=list&page=%';
        """
completed = set(pd.read_sql(sql.text(query), db)['url'].to_list())


# In[10]:


len(completed)


# In[11]:


urls = sorted(list(urls.difference(completed)))


# In[12]:


len(urls)


# In[ ]:


def getPage(url, attempt=1):
    if attempt == 4:
        return (url, np.NaN)
    
    try:
        resp = requests.get(f'http://192.168.0.3:5000/special-requests?url={quote(url)}')
        if resp.text != '':
            return (url, resp.text)
        
        else:
            return getPage(url, attempt+1)
            
    except:
        return getPage(url, attempt+1)  


# In[ ]:


def saveData():
    global list_of_tups, result_dict
    
    for tup in list_of_tups:
        result_dict['url'].append(tup[0])
        result_dict['html_text'].append(tup[1])

    list_of_tups = []

    df = pd.DataFrame(result_dict)

    with db.connect() as con:
        query = f"""DELETE FROM web_scrape 
                    WHERE url in ({str(df['url'].to_list())[1:-1]})"""
        con.execute(sql.text(query))

        df.to_sql('web_scrape', con, if_exists='append', index=False, method='multi')

    del df
    result_dict = {'url':[], 'html_text':[]}


# In[ ]:


chunksize = 10
list_of_tups = []
result_dict = {'url':[], 'html_text':[]}

url_chunks = chunker(urls, chunksize)

for idx, url_chunk in enumerate(tqdm(url_chunks, total=len(urls)/chunksize), 1):
    with ThreadPoolExecutor(max_workers=chunksize) as executor:
        list_of_tups.extend(list(executor.map(getPage, url_chunk)))
 
    if idx != 0 and idx % 10 == 0:
          
        saveData()
            
        if idx % 100 == 0:
            time.sleep(random.randint(30, 60))
        elif idx % 1000 == 0:
            time.sleep(random.randint(300, 600))
        else:
            time.sleep(random.randint(5, 10))
    else:       
        time.sleep(random.randint(1, 2))
        
saveData()


# In[ ]:




