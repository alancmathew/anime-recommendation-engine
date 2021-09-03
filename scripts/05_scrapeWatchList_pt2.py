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


# ### Scrape User Watch List (Additional Pages)

# In[ ]:


query = """
        SELECT * 
        FROM "user" 
        WHERE num_anime_pages > 0;
        """

df = pd.read_sql(sql.text(query), db)
pd.concat([df.head(), df.tail()])


# In[ ]:


def generatePageUrls(row):
    username, num_anime_pages = str(row['username']), int(row['num_anime_pages'])
    urls = [f'https://www.anime-planet.com/users/{username}/anime?sort=title&mylist_view=list&page={i}' for i in range(2, num_anime_pages+1)]
    return urls


# In[ ]:


urls = set(itertools.chain.from_iterable(df.apply(generatePageUrls, axis=1).to_list()))


# In[ ]:


len(urls)


# In[ ]:


query = """
        SELECT url 
        FROM web_scrape 
        WHERE html_text IS NOT NULL
        AND url LIKE 'https://www.anime-planet.com/users/%/anime?sort=title&mylist_view=list&page=%';
        """
completed = set(pd.read_sql(sql.text(query), db)['url'].to_list())


# In[ ]:


len(completed)


# In[ ]:


urls = list(urls.difference(completed))
del completed
random.shuffle(urls)


# In[ ]:


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
            
        if idx % 10000 == 0:
            time.sleep(random.randint(1000, 2000))
        elif idx % 1000 == 0:
            time.sleep(random.randint(100, 200))
        elif idx % 100 == 0:
            time.sleep(random.randint(20, 30))
        else:
            time.sleep(random.randint(10, 20))
    else:       
#         time.sleep(max(0, np.random.normal(2, 0.5)))
        time.sleep(max(min(np.random.poisson(4), 8), 2))
        
saveData()

