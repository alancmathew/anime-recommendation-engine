#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pickle
import numpy as np
import pandas as pd
import json
import sqlalchemy as sql
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from io import StringIO
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor
import threading
from multiprocessing import Pool
import time
import random
import re
import itertools
import zlib
import sys


# In[ ]:


with open('../tools/credentials.json') as file:
    credentials = json.load(file)
    
username = credentials["dblogin"]["username"]
password = credentials["dblogin"]["password"]


# In[ ]:


db_string = f"postgresql://{username}:{password}@192.168.0.3:5432/animeplanet"
db = sql.create_engine(db_string)


# In[ ]:


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# ### Parse User Watch List Tables

# In[ ]:


query = """
        SELECT origin_url
        FROM watch_list;
        """

completed_set = set(pd.read_sql(sql.text(query), db)['origin_url'])


# In[ ]:


def count_rows():
    query = f"""
            SELECT COUNT(*)
            FROM web_scrape
            WHERE html_text IS NOT NULL
            AND url LIKE 'https://www.anime-planet.com/users/%/anime?sort=title&mylist_view=list%';
            """
    
    return pd.read_sql(sql.text(query), db)['count'][0]


# In[ ]:


def read_sql_chunks(chunksize):

    num_rows = count_rows()
    
    for offset in range(0, num_rows, chunksize):
        query = f"""
                SELECT *
                FROM web_scrape
                WHERE html_text IS NOT NULL
                AND url LIKE 'https://www.anime-planet.com/users/%/anime?sort=title&mylist_view=list%'
                LIMIT {chunksize} OFFSET {offset};
                """
            
        chunk = pd.read_sql(sql.text(query), db)
        
        yield chunk


# In[ ]:


def compressText(text):
    return zlib.compress(bytes(text, 'utf-8') if type(text) == str else text)


# In[ ]:


def decompressText(text):
    return str(zlib.decompress(text), 'utf-8')


# In[ ]:


def compressData(data):
    data = data.loc[~data['url'].isin(completed_set)].copy(deep=True)
    
    if data.shape[0] == 0:
        return pd.DataFrame(columns=['url', 'comp_html_bytes'])
    
    data['comp_html_bytes'] = data['html_text'].apply(compressText)
    
    del data['html_text']
    
    return data


# In[ ]:


def parallelize(data, func):
    data_split = np.array_split(data, 15)
    
    with Pool(15) as p:
        data = pd.concat([*p.map(func, data_split)], ignore_index=True)

    return data


# In[ ]:


def saveData(df):
    with db.connect() as con:
        df.to_sql('watch_list', con, if_exists='append', index=False, method='multi')


# In[ ]:


def parseTable(url_html_tup):
    url, html_text = url_html_tup
    try:
        soup = BeautifulSoup(html_text, 'html.parser')
        table = soup.find('table')
        df = pd.read_html(StringIO(str(table)))[0]
        df.columns = ['title', 'type', 'year', 'avg', 'status', 'eps', 'times_watched', 'rating']
        df['times_watched'] = df['times_watched'].str.extract(r'([0-9]*)', expand=False).astype('float')
        df['anime_url'] = [np.where(tag.has_attr('href'), 
                           'https://www.anime-planet.com' + tag.get('href'), 
                           'no link') for tag in [td.find('a') for td in table.find_all('td', attrs={'class':'tableTitle'})]]
        df['anime_url'] = df['anime_url'].astype('string')
        df['username'] = str(re.findall(r'/users/([A-Za-z0-9]*)/', url)[0])
        df['origin_url'] = url
        
        return df
    
    except:
        return pd.DataFrame(columns=['title', 'type', 'year', 'avg', 'status', 
                                     'eps', 'times_watched', 'rating', 'anime_url', 'username', 'origin_url'])


# In[ ]:


chunksize = 1000
num_rows = count_rows()
sql_chunks = read_sql_chunks(chunksize)

with ThreadPoolExecutor(max_workers=1) as executor:
    for idx, chunk in enumerate(tqdm(sql_chunks, total=int(num_rows/chunksize)+1)):
        chunk = chunk.loc[~chunk['url'].isin(completed_set)].copy(deep=True)
        list_of_tups = [tuple(r) for r in chunk.to_numpy()]
        with Pool(14) as p:
            if len(list_of_tups) == 0:
                chunk = pd.DataFrame(columns=['title', 'type', 'year', 'avg', 'status', 
                                              'eps', 'times_watched', 'rating', 'anime_url', 'username', 'origin_url'])
            else:
                chunk = pd.concat([*p.map(parseTable, list_of_tups)], ignore_index=True)
        if idx != 0:
            _ = save_thread.result()
        save_thread = executor.submit(saveData, chunk)

