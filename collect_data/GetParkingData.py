#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sklearn as sk
from datetime import timedelta, date, datetime
from sodapy import Socrata


# In[2]:


import os, sys, getopt
sys.path.append(os.path.dirname(os.path.abspath('.')))
import app_tokens

timeout = 120

opts, args = getopt.getopt(sys.argv[1:], "e:s:t:")
for opt, arg in opts:
    if opt == '-s':
        start_ind = int(arg)
    elif opt == '-e':
        end_ind = int(arg)
    elif opt == '-t':
        timeout = int(arg)

# In[3]:


blockface_detail = pd.read_csv('blockface_detail.csv', index_col=0)


# In[4]:


blockface_detail.info()


# In[5]:


blockface_detail.head()


# In[6]:


#data is delayed 48 hrs
# socrata data keys for parking data
#2019 ytd
data_ytd = 'qktt-2bsy'
#last 30 days
data_mtd = 'rke9-rsvs'
# last 48 hours
data_48hrs = 'hiyf-7edq'


# In[ ]:


# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
#client = Socrata("data.seattle.gov", None)

starttime = datetime.now()
# Example authenticated client (needed for non-public datasets):
client = Socrata('data.seattle.gov',
                 app_tokens.getAppTokens()['seattle_gov'],
                timeout=timeout)





# columns that are numeric
num_cols = ['paidoccupancy',]

for ind, block_key in blockface_detail[start_ind:end_ind]['sourceelementkey'].iteritems():
    # Number of records to read at a time
    loop_size = df_len = 1000

    # list of dataframes
    dfs = []
    i = 0
    #check to see if last record was full or not
    while loop_size == df_len:
        #print(i)
        print('ind: %d\tkey: %d\trecords read: %.3fMM\telapsed time:%s' % (ind, block_key, i*loop_size/1e6, datetime.now()-starttime), end='\r')
        # fetch results from seattle city server
        try:
            results = client.get(data_ytd,
                             sourceelementkey=block_key, 
                             select='occupancydatetime,paidoccupancy',
                             order='occupancydatetime',
                             limit=loop_size,
                             offset=loop_size * i)
        except:
            raise SystemExit('\nFailed to connect at index %d, block_key %d' % (ind, block_key))
        
        #convert to dataframe
        df = pd.DataFrame.from_records(results)

        try:
            # convert to appropriate data types
            df[num_cols] = df[num_cols].apply(pd.to_numeric)
            df[['occupancydatetime']] = df[['occupancydatetime']].apply(pd.to_datetime)    
        except:
            raise SystemExit('\nFailed format data at index %d, block_key %d' % (ind, block_key))

        # get length of new dataframe
        df_len = len(df)
        #append dataframe to list of dataframe
        dfs.append(df)
        #update counter
        i += 1
    
    results_df = pd.concat(dfs, ignore_index=True).set_index('occupancydatetime')
    results_df = results_df.resample('15T').mean()
    results_df.dropna().to_pickle("data_files/2019/2019.%d.pkl" % block_key)    


# In[ ]:


read = pd.read_pickle('2019.1001.pkl')


# In[ ]:


read


# In[ ]:




