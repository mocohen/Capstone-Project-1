#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np




# In[3]:


df = pd.read_pickle('1.collect_data/data_files/5min.pkl')


# In[ ]:


df.head()


# In[ ]:


df['PercentOccupied'] = df['PaidOccupancy']/df['ParkingSpaceCount']

df = df.replace(np.inf, np.nan)


# In[ ]:


means = df.mean()

df.fillna(means, inplace=True)


# In[ ]:


df.reset_index(inplace=True)


# In[ ]:


df.head()


# In[ ]:


df.tail().OccupancyDateTime.dt.dayofweek


# In[ ]:


df['Month'] = df.OccupancyDateTime.dt.month
df['Year'] = df.OccupancyDateTime.dt.year
df['Hour'] = df.OccupancyDateTime.dt.hour
df['DayOfWeek'] = df.OccupancyDateTime.dt.dayofweek


# In[ ]:


df['Dummy'] = 0


# In[ ]:


df.head()


# In[ ]:


import lightgbm as lgb


# In[ ]:


y_train = df['PercentOccupied'].values
X_train = df[['Dummy', 'SourceElementKey']]


# In[ ]:


lgb_train = lgb.Dataset(data=X_train, label=y_train)


# In[ ]:


params = {
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': {'mape'},
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': 0
}


# In[ ]:


print('Starting training...')
# train
gbm = lgb.train(params,
                lgb_train)

print('Saving model...')
# save model to file
gbm.save_model('model.txt')






