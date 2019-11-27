#!/usr/bin/env python
# coding: utf-8



# In[184]:


# import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from statsmodels.tsa.seasonal import seasonal_decompose




# In[186]:


#load in data
df = pd.read_pickle('1.collect_data/data_files/5min.pkl')


# In[187]:


df.head()


# In[188]:



#df['PercentOccupied'] = df['PercentOccupied'].clip(None, 1)


# In[189]:


df.head()


# In[190]:



from pandas.tseries.holiday import USFederalHolidayCalendar as calendar


# In[191]:


level_values = df.index.get_level_values
blocks = level_values(0).unique()
num_blocks = len(blocks)

all_block_dfs = []
for i, block_ind in enumerate(blocks):
    print('block %d out of %d blocks' % (i+1, num_blocks))
    dfs = []
    for year in range(2012,2020):
        #df.loc[1001].index.hour.max()
        current_year = df.loc[block_ind][df.loc[block_ind].index.year == year]
        if len(current_year) > 0:
            max_hour = current_year.index.hour.max()
            min_hour = current_year.index.hour.min()

            current_year = current_year.resample(rule='H').mean()

            # remove non paying hours:
            current_year = current_year.iloc[current_year.index.indexer_between_time('%d:00:00' % min_hour, 
                                                                  '%d:00:00' % max_hour, 
                                                                  include_start=True, 
                                                                  include_end=True)]
            # remove sundays
            current_year = current_year[current_year.index.dayofweek != 6]

            # remove holidays

            cal = calendar()
            holidays = cal.holidays(start=current_year.index[0], end=current_year.index[-1])

            current_year = current_year[~current_year.index.normalize().isin(holidays)]

            # replace infs with nan
            current_year = current_year.replace(np.inf, np.nan)            

            #ffill parking spaces on block
            current_year['ParkingSpaceCount'] = current_year['ParkingSpaceCount'].fillna(method='ffill')

            # Fill nans with mean by time of day
            mask = current_year.PaidOccupancy.isna()

            filled_by_mean = current_year.groupby([current_year.index.hour]).transform(lambda x: x.fillna(x.mean()))

            # this will show the values that were replaced
            #filled_by_mean[mask]


            dfs.append(filled_by_mean)

    block_dfs = pd.concat(dfs).assign(SourceElementKey=block_ind).set_index('SourceElementKey', append=True).swaplevel(0,1)
    all_block_dfs.append(block_dfs)


# In[ ]:


all_dfs = pd.concat(all_block_dfs)
# make a new column for percent of occupied spaces
all_dfs['PercentOccupied'] = all_dfs['PaidOccupancy']/all_dfs['ParkingSpaceCount']

# In[ ]:


#all_dfs.to_pickle('1.collect_data/data_files/15min.pkl')
all_dfs.to_pickle('1.collect_data/data_files/1hr.pkl')
