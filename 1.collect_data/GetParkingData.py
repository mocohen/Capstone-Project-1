#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sklearn as sk
from datetime import timedelta, date, datetime
from sodapy import Socrata
import requests


# In[2]:


import os, sys, getopt
sys.path.append(os.path.dirname(os.path.abspath('.')))
import app_tokens
import argparse





def fetchData(client, socrata_key, block_series, loop_size, max_attempts):
    
    starttime = datetime.now()


    # columns that are numeric in fetched results
    num_cols = ['paidoccupancy','parkingspacecount']

    for ind, block_key in block_series.iteritems():
        
        # initialize dataframe length to loop size so while loop will evaluate as true
        df_len = loop_size

        # list of dataframes
        dfs = []
        
        # index for returned results
        i = 0

        # set number of attempts for connection
        num_attempts = 0

        #check to see if last record was full or not
        while loop_size == df_len and num_attempts < max_attempts:
            results = ''
            print('ind: %d\tkey: %d\trecords read: %.3fMM\telapsed time:%s' % (ind, block_key, i*loop_size/1e6, datetime.now()-starttime), end='\r')

            try:
                # fetch results from seattle city server
                results = client.get(socrata_key,
                                 sourceelementkey=block_key, 
                                 select='occupancydatetime,paidoccupancy,parkingspacecount',
                                 order='occupancydatetime',
                                 limit=loop_size,
                                 offset=loop_size * i)
                #reset num_attempts
                num_attempts = 0
                
                #update counter
                i += 1

                # get number of results
                df_len = len(results)

            except requests.exceptions.Timeout:
                #increase number of attempts
                num_attempts += 1 

                # ensure while loop reruns       
                df_len = loop_size

            if len(results) > 0:
                try:
                    #convert to dataframe
                    df = pd.DataFrame.from_records(results)
                    
                    # convert to appropriate data types and append to array of dataframes
                    df[num_cols] = df[num_cols].apply(pd.to_numeric)
                    df[['occupancydatetime']] = df[['occupancydatetime']].apply(pd.to_datetime)    
                    dfs.append(df)
                    
                    #  # get length of new dataframe
                    # df_len = len(df)

                except:
                    # if fails, dump results
                    print('\ndump dataframe', df.head())
                    print('\ndump DFs', dfs)
                    print('\ndump results', results)
                    raise SystemExit('\nFailed format data at index %d, block_key %d' % (ind, block_key))
                        #append dataframe to list of dataframe
        else:
            # there was an error  - could not get a specific ID
            if num_attempts >= max_attempts:
                # Print out where it failed, but continue to process rest of data. 
                output_string = 'Failed %d times to connect at index %d, block_key %d' % (num_attempts, ind, block_key)
                file = open('failed/%d' % block_key, 'w')
                file.write('ConnectionError\n')
                file.write(output_string)
                file.close()
                print('\n', output_string, '\n')
                #raise SystemExit()    
            else:
                # success
                # output results
                try:
                    results_df = pd.concat(dfs, ignore_index=True).set_index('occupancydatetime')
                    results_df = results_df.resample('15T').mean()
                    results_df.dropna().to_pickle("data_files/%s/%s.%d.pkl" % (socrata_key, socrata_key, block_key)) 
                except ValueError:
                    if len(dfs) != 0:
                        raise SystemExit('\nFailed to concat index %d, block_key %d' % (ind, block_key))
                    else:
                        print('\nNo results for index %d, block_key %d, continuing...\n' % (ind, block_key))


if __name__ == '__main__':
    start_time = datetime.now()
    
    timeout = 120
    max_attempts = 5

    parser = argparse.ArgumentParser(description='Collect parking data from city of seattle through socrata.')
    parser.add_argument('csv_file', help='csv file with blockface keys', type=str)
    parser.add_argument('-s', '--start', help='first row to read in the csv (default: 0)', type=int, default=0)
    parser.add_argument('-e', '--end', help='last row to read in the csv (default: -1)', type=int, default=-1)
    parser.add_argument('-t', '--timeout', help='number of seconds to wait on request', type=int, default=60)
    parser.add_argument('-m', '--max_attempts', help='number of requests to attempt before giving up (default:5)', type=int, default=5)
    parser.add_argument('-n', '--num_records', help='number of records to request at a time (default:1000)', type=int, default=1000)
    parser.add_argument('-k', '--key', help='socrata key for parking data (default: 2019)', type=str, default='2019')

    args = parser.parse_args()



    blockface_detail = pd.read_csv(args.csv_file, index_col=0)




    #data is delayed 48 hrs
    # socrata data keys for parking data
    #2019 ytd

    socrata_keys = {'2018':'6yaw-2m8q',
                    '2019':'qktt-2bsy',
                    'month':'rke9-rsvs',
                    'day': 'hiyf-7edq',
                    }

    data_ytd = 'qktt-2bsy'
    #last 30 days
    data_mtd = 'rke9-rsvs'
    # last 48 hours
    data_48hrs = 'hiyf-7edq'

    data_dir = './data_files/%s' % socrata_keys[args.key]
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)


    # In[ ]:


    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    #client = Socrata("data.seattle.gov", None)

    # Example authenticated client (needed for non-public datasets):
    client = Socrata('data.seattle.gov',
                     app_tokens.getAppTokens()['seattle_gov'],
                    timeout=args.timeout)   

    fetchData(
        client = client, 
        socrata_key = socrata_keys[args.key], 
        block_series = blockface_detail[args.start:args.end]['sourceelementkey'], 
        loop_size = args.num_records, 
        max_attempts = args.max_attempts,
        )
    print('\nCompleted %d key(s) in %s' % (args.end - args.start, datetime.now()-start_time)) 


