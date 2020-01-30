import pandas as pd
import numpy as np

import datetime

import geopy.distance
from scipy.spatial.distance import pdist
from scipy.spatial.distance import squareform

from pandas.tseries.holiday import USFederalHolidayCalendar as calendar


# function for calculating distance 
# u - lat/long for point A
# v - lat/long for point B
def my_dist(u, v):
    #return geopy distance in miles
    return geopy.distance.distance(u, v).miles

# inputfile - parking occupancy input file
# outputfile - parking occupancy output file
# blockfaceFile - blockface detail file
# distanceCutoff - (in miles) radius cutoff for being considered within 1 block
def averageWithinRadius(inputFile, outputFile, blockfaceFile, distanceCutoff):

    blockface_detail = pd.read_csv(blockfaceFile)

    # coordinates for all blocks
    all_coords = np.array((blockface_detail.latitude.values, blockface_detail.longitude.values)).T

    #distance matrix
    dm = pdist(all_coords, my_dist)

    new_dfs = []
    
    data = pd.read_pickle(inputFile).replace([np.inf, -np.inf], np.nan).dropna()

    # iterate through each block
    for i, row in enumerate(squareform(dm)):
        # mask for blocks that are within the distance cutoff
        close_block_inds = np.where(row < distanceCutoff)
        #block keys within cutoff
        block_keys = blockface_detail.iloc[close_block_inds].sourceelementkey.values
        
        idx = pd.IndexSlice
        df1 = data.loc[idx[block_keys,:],:]
        
        within_x_occupancy = df1.groupby(level=['OccupancyDateTime']).sum()
        within_x_occupancy['PercentOccupied'] = within_x_occupancy['PaidOccupancy'] / within_x_occupancy['ParkingSpaceCount']
        new_dfs.append(within_x_occupancy.assign(SourceElementKey=blockface_detail.iloc[i].sourceelementkey).set_index('SourceElementKey', append=True).swaplevel(0,1))

    # cat all the individual block dfs together
    all_dfs = pd.concat(new_dfs)

    all_dfs.to_pickle(outputFile)


# Custom downsampling to remove non-paying hours from dataset
# inputfile - parking occupancy input file
# outputfile - parking occupancy output file
# resampleTime - resampling string (using pandas notation)
def downsampleData(inputFile, outputFile, resampleTime, impute=True):
    df = pd.read_pickle(inputFile)

    #get blocks in the dataframe
    level_values = df.index.get_level_values
    blocks = level_values(0).unique()
    num_blocks = len(blocks)


    all_block_dfs = []
    # loop through blocks
    for i, block_ind in enumerate(blocks):
        dfs = []
        # loop through years
        for year in range(2012,2020):
            
            #subset current year
            current_year = df.loc[block_ind][df.loc[block_ind].index.year == year]
            
            #if there exists data in current year
            if len(current_year) > 0:
                # get min and max hours
                max_hour = current_year.index.hour.max()
                min_hour = current_year.index.hour.min()

                # resample with pandas resample method
                current_year = current_year.resample(rule=resampleTime).mean()

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

                if impute:            

                    #ffill number of parking spaces on block
                    current_year['ParkingSpaceCount'] = current_year['ParkingSpaceCount'].fillna(method='ffill')

                    # Fill nans with mean by time of day
                    mask = current_year.PaidOccupancy.isna()

                    filled_by_mean = current_year.groupby([current_year.index.hour]).transform(lambda x: x.fillna(x.mean()))

                    # this will show the values that were replaced
                    #filled_by_mean[mask]


                    dfs.append(filled_by_mean)
                else:
                    dfs.append(current_year)

        block_dfs = pd.concat(dfs).assign(SourceElementKey=block_ind).set_index('SourceElementKey', append=True).swaplevel(0,1)
        all_block_dfs.append(block_dfs)




    all_dfs = pd.concat(all_block_dfs)
    # make a new column for percent of occupied spaces
    all_dfs['PercentOccupied'] = all_dfs['PaidOccupancy']/all_dfs['ParkingSpaceCount']



    all_dfs.to_pickle(outputFile)
