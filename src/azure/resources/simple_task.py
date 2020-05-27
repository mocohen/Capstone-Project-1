#!/usr/bin/env python

# simple_task.py Code Sample
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER

import os
import sys
import argparse
import csv
from datetime import datetime
import json
from pmdarima import auto_arima

def array_as_string(my_arr):
    s = ''
    for i, val in enumerate(my_arr):
        if i != 0:
            s += ', '
        s += '%.8f' % val
    return s

def format_output(filename, mdl):
    mdl_dict = mdl.to_dict()
    with open(filename, 'w') as fp:
        n_ord = mdl_dict['order']
        s_ord = mdl_dict['seasonal_order']
        fp.write('Order: (%d,%d,%d)x(%d,%d,%d,%d)\n'% 
            (n_ord[0], n_ord[1], n_ord[2], s_ord[0], s_ord[1], s_ord[2], s_ord[3]))
        fp.write('aic: %.4f\nbic: %.4f\noob: %.4f\n' % 
            (mdl_dict['aic'], mdl_dict['bic'], mdl_dict['oob']))
        fp.write('params: %s\n' % array_as_string(mdl_dict['params']))
        fp.write('pvals: %s\n' % array_as_string(mdl_dict['pvalues']))
        fp.write('\n\n'+str(mdl.summary()))

def read_data(input_file_path):

    occupancy_date_time = []
    pct_occupied = []
    with open(input_file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                occupancy_date_time.append(datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"))
                pct_occupied.append(float(row[4]))
                line_count += 1
        print(f'Processed {line_count} lines.') 


    return (np.array(occupancy_date_time), np.array(pct_occupied))

def run_arima(pct_occupied, time_chunks_per_day):

    num_split = int(0.7*len(pct_occupied))
    oob_length = int(0.2*len(pct_occupied))


    tr, tt = pct_occupied[:num_split], pct_occupied[num_split:]

    mdl = auto_arima(tr, error_action='ignore', trace=True,
                     start_p=2, start_q=2, start_P=2, start_Q=2,
                     max_p=10, max_q=10, max_P=10, max_Q=10,
                     d=0, D=0, out_of_sample_size=oob_length, 
                     max_order=None, information_criterion='oob',
                      seasonal=True, m=time_chunks_per_day)
    return mdl

if __name__ == '__main__':
    print("simpletask.py python version:")



    parser = argparse.ArgumentParser(description='SARIMA.')
    parser.add_argument('-b', '--blockid', help='block to model (default: 1001)', type=int, default=1001)
    args = parser.parse_args()

    print(sys.version)
    print('blockid:', args.blockid)

    try:
        import statsmodels as sm
    except:
        print('could not import statsmodels')

    try:
        import numpy as np
    except:
        print('could not import numpy')

    try:
        import pmdarima as pm
    except:
        print('could not import pmdarima')

    print('\nFiles in current directory:')
    for item in os.listdir('.'):
        print(item)

    csvfile = "%d.csv" % args.blockid
    if os.path.isfile(csvfile):
        print('\nSuccess! %s is in current directory' % csvfile)
    else:
        print('\nFailed. %s is NOT in current directory' % csvfile)

    occupancy_date_time, pct_occupied = read_data(csvfile)
    hours = [dt.hour for dt in occupancy_date_time]
    time_chunks_per_day = np.max(hours) - np.min(hours) + 1
    print('time_chunks_per_day:', time_chunks_per_day)
    print(np.min(pct_occupied), np.max(pct_occupied), np.mean(pct_occupied))
    
    mdl = run_arima(pct_occupied, time_chunks_per_day)
    
    print(mdl.summary())

    format_output('%d_model.dat' % args.blockid, mdl)

    # # with open('%d_model.dat' % args.blockid, 'w') as fp:
    # #     json.dump(mdl_dict, fp)
    # for key, value in mdl_dict.items():
    #     print(key, value)

