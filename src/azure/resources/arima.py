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

from azure.storage.table import TableService, TableBatch
from azure.storage.table.models import Entity, EntityProperty, EdmType

from pmdarima import auto_arima


def run_arima(pct_occupied, time_chunks_per_day):

    num_split = int(.7*len(pct_occupied))
    oob_length = int(0.2*len(pct_occupied))


    tr, tt = pct_occupied[:num_split], pct_occupied[num_split:]

    mdl = auto_arima(tr, error_action='ignore', trace=True,
                     start_p=2, start_q=2, start_P=2, start_Q=2,
                     max_p=10, max_q=10, max_P=10, max_Q=10,
                     d=0, D=0, out_of_sample_size=oob_length, 
                     max_order=None, information_criterion='oob',
                      seasonal=True, m=time_chunks_per_day)


def get_data(block_id, sas_url):

    table_service = TableService(azure_storage_account_name, azure_storage_account_key)

    tasks = table_service.query_entities('Parking1hour1BlockAverage', 
                                         filter="PartitionKey eq '%d' and RowKey gt '2019-01-01 00:00:00'" % (1001), 
                                         select='RowKey,PercentOccupied')
    pct_occupieds = []
    hours = []
    for task in tasks:
        pct_occupieds.append(task.PercentOccupied)
        hours.append(datetime.strptime(task.RowKey, "%Y-%m-%d %H:%M:%S").hour)

    time_chunks_per_day = np.max(hours) - np.min(hours) + 1


    run_arima( np.array(pct_occupieds), time_chunks_per_day )


if __name__ == '__main__':
    print("simpletask.py listing files:")



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


