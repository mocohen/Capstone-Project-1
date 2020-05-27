

try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import datetime
import os
import sys

import pandas as pd

import azure.storage.blob as azureblob
import azure.batch._batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

import common.helpers

if __name__ == '__main__':
    global_config = configparser.ConfigParser()
    global_config.read(common.helpers._SAMPLES_CONFIG_FILE_NAME)

    storage_account_key = global_config.get('Storage', 'storageaccountkey')
    storage_account_name = global_config.get('Storage', 'storageaccountname')
    storage_account_suffix = global_config.get(
        'Storage',
        'storageaccountsuffix')


    block_blob_client = azureblob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key,
        endpoint_suffix=storage_account_suffix)

    output_container_name = 'output'


    print("\nListing blobs...")

    # List the blobs in the container
    blob_generator = block_blob_client.list_blobs(output_container_name)
    for blob in blob_generator:
        print(blob.name)
        print("{}".format(blob.name))
        #check if the path contains a folder structure, create the folder structure
        if "/" in "{}".format(blob.name):
            print("there is a path in this")
            #extract the folder path and check if that folder exists locally, and if not create it
            head, tail = os.path.split("{}".format(blob.name))
            print(head)
            print(tail)
            if (os.path.isdir(os.getcwd()+ "/" + head)):
                #download the files to this directory
                print("directory and sub directories exist")
                block_blob_client.get_blob_to_path(output_container_name,blob.name,os.getcwd()+ "/" + head + "/" + tail)
            else:
                #create the diretcory and download the file to it
                print("directory doesn't exist, creating it now")
                os.makedirs(os.getcwd()+ "/" + head, exist_ok=True)
                print("directory created, download initiated")
                block_blob_client.get_blob_to_path(output_container_name,blob.name,os.getcwd()+ "/" + head + "/" + tail)
        else:
            block_blob_client.get_blob_to_path(output_container_name,blob.name,blob.name)

