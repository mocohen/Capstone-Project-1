# sample2_pools_and_resourcefiles.py Code Sample
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
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function
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

_CONTAINER_NAME = 'poolsandresourcefiles'
_SIMPLE_TASK_NAME = 'simple_task.py'
_SIMPLE_TASK_PATH = os.path.join('resources', 'simple_task.py')
_EXPIRY_TIME = datetime.datetime.utcnow() + datetime.timedelta(hours=72)


def create_pool(batch_client, block_blob_client, pool_id, vm_size, vm_count):
    """Creates an Azure Batch pool with the specified id.
    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str pool_id: The id of the pool to create.
    :param str vm_size: vm size (sku)
    :param int vm_count: number of vms to allocate
    """
    # pick the latest supported 16.04 sku for UbuntuServer
    sku_to_use, image_ref_to_use = \
        common.helpers.select_latest_verified_vm_image_with_node_agent_sku(
            batch_client, 'Canonical', 'UbuntuServer', '18.04')

    block_blob_client.create_container(
        _CONTAINER_NAME,
        fail_on_exist=False)

    sas_url = common.helpers.upload_blob_and_create_sas(
        block_blob_client,
        _CONTAINER_NAME,
        _SIMPLE_TASK_NAME,
        _SIMPLE_TASK_PATH,
        _EXPIRY_TIME)

    start_tasks = []

    pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use),
        vm_size=vm_size,
        target_dedicated_nodes=vm_count,
        start_task=batchmodels.StartTask(
            command_line='/bin/bash -c \"sudo apt-get -y update && export DEBIAN_FRONTEND=noninteractive && sudo apt-get install -y python3-pip && sudo pip3 install numpy statsmodels pmdarima\"' ,
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin)),
        ))

    common.helpers.create_pool_if_not_exist(batch_client, pool)


def submit_job_and_add_task(batch_client, block_blob_client, job_id, pool_id, block_indices, input_files, output_container_sas_url):
    """Submits a job to the Azure Batch service and adds
    a task that runs a python script.
    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param block_blob_client: The storage block blob client to use.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str job_id: The id of the job to create.
    :param str pool_id: The id of the pool to use.
    """
    print(block_indices)

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_client.job.add(job)

    block_blob_client.create_container(
        _CONTAINER_NAME,
        fail_on_exist=False)

    sas_url = common.helpers.upload_blob_and_create_sas(
        block_blob_client,
        _CONTAINER_NAME,
        _SIMPLE_TASK_NAME,
        _SIMPLE_TASK_PATH,
        _EXPIRY_TIME)

    tasks = list()

    # Count how many items are stored in the batch
    inBatch = 0    

    for block, input_file in zip(block_indices, input_files):
        input_file_path = input_file.file_path
        output_file_path = "".join((input_file_path).split('.')[:-1]) + '_model.dat'
        task_file = batchmodels.ResourceFile(
                            file_path=_SIMPLE_TASK_NAME,
                            http_url=sas_url)
        print(type(input_file), type(task_file))
        tasks.append( batchmodels.TaskAddParameter(
            id='Task{}'.format(block),
            command_line="python3 %s -b %d" % (_SIMPLE_TASK_NAME, block),
            resource_files=[task_file, input_file],
            output_files=[batchmodels.OutputFile(
                file_pattern=output_file_path,
                destination=batchmodels.OutputFileDestination(
                          container=batchmodels.OutputFileBlobContainerDestination(
                              container_url=output_container_sas_url)),
                upload_options=batchmodels.OutputFileUploadOptions(
                    upload_condition=batchmodels.OutputFileUploadCondition.task_success))])
        )

        inBatch += 1
        # We can only send batches with up to 100 records
        if inBatch > 99:
            batch_client.task.add_collection(job.id, tasks)
            tasks = []
            inBatch = 0

    if inBatch > 0:
        batch_client.task.add_collection(job.id, tasks)

def create_csv_list(block_indices):
    input_file_paths = []

    for block in block_indices:
        input_file_paths.append(os.path.join(sys.path[0], 'csv_files/%d.csv' % block))

    return input_file_paths    


def execute_sample(global_config, sample_config):
    """Executes the sample with the specified configurations.
    :param global_config: The global configuration to use.
    :type global_config: `configparser.ConfigParser`
    :param sample_config: The sample specific configuration to use.
    :type sample_config: `configparser.ConfigParser`
    """
    # Set up the configuration
    batch_account_key = global_config.get('Batch', 'batchaccountkey')
    batch_account_name = global_config.get('Batch', 'batchaccountname')
    batch_service_url = global_config.get('Batch', 'batchserviceurl')

    storage_account_key = global_config.get('Storage', 'storageaccountkey')
    storage_account_name = global_config.get('Storage', 'storageaccountname')
    storage_account_suffix = global_config.get(
        'Storage',
        'storageaccountsuffix')

    should_delete_container = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletecontainer')
    should_delete_job = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletejob')
    should_delete_pool = sample_config.getboolean(
        'DEFAULT',
        'shoulddeletepool')
    pool_vm_size = sample_config.get(
        'DEFAULT',
        'poolvmsize')
    pool_vm_count = sample_config.getint(
        'DEFAULT',
        'poolvmcount')

    blockfacefile = sample_config.get('BLOCKS', 'blockfacefile')
    blockstart = sample_config.getint('BLOCKS', 'startblockindex')
    blockend = sample_config.getint('BLOCKS', 'endblockindex')

    block_inds = pd.read_csv(blockfacefile).sourceelementkey.values[blockstart:blockend]


    block_indices = []
    for block in block_inds:
        if not os.path.isfile('output/%d_model.dat' % block):
            block_indices.append(block)





    # Print the settings we are running with
    common.helpers.print_configuration(global_config)
    common.helpers.print_configuration(sample_config)

    credentials = batchauth.SharedKeyCredentials(
        batch_account_name,
        batch_account_key)
    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=batch_service_url)

    # Retry 5 times -- default is 3
    batch_client.config.retry_policy.retries = 5

    block_blob_client = azureblob.BlockBlobService(
        account_name=storage_account_name,
        account_key=storage_account_key,
        endpoint_suffix=storage_account_suffix)


    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.

    input_container_name = 'input'
    output_container_name = 'output'
    block_blob_client.create_container(input_container_name, fail_on_exist=False)
    block_blob_client.create_container(output_container_name, fail_on_exist=False)
    print('Container [{}] created.'.format(input_container_name))
    print('Container [{}] created.'.format(output_container_name))

    # generate list of input csv files
    input_file_paths = create_csv_list(block_indices)
    
    # Upload the input files. This is the collection of files that are to be processed by the tasks.
    input_files = [
        common.helpers.upload_file_to_container(block_blob_client, input_container_name, file_path, _EXPIRY_TIME, 60)
        for file_path in input_file_paths]    
    
    # Obtain a shared access signature URL that provides write access to the output
    # container to which the tasks will upload their output.

    output_container_sas_url = common.helpers.get_container_sas_url(
        block_blob_client,
        output_container_name,
        azureblob.BlobPermissions.WRITE, 
        output_container_name)


    job_id = common.helpers.generate_unique_resource_name(
        "PoolsAndResourceFilesJob")
    pool_id = common.helpers.generate_unique_resource_name("ArimaPool")
    try:
        create_pool(
            batch_client,
            block_blob_client,
            pool_id,
            pool_vm_size,
            pool_vm_count)

        submit_job_and_add_task(
            batch_client,
            block_blob_client,
            job_id, pool_id,
            block_indices,
            input_files, output_container_sas_url)

        common.helpers.wait_for_tasks_to_complete(
            batch_client,
            job_id,
            datetime.timedelta(hours=72))

        tasks = batch_client.task.list(job_id)
        task_ids = [task.id for task in tasks]

        common.helpers.print_task_output(batch_client, job_id, task_ids)
    finally:
        # clean up
        if should_delete_container:
            block_blob_client.delete_container(
                _CONTAINER_NAME,
                fail_not_exist=False)
        if should_delete_job:
            print("Deleting job: ", job_id)
            batch_client.job.delete(job_id)
        if should_delete_pool:
            print("Deleting pool: ", pool_id)
            batch_client.pool.delete(pool_id)


if __name__ == '__main__':
    global_config = configparser.ConfigParser()
    global_config.read(common.helpers._SAMPLES_CONFIG_FILE_NAME)

    sample_config = configparser.ConfigParser()
    sample_config.read(
        os.path.splitext(os.path.basename(__file__))[0] + '.cfg')

    execute_sample(global_config, sample_config)
    input('Press ENTER to exit...')