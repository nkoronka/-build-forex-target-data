# Responsible for managing raw/processed data from/to storage files or database.


# https://googleapis.dev/python/storage/latest/blobs.html#
# https://console.cloud.google.com/storage/browser/ml_forex_2020;tab=objects?prefix=

import os
from google.cloud import storage

project_dir = os.path.dirname(os.path.realpath(__file__)) + "/.."
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = project_dir+"/credentials/forex-166214-bf279baa58c2.json"


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name, timeout=600)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


def create_local_directories():
    directories = [
        '../data',
        '../data/production',
        '../data/dummy',

        '../data/production/outcome_data',
        '../data/production/targets',
        '../data/production/raw_true_fx',

        '../data/dummy/outcome_data',
        '../data/dummy/targets',
        '../data/dummy/raw_true_fx'
    ]

    for d in directories:
        try:
            os.mkdir(d)
        except OSError:
            print("Creation of the directory %s failed" % d)
        else:
            print("Successfully created the directory %s " % d)


bucket_name = "ml_forex_2020"

registered_data_files = [
    'dummy/raw_true_fx/dummy_25.csv',
    'dummy/raw_true_fx/debug_set.csv',
    'dummy/raw_true_fx/dummy_hour.csv',
    'dummy/raw_true_fx/dummy_trigger_buy_limit.csv',
    'dummy/raw_true_fx/dummy_trigger_buy_stop.csv',
    'dummy/raw_true_fx/dummy_trigger_sell_limit.csv',
    'dummy/raw_true_fx/dummy_trigger_sell_stop.csv',
    'dummy/raw_true_fx/dummy_two_minutes.csv',
    'dummy/raw_true_fx/test.csv',

    'production/outcome_data/header.txt',
    'production/outcome_data/outcome_data.csv',

    'production/raw_true_fx/EURGBP-2017-10.csv'
]

registered_target_files = [
    'production/targets/EURGBP-2017-10_5_5_3600_72f3431.csv',
    'production/targets/EURGBP-2017-10_5_5_4500_72f3431.csv',
    'production/targets/EURGBP-2017-10_5_5_5400_72f3431.csv',
    'production/targets/EURGBP-2017-10_5_5_6300_72f3431.csv',
    'production/targets/EURGBP-2017-10_5_5_7200_72f3431.csv'
]


def handle_files(files_to_handle, operation=None):
    for registered_file in files_to_handle:
        local = "../data/" + registered_file
        remote = registered_file

        if operation == 'upload':
            upload_blob(bucket_name, local, remote)
        elif operation == 'download':
            download_blob(bucket_name, remote, local)
        else:
            print('no operation provided')


if __name__ == '__main__':
    handle_files(registered_target_files, operation='upload')
