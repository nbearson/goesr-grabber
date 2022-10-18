#!/usr/bin/env python3

import os
#from fileinput import filename
import typer
from datetime import datetime, timedelta
from enum import Enum
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import concurrent.futures
#from threading import Event
#import signal
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

class GOESRSatellite(str, Enum):
    goes16 = "goes16"
    goes17 = "goes17"
    goes18 = "goes18"


PRODUCTS=['ABI-L1b-RadF',
          'ABI-L1b-RadC',
          'ABI-L1b-RadM',
          'EXIS-L1b-SFEU',
          'EXIS-L1b-SFXR',
          'GLM-L2-LCFA',
          'MAG-L1b-GEOF',
          'SEIS-L1b-EHIS',
          'SEIS-L1b-MPSH',
          'SEIS-L1b-MPSL',
          'SEIS-L1b-SGPS',
          'SUVI-L1b-Fe093',
          'SUVI-L1b-Fe131',
          'SUVI-L1b-Fe171',
          'SUVI-L1b-Fe195',
          'SUVI-L1b-Fe284',
          'SUVI-L1b-He303',]


progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
    transient=True
)


def init_s3():
    session = boto3.session.Session()
    s3 = session.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
    return s3

def get_bucket(satellite):
    return f"noaa-{satellite}"

import re
def datetime_from_filename(filename):
    match = re.search(r"_s[0-9]{4}[0-9]{3}[0-9]{2}[0-9]{2}[0-9]{2}", filename)
    return datetime.strptime(match.group(0), "_s%Y%j%H%M%S")

def get_file_list(satellite, product, start_time, end_time):
    s3 = init_s3()
    bucket = get_bucket(satellite)

    delta = end_time - start_time
    objlist = []
    for hour in [start_time + timedelta(hours=i) for i in range((delta.days*24) + (delta.seconds // 3600) + 1)]:
        prefix = f'{product}/{hour.strftime("%Y/%j/%H")}/'
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket,
                                'Prefix': prefix}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for obj in page['Contents']:
                filename = filename_from_obj(obj)
                dt = datetime_from_filename(filename)
                if start_time <= dt <= end_time:
                    objlist.append(obj)
    return objlist

def filename_from_obj(obj):
    return obj['Key'].split('/')[-1]

def download_obj(obj, s3, bucket, progress):

    filesize = obj['Size']
    filename = filename_from_obj(obj)

# https://github.com/Textualize/rich/blob/master/examples/downloader.py
    task = progress.add_task("download", filename=filename, total=filesize)
    s3.download_file(bucket, obj['Key'], filename, Callback=lambda bytes_transferred: progress.update(task, advance=bytes_transferred))

    progress.remove_task(task)
    assert(filesize == os.path.getsize(filename))
    progress.print(filename)

    return filename

def main(satellite:GOESRSatellite=GOESRSatellite.goes16,
         start_time:datetime=datetime(year=2022, month=8, day=1, hour=18),
         end_time:datetime=datetime(year=2022, month=8, day=1, hour=19),
         parallel_downloads:int=1,
         dry_run:bool=True):

    all_objects = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_downloads) as executor:
        future_to_products = {executor.submit(get_file_list, satellite, product, start_time, end_time): product for product in PRODUCTS}
        for future in concurrent.futures.as_completed(future_to_products):
            product = future_to_products[future]
            try:
                product_objs = future.result()
                all_objects = all_objects + product_objs
            except Exception as exc:
                print('%r generated an exception: %s' % (product, exc))


    if dry_run:
        print(f"Listing {satellite} files from {start_time} to {end_time}")
        for obj in all_objects:
            print(filename_from_obj(obj))
    else:
        print(f"Downloading {satellite} files from {start_time} to {end_time}")
        with progress:
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_downloads) as executor:
#            future_to_objs = {executor.submit(download_obj, obj, satellite): obj for obj in all_objects}
                n = 0
                future_to_objs = {}
                s3 = init_s3()
                bucket = get_bucket(satellite)
                for obj in all_objects:
                    future = executor.submit(download_obj, obj, s3, bucket, progress)
                    future_to_objs[future] = obj
                    n=n+1
                for future in concurrent.futures.as_completed(future_to_objs):
                    obj = future_to_objs[future]
                    try:
                        filename = future.result()
                        if not os.path.exists(filename):
                            raise RuntimeError
#                            print(f"Downloaded {filename} : {os.path.getsize(filename)} bytes")
                    except Exception as exc:
                        print('%r generated an exception: %s' % (product, exc))


if __name__ == "__main__":
    typer.run(main)
