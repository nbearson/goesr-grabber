#!/usr/bin/env python3

import typer
from datetime import datetime, timedelta
from enum import Enum
from tqdm import tqdm
import boto3
from botocore import UNSIGNED
from botocore.client import Config

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


def hook(t):
  def inner(bytes_amount):
    t.update(bytes_amount)
  return inner


def main(satellite:GOESRSatellite=GOESRSatellite.goes16,
         start_time:datetime=datetime(year=2022, month=8, day=1, hour=18),
         end_time:datetime=datetime(year=2022, month=8, day=1, hour=19),
         progress_bar:bool=True,
         dry_run:bool=True):

    if dry_run:
        print(f"Listing {satellite} files from {start_time} to {end_time}")
    else:
        print(f"Downloading {satellite} files from {start_time} to {end_time}")

    s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version=UNSIGNED))
    bucket = f"noaa-{satellite}"
    for product in PRODUCTS:
        delta = end_time - start_time
        for hour in [start_time + timedelta(hours=i) for i in range((delta.days*24) + (delta.seconds // 3600))]:
            prefix = f'{product}/{hour.strftime("%Y/%j/%H")}/'
            paginator = s3.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': bucket,
                                    'Prefix': prefix}
            page_iterator = paginator.paginate(**operation_parameters)
            for page in page_iterator:
                for obj in page['Contents']:
                    filesize = obj['Size']
                    filename = obj['Key'].split('/')[-1]
                    if dry_run:
                        print(f"{filename}")
                    else:
                        if progress_bar:
                            with tqdm(total=filesize, unit='B', unit_scale=True, desc=filename) as t:
                                s3.download_file(bucket, obj['Key'], filename, Callback=hook(t))
                        else:
                            print(f"{filename}")
                            s3.download_file(bucket, obj['Key'], filename)


if __name__ == "__main__":
    typer.run(main)
