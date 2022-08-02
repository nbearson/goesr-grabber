import typer
from datetime import datetime, timedelta
from enum import Enum
import s3fs

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


def main(satellite:GOESRSatellite=GOESRSatellite.goes16,
         start_time:datetime=datetime(year=2022, month=8, day=1, hour=18),
         end_time:datetime=datetime(year=2022, month=8, day=1, hour=19),
         dry_run:bool=True):

    if dry_run:
        print(f"Listing files of {satellite} from {start_time} to {end_time}")
    else:
        print(f"Downloading files of {satellite} from {start_time} to {end_time}")

    fs = s3fs.S3FileSystem(anon=True)
    s3root = f's3://noaa-{satellite}'

    for product in PRODUCTS:
        delta = end_time - start_time
        for hour in [start_time + timedelta(hours=i) for i in range((delta.days*24) + (delta.seconds // 3600))]:
            remote = f'{s3root}/{product}/{hour.strftime("%Y/%j/%H")}/'
            print(remote)
            files = fs.ls(remote)
            for file in files:
                print(file)
                if not dry_run:
                    fs.get(file, file.split('/')[-1])

if __name__ == "__main__":
    typer.run(main)
