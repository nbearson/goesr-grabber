#!/usr/bin/env python

import pytest

import goesrgrabber.goesrgrabber as gg
from datetime import datetime

def test_partial_hour():
    gs = gg.GOESRSatellite("goes16")
    start_time = datetime(year=2022, month=8, day=1, hour=18, minute=15)
    end_time = datetime(year=2022, month=8, day=1, hour=18, minute=30)
    objlist = gg.get_file_list(satellite=gs.goes16,
                               product="ABI-L1b-RadM",
                               start_time=start_time,
                               end_time=end_time)

    for obj in objlist:
        filename = gg.filename_from_obj(obj)
        dt = gg.datetime_from_filename(filename)
        # check that no files are from before our start time or after our end time
        assert(dt > start_time)
        assert(dt < end_time)
#        print(f'{filename} : {dt}')

    # expected number of files per start/end time:
    # repeat 1 per minute
    # multiplied by 16 channels
    # multiplied by two meso sectors (M1 & M2)
    assert(len(objlist) == (end_time - start_time).total_seconds() / 60 * 16 * 2)