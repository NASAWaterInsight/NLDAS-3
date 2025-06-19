import random
from datetime import datetime
from typing import Union
import s3fs
import numpy as np
import xarray as xr
from log_monitor import S3FSLogMonitor
from titiler.xarray.io import Reader
from rio_tiler.io.xarray import XarrayReader
from rio_tiler.models import ImageData

import signal
import functools

class TimeoutException(Exception):
    pass

def timeout(seconds):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            def handler(signum, frame):
                raise TimeoutException("Function timed out after {} seconds".format(seconds))
            
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wrapper
    return decorator

def list_test_files(
    s3fsfs: s3fs.S3FileSystem,
    bucket: str = 'nasa-veda-scratch',
    directory: str = 'NLDAS/netcdf/test_files',
    file_pattern: str = '*_Tair.nc',
) -> list[str]:
    test_files = s3fsfs.glob(f's3://{bucket}/{directory}/{file_pattern}')
    return test_files
    
def select_random_spatial_range(
    spatial_size: Union[int, float],
    precision: Union[int, float],
    lat_range: list = [40, 62],
    lon_range: list = [-125, -103],
):
    # select random latitude and longitude range by selecting a random integeter between
    random_lat = random.uniform(lat_range[0] + spatial_size, lat_range[1])
    random_lat_range = slice(round(random_lat - spatial_size, precision), round(random_lat, precision))
    random_lon = random.uniform(lon_range[0] + spatial_size, lon_range[1])
    random_lon_range = slice(round(random_lon - spatial_size, precision), round(random_lon, precision))
    return random_lat_range, random_lon_range

backend_open_kwargs = {
    'engine': 'h5netcdf', # could also be netcdf4
    'chunks': {}, # chunks={} loads the data with dask using the engine’s preferred chunk size, generally identical to the format’s chunk size
    'backend_kwargs': {'driver_kwds': {'rdcc_nbytes': 0}}, # 0 bytes for raw data chunk cache
    'cache': False, # If True, cache data loaded from the underlying datastore in memory as NumPy arrays when accessed to avoid reading from the underlying data- store multiple times. Defaults to True unless you specify the chunks argument to use dask, in which case it defaults to False. 
    'decode_cf': False
}

def load_series(
    test_file: str,
    s3fsfs: s3fs.S3FileSystem,
    random_lat_range: slice,
    random_lon_range: slice
):
    monitor = S3FSLogMonitor()
    
    test_file_name = test_file.split('/')[-1]
    print(f"starting test for {test_file_name}")
    open_start_time = datetime.now()
    ds = xr.open_dataset(
        s3fsfs.open(f's3://{test_file}'),
        **backend_open_kwargs
    )
    open_end_time = datetime.now()
    da = ds.Tair
    da.sel(lat=random_lat_range, lon=random_lon_range).load()
    load_time = (datetime.now() - open_end_time).total_seconds()
    open_time = (open_end_time - open_start_time).total_seconds()
    ds.close()
    test_info = {
        'filename': test_file_name,
        'chunk_shape': da.encoding['preferred_chunks'],
        'chunk_size (mb)': np.prod(da.encoding['chunksizes']) * da.dtype.itemsize / 1024 / 1024,
        'open (seconds)': open_time,
        'load': load_time,
        'total_time': open_time + load_time
    }
    s3_request_info = monitor.get_summary_table()
    return test_info, s3_request_info


@timeout(60)  # Set timeout to 60 seconds
def tile_with_timeout(src: XarrayReader, tile: tuple, time_idx: int) -> ImageData:
    return src.tile(*tile, indexes=time_idx)

def load_tile(test_file: str, minzoom: int, time_idx: int = 1, variable: str = 'Tair'):
    monitor = S3FSLogMonitor()
    
    test_file_name = test_file.split('/')[-1]
    print(f"starting test for {test_file_name}")
    open_start_time = datetime.now()
    src = Reader(src_path=test_file, variable=variable)
    open_end_time = datetime.now()
    tile = src.tms.tile(src.bounds[0], src.bounds[1], minzoom)
    try:
        tile_with_timeout(src, tile, time_idx)
    except TimeoutException as e:
        print(e)
    tile_time = (datetime.now() - open_end_time).total_seconds()
    open_time = (open_end_time - open_start_time).total_seconds()
    src.close()
    da = src.input
    test_info = {
        'filename': test_file_name,
        'chunk_shape': da.encoding['preferred_chunks'],
        'chunk_size (MB)': np.prod(da.encoding['chunksizes']) * da.dtype.itemsize / 1024 / 1024,
        'open (seconds)': open_time,
        'tile': tile_time,
        'total_time': open_time + tile_time
    }
    s3_request_info = monitor.get_summary_table()
    return test_info, s3_request_info
    