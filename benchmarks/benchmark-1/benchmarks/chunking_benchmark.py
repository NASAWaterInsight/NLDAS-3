import xarray as xr
import numpy as np
import s3fs
import time
import logging
from typing import Tuple, Dict, List
import pandas as pd
from pathlib import Path
import tempfile
import os
import psutil
import dask
import gc
from dask.diagnostics import ProgressBar

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure Dask to use less memory
dask.config.set(memory_limit='4GB')

def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def calculate_chunk_size(time_steps: int, lat_chunk: int, lon_chunk: int) -> float:
    """Calculate chunk size in MB."""
    bytes_per_cell = 4  # float32
    total_cells = time_steps * lat_chunk * lon_chunk
    return (total_cells * bytes_per_cell) / (1024 * 1024)  # Convert to MB

def calculate_chunk_dimensions(target_size_mb: float, time_steps: int) -> Tuple[int, int]:
    """Calculate lat/lon chunk dimensions to achieve target size in MB."""
    bytes_per_cell = 4  # float32
    total_cells = (target_size_mb * 1024 * 1024) / bytes_per_cell
    cells_per_chunk = total_cells / time_steps
    side_length = int(np.sqrt(cells_per_chunk))
    return side_length, side_length

def create_test_dataset(
    time_steps: int = 24,
    lat_size: int = 1500,  # 15 degrees at 0.01 resolution
    lon_size: int = 3000,  # 30 degrees at 0.01 resolution
    target_chunk_size_mb: float = 10.0
) -> xr.Dataset:
    """Create a test dataset with specified dimensions and target chunk size."""
    # Calculate chunk dimensions to achieve target size
    lat_chunk, lon_chunk = calculate_chunk_dimensions(target_chunk_size_mb, time_steps)
    
    # Create coordinates
    lats = np.linspace(0, 15, lat_size)
    lons = np.linspace(0, 30, lon_size)
    times = pd.date_range('2024-01-01', periods=time_steps, freq='H')
    
    # Create random data
    data = np.random.randn(time_steps, lat_size, lon_size).astype(np.float32)
    
    # Create dataset
    ds = xr.Dataset(
        data_vars={
            'temperature': (['time', 'lat', 'lon'], data)
        },
        coords={
            'time': times,
            'lat': lats,
            'lon': lons
        }
    )
    
    # Set chunking
    ds = ds.chunk({
        'time': time_steps,  # Keep all time steps together
        'lat': lat_chunk,
        'lon': lon_chunk
    })
    
    # Calculate and log actual chunk size
    actual_chunk_size = calculate_chunk_size(time_steps, lat_chunk, lon_chunk)
    logger.info(f"Created dataset with target chunk size: {target_chunk_size_mb:.2f}MB")
    logger.info(f"Actual chunk size: {actual_chunk_size:.2f}MB")
    logger.info(f"Chunk dimensions: time={time_steps}, lat={lat_chunk}, lon={lon_chunk}")
    
    return ds

def upload_to_s3(ds: xr.Dataset, s3_path: str, fs: s3fs.S3FileSystem) -> None:
    """Upload dataset to S3 using a temporary local file."""
    logger.info(f"Uploading dataset to {s3_path}")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        # Write to temporary file
        ds.to_netcdf(tmp_path)
        
        # Upload to S3
        fs.put(tmp_path, s3_path)
        logger.info("Upload complete")
    finally:
        # Clean up temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

def benchmark_timeseries_extraction(
    s3_path: str,
    fs: s3fs.S3FileSystem,
    lat_slice: slice,
    lon_slice: slice,
    time_slice: slice,
    chunk_size: str
) -> Dict[str, float]:
    """Benchmark time series extraction from S3 with detailed timing."""
    logger.info(f"Starting benchmark for {s3_path} with {chunk_size} chunks")
    
    metrics = {}
    
    # Initial memory usage
    metrics['initial_memory_mb'] = get_memory_usage()
    
    # Time opening the dataset
    start_open = time.time()
    # Open with file's chunking strategy
    ds = xr.open_dataset(fs.open(s3_path), engine='netcdf4')
    open_time = time.time() - start_open
    metrics['open_time'] = open_time
    metrics['memory_after_open_mb'] = get_memory_usage()
    
    # Log chunking information
    logger.info(f"Dataset chunking: {ds.chunks}")
    
    # Time creating the selection
    start_select = time.time()
    # Create selection with explicit chunking
    selection = ds.temperature.sel(
        lat=lat_slice,
        lon=lon_slice,
        time=time_slice
    ).chunk({'time': -1})  # Keep time dimension together
    
    # Force Dask to create the task graph
    selection = selection.persist()
    select_time = time.time() - start_select
    metrics['select_time'] = select_time
    metrics['memory_after_select_mb'] = get_memory_usage()
    
    # Log selection shape and chunks
    logger.info(f"Selection shape: {selection.shape}")
    logger.info(f"Selection chunks: {selection.chunks}")
    
    # Time computing the mean
    start_compute = time.time()
    with ProgressBar():
        # Compute mean with explicit rechunking
        ts = selection.mean(['lat', 'lon'])
        # Force computation and wait for result
        ts = ts.compute()
    compute_time = time.time() - start_compute
    metrics['compute_time'] = compute_time
    metrics['memory_after_compute_mb'] = get_memory_usage()
    
    # Calculate total time
    metrics['total_time'] = open_time + select_time + compute_time
    
    # Calculate memory deltas
    metrics['memory_delta_open_mb'] = metrics['memory_after_open_mb'] - metrics['initial_memory_mb']
    metrics['memory_delta_select_mb'] = metrics['memory_after_select_mb'] - metrics['memory_after_open_mb']
    metrics['memory_delta_compute_mb'] = metrics['memory_after_compute_mb'] - metrics['memory_after_select_mb']
    
    # Log final results
    logger.info(f"Total time: {metrics['total_time']:.2f}s")
    logger.info(f"Memory usage: {metrics['memory_after_compute_mb']:.2f} MB")
    
    # Clean up
    del ds
    del selection
    del ts
    gc.collect()
    
    return metrics

def main():
    # Initialize S3 filesystem
    fs = s3fs.S3FileSystem(anon=False)
    
    # Define chunk sizes to test (in MB)
    chunk_sizes = [1, 10, 50, 100, 432]
    
    # Create test datasets with different chunking strategies
    datasets = {}
    for size in chunk_sizes:
        name = f"{size}mb"
        datasets[name] = create_test_dataset(target_chunk_size_mb=size)
    
    # Upload to S3
    bucket = 'nasa-eodc-scratch'
    s3_paths = {}
    for name, ds in datasets.items():
        s3_path = f's3://{bucket}/NLDAS/netcdf/chunking_test/{name}_chunks.nc'
        upload_to_s3(ds, s3_path, fs)
        s3_paths[name] = s3_path
    
    # Define test regions
    test_regions = [
        ('tiny', slice(0, 50), slice(0, 100)),      # 0.5x1 degree area
        ('small', slice(0, 200), slice(0, 400)),    # 2x4 degree area
        ('medium', slice(0, 1000), slice(0, 2000)), # 10x20 degree area
        ('large', slice(0, 1500), slice(0, 3000))   # 15x30 degree area
    ]
    
    # Run benchmarks
    results = []
    time_slice = slice('2024-01-01', '2024-01-01T23:00:00')
    
    for region_name, lat_slice, lon_slice in test_regions:
        for chunk_name in datasets.keys():
            # Run benchmark
            chunk_results = benchmark_timeseries_extraction(
                s3_paths[chunk_name],
                fs,
                lat_slice,
                lon_slice,
                time_slice,
                chunk_name
            )
            chunk_results['chunking'] = chunk_name
            chunk_results['region'] = region_name
            results.append(chunk_results)
            
            # Force garbage collection between tests
            gc.collect()
    
    # Convert results to DataFrame and save
    results_df = pd.DataFrame(results)
    results_df.to_csv('chunking_benchmark_results.csv', index=False)
    logger.info("Benchmark results saved to chunking_benchmark_results.csv")
    
    # Print summary
    print("\nBenchmark Results:")
    print(results_df.to_string())
    
    # Print comparison summary
    print("\nPerformance Comparison Summary:")
    for region in test_regions:
        region_name = region[0]
        region_results = results_df[results_df['region'] == region_name]
        
        print(f"\nRegion: {region_name}")
        for chunk_name in datasets.keys():
            chunk_results = region_results[region_results['chunking'] == chunk_name].iloc[0]
            print(f"\n{chunk_name} chunks:")
            print(f"  Total time: {chunk_results['total_time']:.2f}s")
            print(f"  Memory usage: {chunk_results['memory_after_compute_mb']:.2f} MB")
            print(f"  Open time: {chunk_results['open_time']:.2f}s")
            print(f"  Select time: {chunk_results['select_time']:.2f}s")
            print(f"  Compute time: {chunk_results['compute_time']:.2f}s")

if __name__ == "__main__":
    main() 