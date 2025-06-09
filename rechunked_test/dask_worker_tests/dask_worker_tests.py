#!/usr/bin/env python3
"""
Enhanced NLDAS Chunk Performance Benchmarker
Added: AWS S3 support, configuration selection, variable selection
"""
import os
import time
import glob
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import gc
import psutil
import warnings
import signal
import re
from typing import List, Dict, Optional, Tuple
warnings.filterwarnings('ignore')

# AWS S3 imports
try:
    import boto3
    import s3fs
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    print("Warning: boto3/s3fs not available. AWS S3 support disabled.")

# Optional imports with fallbacks
try:
    import dask
    import dask.array as da
    from dask.distributed import Client
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False
    print("Warning: Dask not available. Some parallel operations will be skipped.")

# GPU imports - only try if explicitly requested
GPU_AVAILABLE = False
try:
    import cupy as cp
    GPU_AVAILABLE = True
    print("Info: CuPy detected - GPU operations available")
except ImportError:
    GPU_AVAILABLE = False
    print("Info: CuPy not available - running in CPU-only mode")

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def get_gpu_memory():
    """Get GPU memory usage if available"""
    if GPU_AVAILABLE:
        try:
            mempool = cp.get_default_memory_pool()
            return mempool.used_bytes() / (1024 * 1024)
        except:
            return 0
    return 0

class ConfigurationSelector:
    """Helper class for selecting chunk configurations and variables"""
    
    @staticmethod
    def parse_config_name(config_name: str) -> Optional[Tuple[int, int, int]]:
        """Parse configuration name from various formats"""
        # Format: T06_Y1000_X1800 or similar
        match = re.match(r'T(\d+)_Y(\d+)_X(\d+)', config_name)
        if match:
            return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        
        # Format: (6, 1000, 1800) from tuple string
        match = re.match(r'\((\d+),\s*(\d+),\s*(\d+)\)', config_name.strip())
        if match:
            return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        
        return None
    
    @staticmethod
    def format_config_name(config_tuple: Tuple[int, int, int]) -> str:
        """Format configuration tuple as readable name"""
        return f"T{config_tuple[0]:02d}_Y{config_tuple[1]:04d}_X{config_tuple[2]:04d}"
    
    @staticmethod
    def select_configurations(available_configs: Dict, selected_names: Optional[List[str]] = None) -> Dict:
        """Select specific configurations or return all"""
        if not selected_names:
            return available_configs
        
        selected_configs = {}
        for name in selected_names:
            config_tuple = ConfigurationSelector.parse_config_name(name)
            if config_tuple and config_tuple in available_configs:
                selected_configs[config_tuple] = available_configs[config_tuple]
            else:
                print(f"Warning: Configuration '{name}' not found in available configurations")
        
        return selected_configs
    
    @staticmethod
    def interactive_config_selection(available_configs: Dict) -> Dict:
        """Interactive configuration selection"""
        print(f"\nAvailable configurations ({len(available_configs)}):")
        config_list = list(available_configs.keys())
        
        for i, config in enumerate(config_list):
            formatted_name = ConfigurationSelector.format_config_name(config)
            file_count = len(available_configs[config])
            print(f"  {i+1:2d}. {formatted_name} ({file_count} files)")
        
        print(f"\nSelection options:")
        print(f"  - Enter 'all' to test all configurations")
        print(f"  - Enter specific numbers (e.g., '1,3,5' for configs 1, 3, and 5)")
        print(f"  - Enter configuration names (e.g., 'T06_Y1000_X1800,T24_Y200_X300')")
        
        selection = input("Your selection: ").strip()
        
        if selection.lower() == 'all':
            return available_configs
        
        selected_configs = {}
        
        # Try parsing as numbers
        if re.match(r'^[\d,\s]+$', selection):
            try:
                indices = [int(x.strip()) - 1 for x in selection.split(',')]
                for idx in indices:
                    if 0 <= idx < len(config_list):
                        config = config_list[idx]
                        selected_configs[config] = available_configs[config]
                    else:
                        print(f"Warning: Index {idx+1} out of range")
            except ValueError:
                print("Error parsing selection as numbers")
        
        # Try parsing as configuration names
        else:
            names = [x.strip() for x in selection.split(',')]
            selected_configs = ConfigurationSelector.select_configurations(available_configs, names)
        
        if not selected_configs:
            print("No valid configurations selected. Using all configurations.")
            return available_configs
        
        print(f"Selected {len(selected_configs)} configuration(s):")
        for config in selected_configs:
            formatted_name = ConfigurationSelector.format_config_name(config)
            print(f"  - {formatted_name}")
        
        return selected_configs

class AWSDataLoader:
    """Handle AWS S3 data loading"""
    
    def __init__(self, bucket_name: str, use_anonymous: bool = True):
        self.bucket_name = bucket_name
        self.use_anonymous = use_anonymous
        
        if not AWS_AVAILABLE:
            raise ImportError("boto3 and s3fs required for AWS support")
        
        # Setup S3 filesystem with correct endpoint addressing
        self.fs = None
        self.s3_client = None
        
        # Based on the error, we need to use the specific regional endpoint
        print("Setting up S3 access for nasa-waterinsight bucket...")
        
        try:
            # Use the specific endpoint mentioned in the error
            import os
            os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
            os.environ['AWS_NO_SIGN_REQUEST'] = 'YES'
            
            # Try with region-specific configuration
            self.fs = s3fs.S3FileSystem(
                anon=True,
                client_kwargs={
                    'region_name': 'us-west-2',
                    'endpoint_url': 'https://s3-us-west-2.amazonaws.com'
                }
            )
            
            # Test the connection
            test_path = f"{self.bucket_name}"
            self.fs.ls(test_path, max_items=1)
            
            print(f"✅ S3 filesystem initialized with regional endpoint")
            return
            
        except Exception as e:
            print(f"⚠️  Regional endpoint failed: {e}")
        
        # Fallback: Try without explicit endpoint
        try:
            self.fs = s3fs.S3FileSystem(
                anon=True,
                client_kwargs={'region_name': 'us-west-2'}
            )
            print(f"✅ S3 filesystem initialized with region only")
            return
        except Exception as e:
            print(f"⚠️  Region-only method failed: {e}")
        
        # Last resort: basic anonymous access
        try:
            self.fs = s3fs.S3FileSystem(anon=True)
            print(f"✅ S3 filesystem initialized with basic anonymous access")
            return
        except Exception as e:
            print(f"❌ All S3 initialization methods failed: {e}")
            raise RuntimeError(f"Cannot initialize S3 access. The bucket requires region-specific endpoint addressing.")
    
    def list_configurations(self, base_path: str, year: str) -> Dict:
        """List available chunk configurations in S3 bucket for specific year"""
        print(f"Scanning S3 bucket: {self.bucket_name}/{base_path}/{year}")
        
        full_path = f"{self.bucket_name}/{base_path.strip('/')}"
        
        try:
            # List directories under base path
            dirs = self.fs.ls(full_path)
            configs = {}
            
            for dir_path in dirs:
                # Extract configuration name from directory
                config_name = os.path.basename(dir_path)
                config_tuple = ConfigurationSelector.parse_config_name(config_name)
                
                if config_tuple:
                    # Look for files in the specified year subdirectory
                    try:
                        year_path = f"{dir_path}/{year}"
                        if self.fs.exists(year_path):
                            files = self.fs.glob(f"{year_path}/*.nc")
                            if files:
                                config_files = [f"s3://{f}" for f in files]
                                configs[config_tuple] = sorted(config_files)
                                print(f"  Found {config_name}/{year}: {len(config_files)} files")
                            else:
                                print(f"  Found {config_name}/{year}: 0 .nc files")
                        else:
                            print(f"  {config_name}: Year {year} not available")
                    
                    except Exception as e:
                        print(f"  Warning: Could not access {config_name}/{year}: {e}")
            
            return configs
        
        except Exception as e:
            print(f"Error scanning S3 bucket: {e}")
            return {}

class EnhancedChunkBenchmarker:
    def __init__(self, data_source, use_dask=True, use_gpu=False, n_workers=4, 
                 variable='Rainf', aws_bucket=None, aws_base_path=None, year=None):
        self.data_source = data_source  # Can be local path or 'aws'
        self.use_dask = use_dask and DASK_AVAILABLE
        self.use_gpu = use_gpu and GPU_AVAILABLE
        self.n_workers = n_workers
        self.variable = variable  # Variable to analyze
        self.year = year  # Year to analyze (for AWS)
        self.client = None
        
        # AWS configuration
        self.aws_bucket = aws_bucket
        self.aws_base_path = aws_base_path
        self.aws_loader = None
        
        if self.data_source == 'aws':
            if not aws_bucket:
                raise ValueError("AWS bucket name required for AWS data source")
            if not year:
                raise ValueError("Year required for AWS data source")
            self.aws_loader = AWSDataLoader(aws_bucket, use_anonymous=True)
        
        # Only print GPU warning if user requested GPU but it's not available
        if use_gpu and not GPU_AVAILABLE:
            print("Warning: GPU requested but CuPy not available. Running in CPU-only mode.")
        
        if self.use_dask:
            try:
                self.client = Client(processes=True, n_workers=n_workers, threads_per_worker=2)
                print(f"Dask client started: {self.client}")
            except Exception as e:
                print(f"Failed to start Dask client: {e}")
                self.use_dask = False
    
    def find_chunk_configurations(self, selected_configs=None):
        """Find available chunk configurations from local or AWS source"""
        if self.data_source == 'aws':
            return self._find_aws_configurations(selected_configs)
        else:
            return self._find_local_configurations(selected_configs)
    
    def _find_aws_configurations(self, selected_configs=None):
        """Find configurations in AWS S3"""
        if not self.aws_loader:
            raise ValueError("AWS loader not initialized")
        
        # Get all available configurations for the specified year
        all_configs = self.aws_loader.list_configurations(self.aws_base_path, self.year)
        
        if not all_configs:
            print(f"No configurations found in AWS S3 for year {self.year}")
            return {}
        
        # Apply selection filter if provided
        if selected_configs:
            filtered_configs = ConfigurationSelector.select_configurations(all_configs, selected_configs)
            return filtered_configs
        
        return all_configs
    
    def _find_local_configurations(self, selected_configs=None):
        """Find configurations in local directory"""
        if isinstance(self.data_source, str):
            data_dir = Path(self.data_source)
        else:
            data_dir = self.data_source
        
        configs = {}
        
        # Look for rechunked files
        pattern = "rechunked_*_*.nc"
        files = list(data_dir.glob(pattern))
        
        for file_path in files:
            # Extract chunk config from filename: rechunked_{time}_{lat}_{lon}_original.nc
            parts = file_path.stem.split('_')
            if len(parts) >= 4:
                try:
                    chunk_config = (int(parts[1]), int(parts[2]), int(parts[3]))
                    if chunk_config not in configs:
                        configs[chunk_config] = []
                    configs[chunk_config].append(str(file_path))
                except ValueError:
                    continue
        
        # Sort files by date for each configuration
        for config in configs:
            configs[config] = sorted(configs[config])
        
        print(f"Found {len(configs)} chunk configurations:")
        for config, files in configs.items():
            print(f"  {config}: {len(files)} files")
        
        # Apply selection filter if provided
        if selected_configs:
            filtered_configs = ConfigurationSelector.select_configurations(configs, selected_configs)
            return filtered_configs
        
        # FAIR COMPARISON: Use same number of files for all configurations
        if configs:
            min_files = min(len(file_list) for file_list in configs.values())
            
            print(f"\n🔄 ENSURING FAIR COMPARISON:")
            print(f"   Minimum files available: {min_files}")
            print(f"   Using first {min_files} files from each configuration")
            
            # Limit all configurations to use the same number of files
            fair_configs = {}
            for config, file_list in configs.items():
                fair_configs[config] = file_list[:min_files]
                print(f"   {config}: {len(file_list)} → {len(fair_configs[config])} files")
            
            return fair_configs
        
        return configs
    
    def load_multi_file_dataset(self, file_list, variable=None):
        """Load multiple files as a single dataset"""
        if variable is None:
            variable = self.variable
        
        if self.use_dask:
            # Use xarray with dask for lazy loading
            if self.data_source == 'aws':
                # For S3, we need to open files using s3fs file handles, not direct URLs
                datasets = []
                successful_files = 0
                
                print(f"📁 Loading {len(file_list)} files from S3...")
                
                for i, file_path in enumerate(file_list):
                    try:
                        # Method 1: Use s3fs to open file, then pass to xarray
                        s3_path = file_path.replace('s3://', '')
                        
                        with self.aws_loader.fs.open(s3_path, 'rb') as f:
                            # Try different engines for better compatibility
                            ds = None
                            for engine in ['netcdf4', 'h5netcdf']:
                                try:
                                    ds = xr.open_dataset(
                                        f, 
                                        chunks={'time': -1},
                                        engine=engine
                                    )
                                    break
                                except Exception as engine_error:
                                    print(f"    Engine {engine} failed: {engine_error}")
                                    continue
                            
                            if ds is None:
                                raise ValueError("All engines failed")
                            
                            # Load the dataset into memory to avoid file handle issues
                            ds_loaded = ds.load()
                            ds.close()
                            datasets.append(ds_loaded)
                            successful_files += 1
                            
                        if (i + 1) % 5 == 0:  # Progress update every 5 files
                            print(f"  📄 Loaded {i + 1}/{len(file_list)} files...")
                            
                    except Exception as e:
                        print(f"Warning: Could not open {file_path}: {e}")
                        
                        # Try alternative method: download to temporary location
                        try:
                            import tempfile
                            import shutil
                            
                            print(f"  🔄 Trying alternative method for {file_path}")
                            
                            # Create temporary file
                            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp_file:
                                # Download file content
                                s3_path = file_path.replace('s3://', '')
                                with self.aws_loader.fs.open(s3_path, 'rb') as s3_file:
                                    shutil.copyfileobj(s3_file, tmp_file)
                                tmp_path = tmp_file.name
                            
                            # Open from temporary file with different engines
                            ds = None
                            for engine in ['netcdf4', 'h5netcdf']:
                                try:
                                    ds = xr.open_dataset(tmp_path, chunks={'time': -1}, engine=engine)
                                    break
                                except:
                                    continue
                            
                            if ds is None:
                                raise ValueError("Could not open with any engine")
                            
                            ds_loaded = ds.load()
                            ds.close()
                            datasets.append(ds_loaded)
                            successful_files += 1
                            
                            # Clean up temporary file
                            import os
                            os.unlink(tmp_path)
                            
                        except Exception as e2:
                            print(f"  ❌ Alternative method also failed: {e2}")
                            continue
                
                if datasets:
                    print(f"✅ Successfully loaded {successful_files}/{len(file_list)} files")
                    # Concatenate all datasets
                    ds = xr.concat(datasets, dim='time')
                    print(f"📊 Combined dataset shape: {dict(ds.dims)}")
                else:
                    raise ValueError(f"No datasets could be loaded from {len(file_list)} files")
            else:
                # For local files, use open_mfdataset
                ds = xr.open_mfdataset(
                    file_list, 
                    combine='nested',
                    concat_dim='time',
                    chunks={'time': -1}
                )
        else:
            # Load sequentially without dask
            datasets = []
            for f in file_list:
                try:
                    if self.data_source == 'aws':
                        # Use s3fs file handle method for AWS
                        s3_path = f.replace('s3://', '')
                        with self.aws_loader.fs.open(s3_path, 'rb') as file_handle:
                            # Try different engines
                            ds = None
                            for engine in ['netcdf4', 'h5netcdf']:
                                try:
                                    ds = xr.open_dataset(file_handle, engine=engine)
                                    break
                                except:
                                    continue
                            
                            if ds is None:
                                raise ValueError("Could not open with any engine")
                            
                            datasets.append(ds.load())  # Load into memory
                    else:
                        datasets.append(xr.open_dataset(f))
                except Exception as e:
                    print(f"Warning: Could not open {f}: {e}")
                    continue
            
            if datasets:
                ds = xr.concat(datasets, dim='time')
            else:
                raise ValueError("No datasets could be loaded")
        
        # Return the specified variable
        if variable in ds:
            return ds[variable]
        else:
            # Fallback: try to find any similar variable
            possible_names = [variable, variable.upper(), variable.lower()]
            if variable.lower() == 'rainf':
                possible_names.extend(['RAINF', 'precipitation', 'precip', 'rain'])
            
            for name in possible_names:
                if name in ds:
                    print(f"    Using variable '{name}' instead of '{variable}'")
                    return ds[name]
            
            # If no matching variable found, list available variables
            available_vars = list(ds.data_vars)
            print(f"    Error: Variable '{variable}' not found!")
            print(f"    Available variables: {available_vars}")
            
            # Use first available variable as fallback
            if available_vars:
                first_var = available_vars[0]
                print(f"    Using '{first_var}' as fallback")
                return ds[first_var]
            else:
                raise ValueError(f"No data variables found in dataset")
    
    def _get_penalty_time(self, error_type, elapsed_time):
        """Assign penalty times based on error type"""
        penalties = {
            'MEMORY_ERROR': 9999.0,
            'TIMEOUT': 9998.0,
            'IO_ERROR': 9997.0,
            'RUNTIME_ERROR': 9996.0,
            'UNKNOWN_ERROR': 9995.0
        }
        
        penalty = penalties.get(error_type, 9999.0)
        
        if elapsed_time > 1.0:
            return min(penalty, elapsed_time + 1000)
        else:
            return penalty
    
    def benchmark_operation(self, operation_name, operation_func, file_list, **kwargs):
        """Benchmark a single operation with robust error handling"""
        print(f"    Running: {operation_name}")
        
        times = []
        memory_usage = []
        gpu_memory_usage = []
        error_types = []
        
        n_runs = kwargs.get('n_runs', 3)
        timeout_seconds = kwargs.get('timeout', 300)
        
        consecutive_io_errors = 0
        max_consecutive_io_errors = 2
        
        for run in range(n_runs):
            gc.collect()
            if self.use_gpu and GPU_AVAILABLE:
                try:
                    cp.get_default_memory_pool().free_all_blocks()
                except:
                    pass
            
            mem_before = get_memory_usage()
            gpu_mem_before = get_gpu_memory() if self.use_gpu else 0
            
            start_time = time.time()
            success = False
            error_type = None
            result = None
            
            try:
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")
                
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout_seconds)
                
                try:
                    result = operation_func(file_list, **kwargs)
                    
                    if hasattr(result, 'compute'):
                        result = result.compute()
                    elif hasattr(result, 'values'):
                        result = result.values
                    
                    if hasattr(result, 'sum'):
                        _ = result.sum()
                    elif isinstance(result, np.ndarray):
                        _ = np.sum(result)
                    
                    success = True
                    consecutive_io_errors = 0
                    
                finally:
                    signal.alarm(0)
                    
            except (MemoryError, OSError) as e:
                error_type = "MEMORY_ERROR"
                print(f"      ❌ Memory/OS Error: {str(e)[:100]}...")
                
            except (TimeoutError) as e:
                error_type = "TIMEOUT"
                print(f"      ⏰ Timeout: Operation took longer than {timeout_seconds}s")
                
            except (RuntimeError, ConnectionResetError) as e:
                if "NetCDF" in str(e) or "HDF" in str(e) or "Connection reset" in str(e):
                    error_type = "IO_ERROR" 
                    consecutive_io_errors += 1
                    print(f"      🔧 I/O Error: {str(e)[:100]}...")
                    
                    if consecutive_io_errors >= max_consecutive_io_errors:
                        print(f"      🚫 EARLY TERMINATION: {consecutive_io_errors} consecutive I/O errors")
                        
                        penalty_time = self._get_penalty_time(error_type, time.time() - start_time)
                        for remaining_run in range(run, n_runs):
                            times.append(penalty_time)
                            memory_usage.append(max(0, get_memory_usage() - mem_before))
                            gpu_memory_usage.append(0)
                            error_types.append("IO_ERROR_EARLY_TERMINATION")
                        
                        return {
                            'avg_time': penalty_time,
                            'std_time': 0.0,
                            'avg_memory': np.mean(memory_usage) if memory_usage else 0,
                            'avg_gpu_memory': 0,
                            'success_rate': 0.0,
                            'times': times,
                            'error_types': error_types,
                            'has_errors': True,
                            'early_termination': True,
                            'termination_reason': f"Consecutive I/O errors ({consecutive_io_errors})"
                        }
                else:
                    error_type = "RUNTIME_ERROR"
                    print(f"      ⚠️  Runtime Error: {str(e)[:100]}...")
                    
            except Exception as e:
                error_type = "UNKNOWN_ERROR"
                print(f"      ❓ Unknown Error: {str(e)[:100]}...")
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            mem_after = get_memory_usage()
            gpu_mem_after = get_gpu_memory() if self.use_gpu else 0
            
            if success:
                times.append(elapsed_time)
                memory_usage.append(mem_after - mem_before)
                gpu_memory_usage.append(gpu_mem_after - gpu_mem_before)
                print(f"      ✅ Run {run+1}: {elapsed_time:.2f}s")
            else:
                penalty_time = self._get_penalty_time(error_type, elapsed_time)
                times.append(penalty_time)
                memory_usage.append(max(0, mem_after - mem_before))
                gpu_memory_usage.append(max(0, gpu_mem_after - gpu_mem_before))
                error_types.append(error_type)
                print(f"      ❌ Run {run+1}: Failed ({error_type}) - penalty: {penalty_time:.2f}s")
            
            try:
                if result is not None:
                    del result
                gc.collect()
                
                if not success and self.client:
                    try:
                        self.client.restart()
                    except:
                        pass
                        
            except Exception:
                pass
        
        if len(times) > 0:
            success_rate = len([t for t in times if t < 9999]) / n_runs
            
            return {
                'avg_time': np.mean(times),
                'std_time': np.std(times),
                'avg_memory': np.mean(memory_usage),
                'avg_gpu_memory': np.mean(gpu_memory_usage) if self.use_gpu else 0,
                'success_rate': success_rate,
                'times': times,
                'error_types': error_types,
                'has_errors': len(error_types) > 0,
                'early_termination': False
            }
        else:
            return {
                'avg_time': float('inf'), 
                'success_rate': 0,
                'error_types': ['COMPLETE_FAILURE'],
                'has_errors': True,
                'early_termination': False
            }
    
    def define_essential_operations(self):
        """Define essential operations for the specified variable"""
        operations = []
        
        # ESSENTIAL OPERATION 1: Monthly Time Series
        operations.append({
            'name': f'Monthly Time Series - Single Point ({self.variable})',
            'description': f'Extract {self.variable} time series for weather station (CRITICAL)',
            'category': 'time_series',
            'func': self.op_monthly_timeseries_point,
            'priority': 'critical'
        })
        
        # ESSENTIAL OPERATION 2: Monthly Climatology
        operations.append({
            'name': f'Monthly Climatology ({self.variable})',
            'description': f'Calculate monthly mean {self.variable} climatology (CRITICAL)', 
            'category': 'spatial',
            'func': self.op_monthly_climatology,
            'priority': 'critical'
        })
        
        # ESSENTIAL OPERATION 3: Multi-Point Time Series
        operations.append({
            'name': f'Multi-Point Time Series ({self.variable})',
            'description': f'Multiple weather stations {self.variable} - tests parallel I/O',
            'category': 'time_series',
            'func': self.op_multi_point_timeseries,
            'priority': 'high'
        })
        
        # ESSENTIAL OPERATION 4: GPU operations (if available)
        if self.use_gpu and GPU_AVAILABLE:
            operations.append({
                'name': f'GPU Monthly Average ({self.variable})',
                'description': f'GPU {self.variable} acceleration test - memory transfer efficiency',
                'category': 'gpu',
                'func': self.op_gpu_monthly_average,
                'priority': 'high'
            })
        
        print(f"Defined {len(operations)} essential operations for variable: {self.variable}")
        if self.use_gpu and GPU_AVAILABLE:
            print("  (Including GPU operations)")
        else:
            print("  (CPU-only operations)")
        
        return operations
    
    # Essential operation implementations
    def op_monthly_timeseries_point(self, file_list, **kwargs):
        """Extract time series for a single point"""
        data = self.load_multi_file_dataset(file_list, variable=self.variable)
        lat_idx = data.shape[1] // 2
        lon_idx = data.shape[2] // 2
        return data[:, lat_idx, lon_idx]
    
    def op_monthly_climatology(self, file_list, **kwargs):
        """Calculate monthly climatology"""
        data = self.load_multi_file_dataset(file_list, variable=self.variable)
        return data.mean(dim='time')
    
    def op_multi_point_timeseries(self, file_list, **kwargs):
        """Extract time series for multiple points"""
        data = self.load_multi_file_dataset(file_list, variable=self.variable)
        n_points = 5
        lat_indices = np.linspace(0, data.shape[1]-1, n_points, dtype=int)
        lon_indices = np.linspace(0, data.shape[2]-1, n_points, dtype=int)
        
        results = []
        for lat_idx, lon_idx in zip(lat_indices, lon_indices):
            results.append(data[:, lat_idx, lon_idx])
        
        return xr.concat(results, dim='station')
    
    def op_gpu_monthly_average(self, file_list, **kwargs):
        """Calculate monthly average using GPU"""
        if not (self.use_gpu and GPU_AVAILABLE):
            raise RuntimeError("GPU not available or not enabled")
        
        data = self.load_multi_file_dataset(file_list, variable=self.variable)
        gpu_data = cp.asarray(data.values)
        monthly_avg = cp.mean(gpu_data, axis=0)
        return cp.asnumpy(monthly_avg)
    
    def run_benchmarks(self, n_runs=3, selected_configs=None):
        """Run benchmarks on selected configurations"""
        print("="*60)
        print(f"ENHANCED NLDAS CHUNK BENCHMARKING - {self.variable.upper()}")
        print(f"Data source: {self.data_source}")
        if self.data_source == 'aws':
            print(f"AWS bucket: {self.aws_bucket}/{self.aws_base_path}")
        print("="*60)
        
        # Find configurations
        configs = self.find_chunk_configurations(selected_configs)
        if not configs:
            print("No chunk configurations found!")
            return None
        
        # Define operations
        operations = self.define_essential_operations()
        
        print(f"\nRunning {len(operations)} operations on {len(configs)} configurations")
        if configs:
            print(f"Each configuration tested with {len(list(configs.values())[0])} files for FAIR comparison")
        print(f"Total tests: {len(operations) * len(configs)}")
        if self.use_dask:
            print(f"Using Dask with {self.n_workers} workers")
        if self.use_gpu and GPU_AVAILABLE:
            print("GPU acceleration enabled")
        else:
            print("Running in CPU-only mode")
        
        # Run benchmarks
        results = []
        total_tests = len(operations) * len(configs)
        current_test = 0
        skipped_configs = set()
        
        for config, file_list in configs.items():
            config_name = ConfigurationSelector.format_config_name(config)
            print(f"\n--- Testing Configuration: {config_name} ---")
            print(f"Files: {len(file_list)}")
            
            if config in skipped_configs:
                print(f"🚫 SKIPPING configuration {config_name} - marked as problematic")
                for operation in operations:
                    current_test += 1
                    results.append({
                        'operation': operation['name'],
                        'category': operation['category'],
                        'priority': operation['priority'],
                        'chunk_config': config_name,
                        'chunk_time': config[0],
                        'chunk_lat': config[1],
                        'chunk_lon': config[2],
                        'n_files': len(file_list),
                        'avg_time': 9999.0,
                        'std_time': 0.0,
                        'avg_memory': 0,
                        'avg_gpu_memory': 0,
                        'success_rate': 0.0,
                        'times': [9999.0] * n_runs,
                        'error_types': ['CONFIG_SKIPPED'],
                        'has_errors': True,
                        'early_termination': True
                    })
                continue
            
            config_failed_operations = 0
            
            for operation in operations:
                current_test += 1
                print(f"[{current_test}/{total_tests}] {operation['name']}")
                
                result = self.benchmark_operation(
                    operation['name'],
                    operation['func'],
                    file_list,
                    n_runs=n_runs
                )
                
                if result.get('early_termination', False):
                    config_failed_operations += 1
                    print(f"      🚫 Early termination: {result.get('termination_reason', 'Unknown')}")
                    
                    if config_failed_operations >= 1:
                        print(f"      🚫 MARKING CONFIGURATION {config_name} AS PROBLEMATIC")
                        skipped_configs.add(config)
                
                results.append({
                    'operation': operation['name'],
                    'category': operation['category'],
                    'priority': operation['priority'],
                    'chunk_config': config_name,
                    'chunk_time': config[0],
                    'chunk_lat': config[1],
                    'chunk_lon': config[2],
                    'n_files': len(file_list),
                    **result
                })
                
                # Enhanced result reporting
                if result.get('early_termination', False):
                    print(f"      🚫 TERMINATED EARLY - Configuration appears problematic")
                elif result['success_rate'] > 0:
                    if result.get('has_errors', False):
                        error_summary = ', '.join(set(result.get('error_types', [])))
                        print(f"      ⚠️  Time: {result['avg_time']:.3f} ± {result['std_time']:.3f}s (Errors: {error_summary})")
                    else:
                        print(f"      ✅ Time: {result['avg_time']:.3f} ± {result['std_time']:.3f}s")
                    print(f"      💾 Memory: {result['avg_memory']:.1f} MB")
                    if self.use_gpu and GPU_AVAILABLE:
                        print(f"      🎮 GPU Memory: {result['avg_gpu_memory']:.1f} MB")
                    print(f"      📊 Success Rate: {result['success_rate']*100:.0f}%")
                else:
                    error_summary = ', '.join(set(result.get('error_types', ['UNKNOWN'])))
                    print(f"      ❌ FAILED - Errors: {error_summary}")
                    print(f"      🚫 This chunk config is problematic for this operation")
                
                # If this config is now marked as problematic, break out of operations loop
                if config in skipped_configs:
                    print(f"      🚫 Skipping remaining operations for configuration {config_name}")
                    # Add failed results for remaining operations
                    remaining_operations = operations[operations.index(operation) + 1:]
                    for remaining_op in remaining_operations:
                        current_test += 1
                        results.append({
                            'operation': remaining_op['name'],
                            'category': remaining_op['category'],
                            'priority': remaining_op['priority'],
                            'chunk_config': config_name,
                            'chunk_time': config[0],
                            'chunk_lat': config[1],
                            'chunk_lon': config[2],
                            'n_files': len(file_list),
                            'avg_time': 9999.0,
                            'std_time': 0.0,
                            'avg_memory': 0,
                            'avg_gpu_memory': 0,
                            'success_rate': 0.0,
                            'times': [9999.0] * n_runs,
                            'error_types': ['CONFIG_SKIPPED'],
                            'has_errors': True,
                            'early_termination': True
                        })
                    break
        
        return pd.DataFrame(results)
    
    def analyze_results(self, results_df):
        """Enhanced analysis that handles failed operations as negative scores"""
        if results_df is None or results_df.empty:
            print("No results to analyze!")
            return None
        
        print("\n" + "="*60)
        print("ENHANCED BENCHMARK ANALYSIS")
        print("="*60)
        
        # Separate successful and failed results
        successful = results_df[results_df['success_rate'] > 0].copy()
        failed = results_df[results_df['success_rate'] == 0].copy()
        partial = results_df[(results_df['success_rate'] > 0) & (results_df['success_rate'] < 1)].copy()
        
        print(f"\n📊 OVERALL STATISTICS:")
        print(f"  Total tests: {len(results_df)}")
        print(f"  ✅ Successful: {len(successful)} ({len(successful)/len(results_df)*100:.1f}%)")
        print(f"  ⚠️  Partial success: {len(partial)} ({len(partial)/len(results_df)*100:.1f}%)")
        print(f"  ❌ Failed: {len(failed)} ({len(failed)/len(results_df)*100:.1f}%)")
        
        if successful.empty:
            print("❌ No successful operations! All chunk configurations failed.")
            return None
            
        # 1. Critical operations analysis
        critical_ops = successful[successful['priority'] == 'critical']
        if not critical_ops.empty:
            print("\n🔥 CRITICAL OPERATIONS ANALYSIS:")
            print("-" * 50)
            
            critical_avg = critical_ops.groupby('chunk_config')['avg_time'].mean().sort_values()
            best_critical_config = critical_avg.index[0]
            print(f"🏆 BEST for critical operations: {best_critical_config}")
            print(f"    Average time: {critical_avg.iloc[0]:.3f}s")
            
            print(f"\nPerformance on each critical operation:")
            for op in critical_ops['operation'].unique():
                op_data = critical_ops[critical_ops['operation'] == op]
                if not op_data.empty:
                    best_for_op = op_data.loc[op_data['avg_time'].idxmin()]
                    worst_for_op = op_data.loc[op_data['avg_time'].idxmax()]
                    speedup = worst_for_op['avg_time'] / best_for_op['avg_time']
                    print(f"  📊 {op}:")
                    print(f"      Best: {best_for_op['chunk_config']} ({best_for_op['avg_time']:.3f}s)")
                    print(f"      Speedup vs worst: {speedup:.2f}x")
        
        # 2. Overall ranking
        print(f"\n📈 SUCCESSFUL CONFIGURATION RANKINGS:")
        print("-" * 40)
        
        overall_avg = successful.groupby('chunk_config')['avg_time'].mean().sort_values()
        for i, (config, avg_time) in enumerate(overall_avg.items()):
            rank_emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i+1}."
            
            # Count failures for this config
            config_failures = len(failed[failed['chunk_config'] == config])
            config_total = len(results_df[results_df['chunk_config'] == config])
            reliability = (config_total - config_failures) / config_total * 100
            
            print(f"{rank_emoji} {config}: {avg_time:.3f}s avg (Reliability: {reliability:.0f}%)")
        
        # 3. Final recommendations
        print(f"\n🎯 FINAL RECOMMENDATIONS:")
        print("-" * 25)
        
        if not critical_ops.empty:
            print(f"🏆 BEST FOR CRITICAL ANALYSIS: {best_critical_config}")
            print(f"    (Optimized for critical {self.variable} operations)")
        
        best_overall = overall_avg.index[0]
        print(f"⚡ BEST OVERALL PERFORMANCE: {best_overall}")
        print(f"    (Best average across all {self.variable} operations)")
        
        # Identify problematic configurations
        if not failed.empty:
            print(f"\n⚠️  CONFIGURATIONS TO AVOID:")
            failure_by_config = failed.groupby('chunk_config').size().sort_values(ascending=False)
            for config, fail_count in failure_by_config.items():
                total_ops = len(results_df[results_df['chunk_config'] == config])
                fail_rate = fail_count / total_ops * 100
                if fail_rate > 30:  # Only show configs with >30% failure rate
                    print(f"    🚫 {config} (Failure rate: {fail_rate:.0f}%)")
        
        return successful
    
    def cleanup(self):
        """Clean up resources"""
        if self.client:
            self.client.close()

class AWSDataLoader:
    """Handle AWS S3 data loading"""
    
    def __init__(self, bucket_name: str, use_anonymous: bool = True):
        self.bucket_name = bucket_name
        self.use_anonymous = use_anonymous
        
        if not AWS_AVAILABLE:
            raise ImportError("boto3 and s3fs required for AWS support")
        
        # Setup S3 filesystem with correct endpoint addressing
        self.fs = None
        self.s3_client = None
        
        # Based on the error, we need to use the specific regional endpoint
        print("Setting up S3 access for nasa-waterinsight bucket...")
        
        try:
            # Use the specific endpoint mentioned in the error
            import os
            os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
            os.environ['AWS_NO_SIGN_REQUEST'] = 'YES'
            
            # Try with region-specific configuration
            self.fs = s3fs.S3FileSystem(
                anon=True,
                client_kwargs={
                    'region_name': 'us-west-2',
                    'endpoint_url': 'https://s3-us-west-2.amazonaws.com'
                }
            )
            
            # Test the connection
            test_path = f"{self.bucket_name}"
            self.fs.ls(test_path, max_items=1)
            
            print(f"✅ S3 filesystem initialized with regional endpoint")
            return
            
        except Exception as e:
            print(f"⚠️  Regional endpoint failed: {e}")
        
        # Fallback: Try without explicit endpoint
        try:
            self.fs = s3fs.S3FileSystem(
                anon=True,
                client_kwargs={'region_name': 'us-west-2'}
            )
            print(f"✅ S3 filesystem initialized with region only")
            return
        except Exception as e:
            print(f"⚠️  Region-only method failed: {e}")
        
        # Last resort: basic anonymous access
        try:
            self.fs = s3fs.S3FileSystem(anon=True)
            print(f"✅ S3 filesystem initialized with basic anonymous access")
            return
        except Exception as e:
            print(f"❌ All S3 initialization methods failed: {e}")
            raise RuntimeError(f"Cannot initialize S3 access. The bucket requires region-specific endpoint addressing.")
    
    def list_available_years(self, base_path: str) -> List[str]:
        """List available years across all configurations"""
        print(f"Scanning for available years in: {self.bucket_name}/{base_path}")
        
        if not self.fs:
            print("❌ S3 filesystem not initialized")
            return ['2023']  # Fallback
        
        full_path = f"{self.bucket_name}/{base_path.strip('/')}"
        years = set()
        
        # Method 1: Try to list directories
        try:
            config_dirs = self.fs.ls(full_path)
            print(f"Found {len(config_dirs)} configuration directories")
            
            for config_dir in config_dirs:
                try:
                    year_dirs = self.fs.ls(config_dir)
                    for year_dir in year_dirs:
                        year_name = os.path.basename(year_dir)
                        # Check if it looks like a year (4 digits)
                        if re.match(r'^\d{4}$', year_name):
                            years.add(year_name)
                            print(f"  Found year: {year_name} in {os.path.basename(config_dir)}")
                except Exception as e:
                    print(f"  Warning: Could not scan {config_dir}: {e}")
                    continue
            
            if years:
                available_years = sorted(list(years))
                print(f"Available years: {available_years}")
                return available_years
                
        except Exception as e:
            print(f"Method 1 failed: {e}")
        
        # Method 2: Try direct path checking for common years
        print("💡 Trying alternative year detection method...")
        known_years = ['2020', '2021', '2022', '2023', '2024']
        found_years = []
        
        # Test with a known configuration pattern
        test_configs = ['T01_Y250_X450', 'T06_Y200_X360', 'T24_Y100_X180']
        
        for year in known_years:
            year_found = False
            for test_config in test_configs:
                try:
                    test_path = f"{full_path}/{test_config}/{year}"
                    if self.fs.exists(test_path):
                        found_years.append(year)
                        year_found = True
                        print(f"  Found year {year} in {test_config}")
                        break
                except:
                    continue
            if year_found:
                break  # Found at least one year
        
        if found_years:
            print(f"Found years using alternative method: {found_years}")
            return found_years
        
        # Method 3: Manual specification fallback
        print("💡 Could not auto-detect years. Providing common options...")
        return ['2023']  # Most likely year based on your earlier output
    
    def list_configurations(self, base_path: str, year: str) -> Dict:
        """List available chunk configurations in S3 bucket for specific year"""
        print(f"Scanning S3 bucket: {self.bucket_name}/{base_path}/{year}")
        
        full_path = f"{self.bucket_name}/{base_path.strip('/')}"
        
        try:
            # List directories under base path
            dirs = self.fs.ls(full_path)
            configs = {}
            
            print(f"Found {len(dirs)} potential configuration directories")
            
            for dir_path in dirs:
                # Extract configuration name from directory
                config_name = os.path.basename(dir_path)
                config_tuple = ConfigurationSelector.parse_config_name(config_name)
                
                if config_tuple:
                    # Look for files in the specified year subdirectory
                    try:
                        year_path = f"{dir_path}/{year}"
                        if self.fs.exists(year_path):
                            files = self.fs.glob(f"{year_path}/*.nc")
                            if files:
                                config_files = [f"s3://{f}" for f in files]
                                configs[config_tuple] = sorted(config_files)
                                print(f"  Found {config_name}/{year}: {len(config_files)} files")
                            else:
                                print(f"  Found {config_name}/{year}: 0 .nc files")
                        else:
                            print(f"  {config_name}: Year {year} not available")
                    
                    except Exception as e:
                        print(f"  Warning: Could not access {config_name}/{year}: {e}")
                else:
                    print(f"  Skipping non-configuration directory: {config_name}")
            
            print(f"\nTotal configurations found: {len(configs)}")
            return configs
        
        except Exception as e:
            print(f"Error scanning S3 bucket: {e}")
            print("💡 This might be due to S3 access permissions or configuration issues.")
            print("💡 Try setting these environment variables:")
            print("   export AWS_DEFAULT_REGION=us-west-2")
            print("   export AWS_NO_SIGN_REQUEST=YES")
            return {}

def get_user_configuration():
    """Interactive configuration setup"""
    print("Enhanced NLDAS Chunk Performance Benchmarker")
    print("=" * 50)
    
    # Data source selection
    print("\n📁 DATA SOURCE SELECTION:")
    print("1. Local directory")
    print("2. AWS S3 bucket")
    
    source_choice = input("Select data source (1 or 2, default: 1): ").strip()
    
    if source_choice == '2' and AWS_AVAILABLE:
        data_source = 'aws'
        aws_bucket = input("Enter AWS S3 bucket name (default: nasa-waterinsight): ").strip()
        if not aws_bucket:
            aws_bucket = "nasa-waterinsight"
        
        aws_base_path = input("Enter base path in bucket (default: NLDAS3/forcing/rechunked_test): ").strip()
        if not aws_base_path:
            aws_base_path = "NLDAS3/forcing/rechunked_test"
        
        print(f"Scanning for available years: s3://{aws_bucket}/{aws_base_path}")
        
        # Create temporary AWS loader to get available years
        try:
            temp_loader = AWSDataLoader(aws_bucket, use_anonymous=True)
            available_years = temp_loader.list_available_years(aws_base_path)
            
            if not available_years:
                print("❌ No years found in the bucket. Please check the path.")
                return None
            
            print(f"\n📅 YEAR SELECTION:")
            print(f"Available years: {', '.join(available_years)}")
            
            if len(available_years) == 1:
                year = available_years[0]
                print(f"Only one year available, using: {year}")
            else:
                year = input(f"Enter year to analyze (available: {', '.join(available_years)}): ").strip()
                if year not in available_years:
                    print(f"Invalid year. Using most recent: {available_years[-1]}")
                    year = available_years[-1]
            
        except Exception as e:
            print(f"Error accessing AWS: {e}")
            print("Please check your bucket name and path.")
            return None
        
        print(f"Will analyze data from: s3://{aws_bucket}/{aws_base_path}/*/{year}/")
        
    else:
        if source_choice == '2' and not AWS_AVAILABLE:
            print("AWS support not available (install boto3 and s3fs). Using local directory.")
        
        data_source = input("Enter path to rechunked data directory: ").strip()
        if not data_source:
            data_source = "./nldas_rechunked"
        aws_bucket = None
        aws_base_path = None
        year = None
    
    # Variable selection
    print(f"\n🌡️  VARIABLE SELECTION:")
    default_variable = "Rainf"
    print(f"Common variables: Rainf (precipitation), Tair (temperature), Wind (wind speed)")
    variable = input(f"Enter variable name (default: {default_variable}): ").strip()
    if not variable:
        variable = default_variable
    
    # Configuration selection
    config_selection_mode = None
    selected_configs = None
    
    print(f"\n⚙️  CONFIGURATION SELECTION:")
    print("1. Test all available configurations")
    print("2. Interactive selection (will show available configs)")
    print("3. Specify configuration names manually")
    
    config_choice = input("Select option (1, 2, or 3, default: 1): ").strip()
    
    if config_choice == '2':
        config_selection_mode = 'interactive'
    elif config_choice == '3':
        print("Example configuration names: T01_Y200_X300, T06_Y1000_X1800, T24_Y200_X300")
        config_input = input("Enter configuration names (comma-separated): ").strip()
        if config_input:
            selected_configs = [x.strip() for x in config_input.split(',')]
            config_selection_mode = 'manual'
    else:
        config_selection_mode = 'all'
    
    # Processing options
    print(f"\n🔧 PROCESSING OPTIONS:")
    use_dask = input("Use Dask for parallel processing? (y/n, default: y): ").lower() != 'n'
    
    use_gpu = False
    if GPU_AVAILABLE:
        use_gpu = input("Use GPU acceleration? (y/n, default: y): ").lower() != 'n'
    else:
        print("GPU not available - running in CPU-only mode")
    
    if use_dask:
        n_workers = input("Number of Dask workers (default: 4): ")
        try:
            n_workers = int(n_workers)
        except:
            n_workers = 4
    else:
        n_workers = 1
    
    n_runs = input("Number of runs per benchmark (default: 3): ")
    try:
        n_runs = int(n_runs)
    except:
        n_runs = 3
    
    return {
        'data_source': data_source,
        'aws_bucket': aws_bucket,
        'aws_base_path': aws_base_path,
        'year': year,
        'variable': variable,
        'config_selection_mode': config_selection_mode,
        'selected_configs': selected_configs,
        'use_dask': use_dask,
        'use_gpu': use_gpu,
        'n_workers': n_workers,
        'n_runs': n_runs
    }

def main():
    """Main execution function"""
    config = get_user_configuration()
    
    if config is None:
        print("Configuration failed. Exiting.")
        return
    
    print(f"\n🚀 Starting enhanced benchmarking:")
    print(f"   Data source: {config['data_source']}")
    if config['data_source'] == 'aws':
        print(f"   AWS bucket: {config['aws_bucket']}/{config['aws_base_path']}")
        print(f"   Year: {config['year']}")
    print(f"   Variable: {config['variable']}")
    print(f"   Workers: {config['n_workers']}")
    print(f"   GPU: {'Yes' if config['use_gpu'] and GPU_AVAILABLE else 'No'}")
    print(f"   Runs: {config['n_runs']}")
    
    # Initialize benchmarker
    benchmarker = EnhancedChunkBenchmarker(
        data_source=config['data_source'],
        use_dask=config['use_dask'],
        use_gpu=config['use_gpu'],
        n_workers=config['n_workers'],
        variable=config['variable'],
        aws_bucket=config['aws_bucket'],
        aws_base_path=config['aws_base_path'],
        year=config['year']
    )
    
    try:
        # Handle configuration selection
        selected_configs = None
        
        if config['config_selection_mode'] == 'interactive':
            # Load available configurations first
            all_configs = benchmarker.find_chunk_configurations()
            if all_configs:
                selected_configs_dict = ConfigurationSelector.interactive_config_selection(all_configs)
                # Extract the selected configuration names
                selected_configs = [ConfigurationSelector.format_config_name(cfg) for cfg in selected_configs_dict.keys()]
        elif config['config_selection_mode'] == 'manual':
            selected_configs = config['selected_configs']
        
        # Run benchmarks
        results = benchmarker.run_benchmarks(
            n_runs=config['n_runs'],
            selected_configs=selected_configs
        )
        
        if results is not None:
            # Save results
            year_suffix = f"_{config['year']}" if config['year'] else ""
            output_filename = f'enhanced_chunk_results_{config["variable"].lower()}{year_suffix}.csv'
            results.to_csv(output_filename, index=False)
            print(f"\n💾 Results saved to: {output_filename}")
            
            # Analyze results
            analysis = benchmarker.analyze_results(results)
            
        else:
            print("❌ No results generated!")
            
    finally:
        benchmarker.cleanup()

if __name__ == "__main__":
    main()