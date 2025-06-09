#!/usr/bin/env python3
"""
Clean Focused NLDAS Chunk Performance Benchmarker - NO GPU VERSION
Only the most essential operations for chunk size optimization
Fixed to work properly without GPU/CUDA
REMOVED: Full Month Load operation (memory intensive)
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
warnings.filterwarnings('ignore')

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

class FocusedChunkBenchmarker:
    def __init__(self, data_directory, use_dask=True, use_gpu=False, n_workers=4):
        self.data_dir = Path(data_directory)
        self.use_dask = use_dask and DASK_AVAILABLE
        self.use_gpu = use_gpu and GPU_AVAILABLE
        self.n_workers = n_workers
        self.client = None
        
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
    
    def find_chunk_configurations(self):
        """Find all available chunk configurations and ensure fair comparison"""
        configs = {}
        
        # Look for rechunked files
        pattern = "rechunked_*_*.nc"
        files = list(self.data_dir.glob(pattern))
        
        for file_path in files:
            # Extract chunk config from filename: rechunked_{time}_{lat}_{lon}_original.nc
            parts = file_path.stem.split('_')
            if len(parts) >= 4:
                try:
                    chunk_config = (int(parts[1]), int(parts[2]), int(parts[3]))
                    if chunk_config not in configs:
                        configs[chunk_config] = []
                    configs[chunk_config].append(file_path)
                except ValueError:
                    continue
        
        # Sort files by date for each configuration
        for config in configs:
            configs[config] = sorted(configs[config])
        
        print(f"Found {len(configs)} chunk configurations:")
        for config, files in configs.items():
            print(f"  {config}: {len(files)} files")
        
        # FAIR COMPARISON: Use same number of files for all configurations
        # Find the minimum number of files across all configurations
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
    
    def load_multi_file_dataset(self, file_list, variable='Rainf'):
        """Load multiple files as a single dataset - ONLY Rainf variable"""
        if self.use_dask:
            # Use xarray with dask for lazy loading - ONLY load Rainf
            ds = xr.open_mfdataset(
                file_list, 
                combine='nested',
                concat_dim='time',
                chunks={'time': -1}  # Let existing chunks determine chunking
            )
        else:
            # Load sequentially - ONLY load Rainf
            datasets = [xr.open_dataset(f) for f in file_list]
            ds = xr.concat(datasets, dim='time')
        
        # Always return ONLY the Rainf variable to avoid loading other variables
        if variable in ds:
            return ds[variable]
        else:
            # Fallback: try to find any precipitation variable
            possible_names = ['Rainf', 'RAINF', 'precipitation', 'precip', 'rain']
            for name in possible_names:
                if name in ds:
                    print(f"    Using variable '{name}' instead of '{variable}'")
                    return ds[name]
            
            # If no precipitation variable found, use first available
            first_var = list(ds.data_vars)[0]
            print(f"    Warning: '{variable}' not found, using '{first_var}'")
            return ds[first_var]
    
    def _get_penalty_time(self, error_type, elapsed_time):
        """Assign penalty times based on error type"""
        penalties = {
            'MEMORY_ERROR': 9999.0,     # Worst penalty - chunk size is too large
            'TIMEOUT': 9998.0,          # Second worst - too slow
            'IO_ERROR': 9997.0,         # Third worst - I/O problems
            'RUNTIME_ERROR': 9996.0,    # Runtime issues
            'UNKNOWN_ERROR': 9995.0     # Unknown problems
        }
        
        # Use penalty time, but if operation ran for a while, use actual time + penalty
        penalty = penalties.get(error_type, 9999.0)
        
        # If the operation ran for some time before failing, add that time
        if elapsed_time > 1.0:  # If it ran for more than 1 second
            return min(penalty, elapsed_time + 1000)  # Add penalty but cap it
        else:
            return penalty

    def benchmark_operation(self, operation_name, operation_func, file_list, **kwargs):
        """Benchmark a single operation with robust error handling and early termination"""
        print(f"    Running: {operation_name}")
        
        times = []
        memory_usage = []
        gpu_memory_usage = []
        error_types = []
        
        n_runs = kwargs.get('n_runs', 3)
        timeout_seconds = kwargs.get('timeout', 300)  # 5 minute timeout per operation
        
        # Early termination flags
        consecutive_io_errors = 0
        max_consecutive_io_errors = 2  # Skip after 2 consecutive I/O errors
        
        for run in range(n_runs):
            # Clean up memory before each run
            gc.collect()
            if self.use_gpu and GPU_AVAILABLE:
                try:
                    cp.get_default_memory_pool().free_all_blocks()
                except:
                    pass  # Ignore cleanup errors
            
            # Record initial state
            mem_before = get_memory_usage()
            gpu_mem_before = get_gpu_memory() if self.use_gpu else 0
            
            # Run operation with comprehensive error handling
            start_time = time.time()
            success = False
            error_type = None
            result = None
            
            try:
                # Set up timeout for operation
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")
                
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout_seconds)
                
                try:
                    result = operation_func(file_list, **kwargs)
                    
                    # Force computation if it's a dask array
                    if hasattr(result, 'compute'):
                        result = result.compute()
                    elif hasattr(result, 'values'):
                        result = result.values
                    
                    # Simple operation to ensure data is loaded
                    if hasattr(result, 'sum'):
                        _ = result.sum()
                    elif isinstance(result, np.ndarray):
                        _ = np.sum(result)
                    
                    success = True
                    consecutive_io_errors = 0  # Reset counter on success
                    
                finally:
                    signal.alarm(0)  # Cancel timeout
                    
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
                    
                    # EARLY TERMINATION: If we get multiple I/O errors, this chunk config is problematic
                    if consecutive_io_errors >= max_consecutive_io_errors:
                        print(f"      🚫 EARLY TERMINATION: {consecutive_io_errors} consecutive I/O errors detected")
                        print(f"      🚫 This chunk configuration appears to be fundamentally problematic")
                        print(f"      🚫 Skipping remaining runs for this operation...")
                        
                        # Fill remaining runs with penalty scores
                        penalty_time = self._get_penalty_time(error_type, time.time() - start_time)
                        for remaining_run in range(run, n_runs):
                            times.append(penalty_time)
                            memory_usage.append(max(0, get_memory_usage() - mem_before))
                            gpu_memory_usage.append(0)
                            error_types.append("IO_ERROR_EARLY_TERMINATION")
                        
                        # Return early termination result
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
            
            # Record final state
            mem_after = get_memory_usage()
            gpu_mem_after = get_gpu_memory() if self.use_gpu else 0
            
            if success:
                times.append(elapsed_time)
                memory_usage.append(mem_after - mem_before)
                gpu_memory_usage.append(gpu_mem_after - gpu_mem_before)
                print(f"      ✅ Run {run+1}: {elapsed_time:.2f}s")
            else:
                # Assign penalty scores for failed operations
                penalty_time = self._get_penalty_time(error_type, elapsed_time)
                times.append(penalty_time)
                memory_usage.append(max(0, mem_after - mem_before))
                gpu_memory_usage.append(max(0, gpu_mem_after - gpu_mem_before))
                error_types.append(error_type)
                print(f"      ❌ Run {run+1}: Failed ({error_type}) - penalty: {penalty_time:.2f}s")
            
            # Aggressive cleanup after each run
            try:
                if result is not None:
                    del result
                gc.collect()
                
                # Force close any lingering dask workers if there were errors
                if not success and self.client:
                    try:
                        self.client.restart()
                    except:
                        pass
                        
            except Exception:
                pass  # Ignore cleanup errors
        
        # Calculate results with penalty handling
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
        """Define only the most essential operations for chunk size optimization - RAINF ONLY"""
        operations = []
        
        # ESSENTIAL OPERATION 1: Monthly Time Series (Most Important for Climate)
        operations.append({
            'name': 'Monthly Time Series - Single Point (Rainf)',
            'description': 'Extract Rainf time series for weather station (CRITICAL)',
            'category': 'time_series',
            'func': self.op_monthly_timeseries_point,
            'priority': 'critical'
        })
        
        # ESSENTIAL OPERATION 2: Monthly Climatology (Critical for Climate Analysis)  
        operations.append({
            'name': 'Monthly Climatology (Rainf)',
            'description': 'Calculate monthly mean Rainf climatology (CRITICAL)', 
            'category': 'spatial',
            'func': self.op_monthly_climatology,
            'priority': 'critical'
        })
        
        # REMOVED: Full Month Load operation (was too memory intensive)
        
        # ESSENTIAL OPERATION 3: Multi-Point Time Series (Common Analysis Pattern)
        operations.append({
            'name': 'Multi-Point Time Series (Rainf)',
            'description': 'Multiple weather stations Rainf - tests parallel I/O',
            'category': 'time_series',
            'func': self.op_multi_point_timeseries,
            'priority': 'high'
        })
        
        # REMOVED: Streaming Statistics operation
        
        # ESSENTIAL OPERATION 5: GPU Monthly Average (Only if GPU is actually available and working)
        if self.use_gpu and GPU_AVAILABLE:
            operations.append({
                'name': 'GPU Monthly Average (Rainf)',
                'description': 'GPU Rainf acceleration test - memory transfer efficiency',
                'category': 'gpu',
                'func': self.op_gpu_monthly_average,
                'priority': 'high'
            })
        
        print(f"Defined {len(operations)} essential operations (RAINF-focused)")
        print("  (REMOVED: Full Month Load & Streaming Statistics operations)")
        if self.use_gpu and GPU_AVAILABLE:
            print("  (Including GPU operations)")
        else:
            print("  (CPU-only operations)")
        
        return operations
    
    # Essential operation implementations - ALL FOCUSED ON RAINF ONLY
    def op_monthly_timeseries_point(self, file_list, **kwargs):
        """Extract Rainf time series for a single point across all files - MOST CRITICAL"""
        data = self.load_multi_file_dataset(file_list, variable='Rainf')
        # Central US coordinates  
        lat_idx = data.shape[1] // 2
        lon_idx = data.shape[2] // 2
        return data[:, lat_idx, lon_idx]
    
    def op_monthly_climatology(self, file_list, **kwargs):
        """Calculate monthly Rainf climatology - CRITICAL FOR CLIMATE ANALYSIS"""
        data = self.load_multi_file_dataset(file_list, variable='Rainf')
        return data.mean(dim='time')
    
    # REMOVED: op_full_month_load function (was memory intensive)
    
    def op_multi_point_timeseries(self, file_list, **kwargs):
        """Extract Rainf time series for multiple points - TESTS PARALLEL I/O"""
        data = self.load_multi_file_dataset(file_list, variable='Rainf')
        # Sample 5 points across North America (reduced from 10)
        n_points = 5
        lat_indices = np.linspace(0, data.shape[1]-1, n_points, dtype=int)
        lon_indices = np.linspace(0, data.shape[2]-1, n_points, dtype=int)
        
        results = []
        for lat_idx, lon_idx in zip(lat_indices, lon_indices):
            results.append(data[:, lat_idx, lon_idx])
        
        return xr.concat(results, dim='station')
    
    # REMOVED: op_streaming_statistics function
    
    def op_gpu_monthly_average(self, file_list, **kwargs):
        """Calculate monthly average using GPU - TESTS GPU MEMORY TRANSFER (Rainf only)"""
        if not (self.use_gpu and GPU_AVAILABLE):
            raise RuntimeError("GPU not available or not enabled")
        
        # Load ONLY Rainf data and transfer to GPU
        data = self.load_multi_file_dataset(file_list, variable='Rainf')
        gpu_data = cp.asarray(data.values)
        
        # Calculate monthly average on GPU
        monthly_avg = cp.mean(gpu_data, axis=0)
        
        # Transfer back to CPU
        return cp.asnumpy(monthly_avg)
    
    def run_benchmarks(self, n_runs=3):
        """Run focused benchmarks on essential operations only"""
        print("="*60)
        print("FOCUSED NLDAS CHUNK BENCHMARKING - RAINF ONLY")
        print("="*60)
        
        # Find configurations
        configs = self.find_chunk_configurations()
        if not configs:
            print("No chunk configurations found!")
            return None
        
        # Define operations
        operations = self.define_essential_operations()
        
        print(f"\nRunning {len(operations)} ESSENTIAL operations on {len(configs)} configurations")
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
        skipped_configs = set()  # Track configs that should be skipped entirely
        
        for config, file_list in configs.items():
            print(f"\n--- Testing Configuration: {config} ---")
            print(f"Files: {len(file_list)}")
            
            # Skip this config if it's been marked as problematic
            if config in skipped_configs:
                print(f"🚫 SKIPPING configuration {config} - marked as problematic from previous operations")
                # Add failed results for all remaining operations
                for operation in operations:
                    current_test += 1
                    results.append({
                        'operation': operation['name'],
                        'category': operation['category'],
                        'priority': operation['priority'],
                        'chunk_config': str(config),
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
                
                # Run benchmark
                result = self.benchmark_operation(
                    operation['name'],
                    operation['func'],
                    file_list,
                    n_runs=n_runs
                )
                
                # Check if this operation had early termination due to I/O errors
                if result.get('early_termination', False):
                    config_failed_operations += 1
                    print(f"      🚫 Early termination: {result.get('termination_reason', 'Unknown')}")
                    
                    # If multiple operations fail on this config, mark it as problematic
                    if config_failed_operations >= 1:  # Skip after just 1 failed operation
                        print(f"      🚫 MARKING CONFIGURATION {config} AS PROBLEMATIC")
                        print(f"      🚫 Will skip remaining operations for this configuration")
                        skipped_configs.add(config)
                
                # Store result
                results.append({
                    'operation': operation['name'],
                    'category': operation['category'],
                    'priority': operation['priority'],
                    'chunk_config': str(config),
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
                    print(f"      🚫 Skipping remaining operations for configuration {config}")
                    # Add failed results for remaining operations
                    remaining_operations = operations[operations.index(operation) + 1:]
                    for remaining_op in remaining_operations:
                        current_test += 1
                        results.append({
                            'operation': remaining_op['name'],
                            'category': remaining_op['category'],
                            'priority': remaining_op['priority'],
                            'chunk_config': str(config),
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
            print(f"🏆 BEST FOR CLIMATE ANALYSIS: {best_critical_config}")
            print(f"    (Optimized for critical climate operations)")
        
        best_overall = overall_avg.index[0]
        print(f"⚡ BEST OVERALL PERFORMANCE: {best_overall}")
        print(f"    (Best average across all operations)")
        
        # Identify problematic configurations
        if not failed.empty:
            print(f"\n⚖️  CONFIGURATIONS TO AVOID:")
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

def main():
    """Main execution function for focused benchmarking"""
    print("Clean Focused NLDAS Chunk Performance Benchmarker - RAINF ONLY")
    print("Essential operations focused on precipitation data")
    print("REMOVED: Full Month Load & Streaming Statistics operations")
    print("="*50)
    
    # Get configuration
    data_dir = input("Enter path to rechunked data directory: ").strip()
    if not data_dir:
        data_dir = "./nldas_rechunked"
    
    if not os.path.exists(data_dir):
        print(f"Directory not found: {data_dir}")
        return
    
    # Configuration options
    use_dask = input("Use Dask for parallel processing? (y/n, default: y): ").lower() != 'n'
    
    # Only ask about GPU if it's actually available
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
    
    print(f"\n🚀 Starting focused benchmarking:")
    print(f"   Workers: {n_workers}")
    print(f"   GPU: {'Yes' if use_gpu and GPU_AVAILABLE else 'No'}")
    print(f"   Runs: {n_runs}")
    print(f"   Expected time: ~15-30 minutes (reduced without Full Month Load & Streaming Statistics)")
    
    # Run benchmarks
    benchmarker = FocusedChunkBenchmarker(data_dir, use_dask, use_gpu, n_workers)
    
    try:
        # Run benchmarks
        results = benchmarker.run_benchmarks(n_runs=n_runs)
        
        if results is not None:
            # Save results
            results.to_csv('focused_chunk_results.csv', index=False)
            print(f"\n💾 Results saved to: focused_chunk_results.csv")
            
            # Analyze results
            analysis = benchmarker.analyze_results(results)
            
        else:
            print("❌ No results generated!")
            
    finally:
        benchmarker.cleanup()

if __name__ == "__main__":
    main()