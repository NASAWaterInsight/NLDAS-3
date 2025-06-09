# Enhanced NLDAS Chunk Performance Benchmarker

A comprehensive tool for benchmarking different chunk size configurations for NLDAS forcing data operations, with support for both local storage and AWS S3.

## Purpose

this tool supports:

- **Multiple data sources**: Local files or AWS S3 buckets
- **Variable selection**: Test any variable (Rainf, Tair, Wind, etc.)
- **Configuration selection**: Test all or specific chunk configurations
- **Year selection**: Analyze specific years of data
- **Performance metrics**: Execution time, memory usage, and success rates

## Prerequisites

## Installation & Dependencies

### Core Requirements (Essential)
```bash
# Essential scientific computing stack
pip install numpy pandas xarray

# System monitoring and utilities
pip install psutil

# Visualization (for potential future plotting features)
pip install matplotlib seaborn

# For accessing NASA data on AWS S3
pip install boto3 s3fs

# NetCDF file format support (critical for NLDAS data)
pip install netcdf4 h5netcdf

# For faster processing with multiple workers
pip install dask distributed

# Only if you have NVIDIA GPU and want GPU testing
# Choose based on your CUDA version:
pip install cupy-cuda11x    # For CUDA 11.x
# OR
pip install cupy-cuda12x    # For CUDA 12.x
```

## Data Structure Requirements

### AWS S3 Structure
The rechunked data is organized in AWS buckes as follow:
```
s3://nasa-waterinsight/NLDAS3/forcing/rechunked_test/<CONFIG_NAME>/year/

├── T01_Y200_X300/
│   ├── 2022/
│   │   ├── file1.nc
│   │   ├── file2.nc
│   │   └── ...
│   ├── 2023/
│   └── 2024/
├── T06_Y1000_X1800/
│   ├── 2022/
│   ├── 2023/
│   └── 2024/
└── ...
```

### Local Directory Structure
For local files:
```
your-data-directory/
├── rechunked_1_200_300.nc
├── rechunked_6_1000_1800.nc
├── rechunked_12_1000_1800.nc
└── ...
```

### Configuration Naming Convention
- **AWS**: `T{time}_Y{lat}_X{lon}` (e.g., `T06_Y1000_X1800`)
- **Local**: `rechunked_{time}_{lat}_{lon}_*.nc`

Where:
- `time`: Time dimension chunk size
- `lat`: Latitude dimension chunk size (Y-axis)
- `lon`: Longitude dimension chunk size (X-axis)

## Quick Start

### Basic Usage
```bash
python enhanced_chunk_benchmarker.py
```

The tool will guide you through an interactive setup process.

### Example: NASA WaterInsight S3 Bucket
```bash
python enhanced_chunk_benchmarker.py

# Follow prompts:
# 1. Select AWS S3 as data source
# 2. Use default bucket: nasa-waterinsight  
# 3. Use default path: NLDAS3/forcing/rechunked_test
# 4. Select year: 2023
# 5. Choose variable: Rainf (or your preferred variable)
# 6. Select configurations: all or specific ones
```

## Interactive Setup Guide

When you run the tool, you'll be prompted for the following configurations:

### 1. Data Source Selection
```
DATA SOURCE SELECTION:
1. Local directory
2. AWS S3 bucket
Select data source (1 or 2, default: 1):
```

**Choose Option 1** for local files, **Option 2** for AWS S3.

### 2. AWS Configuration (if selected)
```
Enter AWS S3 bucket name (default: nasa-waterinsight): 
Enter base path in bucket (default: NLDAS3/forcing/rechunked_test):
```

The tool will automatically scan for available years:
```
YEAR SELECTION:
Available years: 2022, 2023, 2024
Enter year to analyze (available: 2022, 2023, 2024): 2023
```

### 3. Variable Selection
```
VARIABLE SELECTION:
Common variables: Rainf (precipitation), Tair (temperature), Wind (wind speed)
Enter variable name (default: Rainf): 
```

**Common NLDAS variables:**
- `Rainf`: Precipitation rate
- `Tair`: Air temperature
- `Wind`: Wind speed
- `SWdown`: Downward shortwave radiation
- `LWdown`: Downward longwave radiation
- `Psurf`: Surface pressure
- `Qair`: Specific humidity

### 4. Configuration Selection
```
⚙️ CONFIGURATION SELECTION:
1. Test all available configurations
2. Interactive selection (will show available configs)
3. Specify configuration names manually
```

**Option 1**: Tests all available chunk configurations
**Option 2**: Shows a list and lets you pick specific ones
**Option 3**: Enter specific names like `T06_Y1000_X1800,T24_Y200_X300`

### 5. Processing Options
```
🔧 PROCESSING OPTIONS:
Use Dask for parallel processing? (y/n, default: y): y
Use GPU acceleration? (y/n, default: y): n
Number of Dask workers (default: 4): 4
Number of runs per benchmark (default: 3): 3
```

## Operations Tested

The benchmarker tests these essential operations:

1. **Monthly Time Series - Single Point**: Extract time series for a weather station location
2. **Monthly Climatology**: Calculate spatial average across all grid points
3. **Multi-Point Time Series**: Extract data for multiple locations simultaneously
4. **GPU Monthly Average** (if GPU available): Test GPU acceleration efficiency

## Output and Results

### Console Output
The tool provides real-time progress updates:
```
[1/12] Monthly Time Series - Single Point (Rainf)
    Running: Monthly Time Series - Single Point (Rainf)
      ✅ Run 1: 2.456s
      ✅ Run 2: 2.398s  
      ✅ Run 3: 2.445s
      ✅ Time: 2.433 ± 0.030s
      💾 Memory: 145.2 MB
      📊 Success Rate: 100%
```

### CSV Results File
Results are saved to: `enhanced_chunk_results_{variable}_{year}.csv`

**Columns include:**
- `operation`: Name of the tested operation
- `chunk_config`: Configuration name (e.g., T06_Y1000_X1800)
- `avg_time`: Average execution time (seconds)
- `std_time`: Standard deviation of execution times
- `avg_memory`: Average memory usage (MB)
- `success_rate`: Percentage of successful runs
- `chunk_time`, `chunk_lat`, `chunk_lon`: Individual chunk dimensions

### Analysis Summary
The tool provides recommendations:
```
FINAL RECOMMENDATIONS:
BEST FOR CRITICAL ANALYSIS: T06_Y1000_X1800
    (Optimized for critical Rainf operations)
⚡ BEST OVERALL PERFORMANCE: T06_Y1000_X1800  
    (Best average across all Rainf operations)
```

The tool is designed to be robust and provide helpful error messages to guide you through any issues.