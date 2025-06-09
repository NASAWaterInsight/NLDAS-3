#!/usr/bin/env python3
# NLDAS NetCDF Rechunker with Integrity Checking - Command Line Version
# Usage: python nldas_rechunker.py --month 1 --chunk-config 24,50,90

import os
import glob
import xarray as xr
import netCDF4
import argparse
import sys

def quick_file_check(file_path):
    """
    Quick check to see if a NetCDF file is complete and readable
    Returns True if file is OK, False if corrupted/incomplete
    """
    try:
        # Check if file exists and has reasonable size
        if not os.path.exists(file_path):
            return False
            
        # Check file size (should be at least 1MB for NLDAS data)
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if size_mb < 1:
            print(f"    Warning: File size ({size_mb:.2f} MB) seems too small")
            return False
        
        # Try to open and read basic info with xarray
        with xr.open_dataset(file_path) as ds:
            # Check if it has data variables
            if len(ds.data_vars) == 0:
                print(f"    Warning: No data variables found")
                return False
            
            # Try to access the shape of first variable (this forces reading metadata)
            first_var = list(ds.data_vars.keys())[0]
            shape = ds[first_var].shape
            
            # Check if dimensions make sense for NLDAS data
            if len(shape) != 3:
                print(f"    Warning: Variable has {len(shape)} dimensions, expected 3")
                return False
                
        return True
        
    except Exception as e:
        print(f"    Error during file check: {str(e)}")
        return False

def rechunk_file(input_file, chunk_size, output_dir=None):
    """
    Rechunk a NetCDF file to the specified chunk size and save it
    Returns the path to the rechunked file
    """
    print(f"Rechunking file to {chunk_size}...")
    
    # Get filename
    base_name = os.path.basename(input_file)
    
    # Format chunk size for filename
    chunk_str = f"{chunk_size[0]}_{chunk_size[1]}_{chunk_size[2]}"
    
    # Set output directory to current directory if not specified
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "nldas_rechunked")
        os.makedirs(output_dir, exist_ok=True)
    
    # Create output path
    output_file = os.path.join(output_dir, f"rechunked_{chunk_str}_{base_name}")
    
    # Check if file already exists and is valid
    if os.path.exists(output_file):
        print(f"Rechunked file already exists: {output_file}")
        print("  Checking file integrity...")
        
        if quick_file_check(output_file):
            print("  ✓ File is valid, skipping rechunking")
            return output_file
        else:
            print("  ✗ File appears corrupted, will regenerate")
            # Remove corrupted file
            try:
                os.remove(output_file)
                print("  Removed corrupted file")
            except Exception as e:
                print(f"  Warning: Could not remove corrupted file: {e}")
    
    # Open the input file
    print(f"Opening {input_file}...")
    ds = xr.open_dataset(input_file)
    
    # Rechunk the dataset
    print(f"Rechunking to chunks: time={chunk_size[0]}, lat={chunk_size[1]}, lon={chunk_size[2]}")
    rechunked_ds = ds.chunk({'time': chunk_size[0], 'lat': chunk_size[1], 'lon': chunk_size[2]})
    
    # Set up encoding with specified chunks and compression
    encoding = {}
    for var in ds.data_vars:
        # Get variable dimensions
        dims = ds[var].dims
        
        # Skip variables that don't have the expected dimensions
        if set(dims) != set(['time', 'lat', 'lon']):
            encoding[var] = {'zlib': True, 'complevel': 5}
            continue
        
        # Map dimensions to chunk sizes
        var_chunks = []
        for dim in dims:
            if dim == 'time':
                var_chunks.append(chunk_size[0])
            elif dim == 'lat':
                var_chunks.append(chunk_size[1])
            elif dim == 'lon':
                var_chunks.append(chunk_size[2])
        
        # Set encoding
        encoding[var] = {
            'chunksizes': tuple(var_chunks),
            'zlib': True,
            'complevel': 5
        }
    
    # Save the rechunked file
    print(f"Writing to {output_file}...")
    try:
        rechunked_ds.to_netcdf(output_file, encoding=encoding, engine='netcdf4')
        
        # Close datasets
        ds.close()
        rechunked_ds.close()
        
        # Verify the written file
        print("  Verifying written file...")
        if quick_file_check(output_file):
            print(f"✓ Successfully created and verified {output_file}")
            return output_file
        else:
            print(f"✗ File was created but failed verification: {output_file}")
            # Try to remove the bad file
            try:
                os.remove(output_file)
            except:
                pass
            return None
            
    except Exception as e:
        print(f"✗ Error writing file: {str(e)}")
        # Close datasets in case of error
        try:
            ds.close()
            rechunked_ds.close()
        except:
            pass
        return None

def rechunk_multiple_configs(input_file, chunk_configs, output_dir=None):
    """
    Rechunk a single file to multiple chunk configurations
    """
    print(f"\n=== NLDAS NetCDF Rechunker with Integrity Checking ===\n")
    
    # Create output directory
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "nldas_rechunked")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Check existing files for this specific input file
    valid_files, missing_configs = check_existing_files_for_input(input_file, output_dir, chunk_configs)
    
    # Only process missing configurations
    if not missing_configs:
        print(f"\n✓ All chunk configurations already exist and are valid!")
        print(f"Found {len(valid_files)} existing valid files.")
        return valid_files, []
    
    print(f"\nNeed to create {len(missing_configs)} missing configurations:")
    for config in missing_configs:
        print(f"  - {config}")
    
    # List to store successfully created files
    created_files = list(valid_files)  # Start with existing valid files
    failed_configs = []
    
    # Process only missing chunk configurations
    for i, chunk_size in enumerate(missing_configs):
        print(f"\n[{i+1}/{len(missing_configs)}] Processing chunk size: {chunk_size}")
        
        try:
            output_file = rechunk_file(input_file, chunk_size, output_dir)
            
            if output_file:
                created_files.append(output_file)
                print(f"✓ Success: {os.path.basename(output_file)}")
            else:
                failed_configs.append(chunk_size)
                print(f"✗ Failed: chunk size {chunk_size}")
                
        except Exception as e:
            failed_configs.append(chunk_size)
            print(f"✗ Error with chunk size {chunk_size}: {str(e)}")
    
    # Final summary
    print(f"\n=== Rechunking Complete ===")
    print(f"Total valid files: {len(created_files)} (including {len(valid_files)} pre-existing)")
    print(f"Newly created: {len(created_files) - len(valid_files)}")
    print(f"Failed: {len(failed_configs)}")
    
    if len(created_files) - len(valid_files) > 0:
        print(f"\nNewly created files:")
        new_files = created_files[len(valid_files):]
        for file_path in new_files:
            print(f"  ✓ {os.path.basename(file_path)}")
    
    if failed_configs:
        print(f"\nFailed configurations:")
        for config in failed_configs:
            print(f"  ✗ {config}")
        print(f"\nYou can retry these configurations by running the script again.")
    
    return created_files, failed_configs

def check_existing_files_for_input(input_file, output_dir, chunk_configs):
    """
    Check existing rechunked files only for the specific input file
    """
    if not os.path.exists(output_dir):
        return [], chunk_configs
    
    # Get the base name of input file for matching
    input_base_name = os.path.basename(input_file)
    
    # Build list of expected output files for this input
    expected_files = []
    for chunk_size in chunk_configs:
        chunk_str = f"{chunk_size[0]}_{chunk_size[1]}_{chunk_size[2]}"
        expected_filename = f"rechunked_{chunk_str}_{input_base_name}"
        expected_path = os.path.join(output_dir, expected_filename)
        expected_files.append((expected_path, chunk_size))
    
    print(f"\nChecking existing files for {input_base_name}...")
    
    valid_files = []
    invalid_files = []
    missing_configs = []
    
    for file_path, chunk_size in expected_files:
        if os.path.exists(file_path):
            print(f"Found: {os.path.basename(file_path)}")
            if quick_file_check(file_path):
                valid_files.append(file_path)
                print("  ✓ Valid")
            else:
                invalid_files.append((file_path, chunk_size))
                print("  ✗ Invalid/Corrupted")
        else:
            missing_configs.append(chunk_size)
    
    print(f"\nStatus for {input_base_name}:")
    print(f"  Valid files: {len(valid_files)}")
    print(f"  Invalid files: {len(invalid_files)}")
    print(f"  Missing configs: {len(missing_configs)}")
    
    if invalid_files:
        print(f"\nInvalid files found:")
        for file_path, chunk_size in invalid_files:
            print(f"  ✗ {os.path.basename(file_path)} (config: {chunk_size})")
        
        print(f"\nRemoving {len(invalid_files)} corrupted files automatically...")
        for file_path, chunk_size in invalid_files:
            try:
                os.remove(file_path)
                print(f"  Removed: {os.path.basename(file_path)}")
                missing_configs.append(chunk_size)  # Add to missing so it gets recreated
            except Exception as e:
                print(f"  Failed to remove {os.path.basename(file_path)}: {e}")
    
    return valid_files, missing_configs

def get_data_directory():
    """
    Get the data directory path for January 2023
    """
    base_dir = "/home/mghaziza/JupyterLinks/projects/eis_nldas3/DATA/forcing/hourly"
    return os.path.join(base_dir, "202301")

def find_files_for_day(day):
    """
    Find NLDAS files for the specified day in January 2023
    """
    data_dir = get_data_directory()
    pattern = f"NLDAS_FOR0010_H.A202301{day:02d}*.nc"
    search_pattern = os.path.join(data_dir, pattern)
    
    if not os.path.exists(data_dir):
        print(f"Error: Directory {data_dir} not found")
        return []
    
    files = sorted(glob.glob(search_pattern))
    return files

def parse_chunk_config(chunk_str):
    """
    Parse chunk configuration from command line string
    Example: "24,50,90" -> (24, 50, 90)
    """
    try:
        parts = chunk_str.split(',')
        if len(parts) != 3:
            raise ValueError("Chunk configuration must have exactly 3 values")
        
        return tuple(int(x.strip()) for x in parts)
    except ValueError as e:
        print(f"Error parsing chunk configuration '{chunk_str}': {e}")
        print("Format should be: time,lat,lon (e.g., 24,50,90)")
        sys.exit(1)

def main():
    """
    Main function with command line argument parsing
    """
    parser = argparse.ArgumentParser(
        description='NLDAS NetCDF Rechunker with Integrity Checking',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nldas_rechunker.py --day 1
  python nldas_rechunker.py -d 15 --output ./output
  python nldas_rechunker.py -d 31 --file-index 5
        """
    )
    
    parser.add_argument('-d', '--day', type=int, required=True, 
                        help='Day to process (1-31) for January 2023', choices=range(1, 32))
    
    parser.add_argument('-o', '--output', default=None,
                        help='Output directory (default: ./nldas_rechunked)')
    
    parser.add_argument('--file-index', type=int, default=0,
                        help='Index of file to process (0-based, default: 0 for first file)')
    
    args = parser.parse_args()
    
    # Define chunk configurations here (hardcoded)
    chunk_configs = [
        (24, 50, 90),
        (24,100,180),
        (24,500,900),
        (24, 1000, 1800),
        (6,100,180),
        (6,200,360),
        (6,1000,1800),
        (6,2000,3600),
        (1,250,450),
        (1,500,900),
        (1,2500,4500),
        (1,5000,9000)
    ]

    
    print("NLDAS NetCDF Rechunker with Integrity Checking")
    print("===============================================")
    print(f"Processing January {args.day}, 2023")
    print(f"Output directory: {args.output or './nldas_rechunked'}")
    print(f"Chunk configurations:")
    for i, config in enumerate(chunk_configs):
        print(f"  {i+1}. Time: {config[0]}, Lat: {config[1]}, Lon: {config[2]}")
    
    # Find files for the specified day
    files = find_files_for_day(args.day)
    
    if not files:
        print(f"No files found for January {args.day}, 2023")
        print(f"Looking for pattern: NLDAS_FOR0010_H.A202301{args.day:02d}*.nc")
        sys.exit(1)
    
    print(f"\nFound {len(files)} files for January {args.day}, 2023")
    for i, file in enumerate(files):
        print(f"  {i}: {os.path.basename(file)}")
    
    # Select file to process
    if args.file_index >= len(files):
        print(f"File index {args.file_index} is out of range (0-{len(files)-1})")
        print("Using first file (index 0)")
        file_index = 0
    else:
        file_index = args.file_index
    
    selected_file = files[file_index]
    print(f"\nProcessing file {file_index + 1}/{len(files)}: {os.path.basename(selected_file)}")
    
    # Run rechunking
    created_files, failed_configs = rechunk_multiple_configs(
        selected_file, chunk_configs, args.output
    )
    
    print(f"\n=== Final Summary ===")
    print(f"Successfully processed: {len(created_files)} files")
    print(f"Failed configurations: {len(failed_configs)}")
    
    if failed_configs:
        print(f"\nFailed configurations:")
        for config in failed_configs:
            print(f"  ✗ {config}")
        print(f"\nYou can retry by running the script again with the same parameters.")
        sys.exit(1)
    else:
        print(f"\n✓ All configurations completed successfully!")

if __name__ == "__main__":
    main()