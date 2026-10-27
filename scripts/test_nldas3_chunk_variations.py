"""
Script for generating a subset of nldas3 data using various chunk layouts,
and benchmarking access across space and time.
"""
import time
import json
import random
import multiprocessing as mp
import numpy as np
import fsspec
import xarray as xr
import zarr
import dask
from pathlib import Path
from pprint import pprint

class ChunkConfig:
    def __init__(self, nlat, nlon, ntime):
        self.nlat = nlat
        self.nlon = nlon
        self.ntime = ntime

    def chunk_size_mb(self, dtype_size=4):
        return self.nlat*self.nlon*self.ntime*dtype_size/1000**2

    def chunk_layout(self, total_lats, total_lons, total_times):
        return (
            (total_lats // self.nlat) + int((total_lats % self.nlat) != 0),
            (total_lons // self.nlon) + int((total_lons % self.nlon) != 0),
            (total_times // self.ntime) + int((total_times % self.ntime) != 0),
            )

    def __str__(self):
        return "ChunkConfig(" + \
            f"nlat={self.nlat}, nlon={self.nlon}, ntime={self.ntime})"

    def __repr__(self):
        return str(self)

    def as_tuple(self):
        return (self.ntime, self.nlat, self.nlon)

def nldas3_subset_to_zarr(time_slice, lat_slice, lon_slice, times_per_load,
        acquire_var, out_zarr_path, out_chunks, out_dtype):
    """
    :@param time_slice: slice of `%Y-%m-%d` strings indicating date ranges to
        acquire for the subset
    :@param lat_slice: index slice of latitudes to acquire for subset
    :@param lon_slice: index slice of longitudes to acquire for subset
    :@param times_per_load: Number of timesteps to acquire for each call to
        xarray.Dataset.load()
    :@param acquire_var: String label of nldas3 variable to download
    :@param out_zarr_path: non-existing zarr directory store path to dump to
    :@param out_chunks: chunk configuration of output array.
    """
    ## set up a virtual file system using the kerchunk references
    ref_fs = fsspec.filesystem(
        "reference",
        fo="s3://nasa-waterinsight/virtual/nldas3_daily.parq",
        remote_protocol="s3",
        asynchronous=True,
        remote_options={"asynchronous":True, "anon":True},
        target_options={"anon":True},
        lazy=True
        )

    ## create a xarray Dataset object based on the virtual chunks.
    ds = xr.open_zarr(
            ref_fs.get_mapper(""),
            consolidated=False,
            )

    ## declare the overall subset and determine time slices
    sub = ds.sel(time=time_slice).isel(lat=lat_slice, lon=lon_slice)
    ntimes = sub.time.size
    nlats = sub.lat.size
    nlons = sub.lon.size
    nslices = ntimes // times_per_load + int(ntimes % times_per_load != 0)
    time_slices = [
        slice(int(ix0), int(min(ix0+times_per_load, ntimes)))
        for ix0 in np.arange(nslices) * times_per_load
        ]

    default_array_label = acquire_var + "-" + ".".join(map(str,out_chunks))
    default_array = dask.array.zeros(
        (ntimes, nlats, nlons),
        chunks=out_chunks,
        dtype=out_dtype,
        )
    ds = xr.Dataset(
        {default_array_label:(("time", "lat", "lon"), default_array)},
        coords={
            "time":sub.time.load().to_numpy(),
            "lat":sub.lat.load().to_numpy(),
            "lon":sub.lon.load().to_numpy(),
            },
        )
    ds.to_zarr(out_zarr_path, compute=False)

    ## load data into the file one slice at a time
    for s in time_slices:
        sub_array = sub[acquire_var].isel(time=s).load().to_numpy()
        ds_slice = xr.Dataset({
            default_array_label:(("time", "lat", "lon"), sub_array)
            })
        ds_slice.to_zarr(out_zarr_path, region={"time":s})
        print(f"Loaded {s}")

    return out_zarr_path

def mp_run_benchmark(args):
    print(f"running benchmark", args["test_type"], args["var_label"])
    return args,run_benchmark(**args)

def run_benchmark(zarr_url, test_type, var_label,
        test_kwargs:dict={}, seed=None, debug=False):
    """
    :@param zarr_url: path or remote url
    :@param test_type: determines what kind of subset to extract and time;
        may be 'pixel', 'timestep', 'chunk', or 'multichunk'
    """
    test_settings = {
        "nchunks":2, ## only applies to multichunk
        }
    test_settings.update(test_kwargs)

    assert test_type in ["pixel", "timestep", "chunk", "multichunk"]
    rng = np.random.default_rng(seed)
    xr_kwargs = [{},{"storage_options":{"anon":True}}][zarr_url[:3]=="s3:"]
    if debug:
        print(f"opening zarr at {zarr_url}")
    t0_init = time.perf_counter()
    arr = xr.open_zarr(
            zarr_url,
            consolidated=False,
            **xr_kwargs,
            )[var_label]
    tf_init = time.perf_counter()
    if debug:
        print(arr)

    if test_type=="pixel":
        ixy = rng.integers(arr.shape[1])
        ixx = rng.integers(arr.shape[2])
        if debug:
            print(f"Extracting pixel ({ixy}, {ixx})")
        t0_load = time.perf_counter()
        sub_out = arr[:,ixy,ixx].load().to_numpy()
        tf_load = time.perf_counter()
        point_count = sub_out.size

    elif test_type=="timestep":
        ixt = rng.integers(arr.shape[1])
        if debug:
            print(f"Extracting timestep {ixt}")
        t0_load = time.perf_counter()
        sub_out = arr[ixt,:,:].load().to_numpy()
        tf_load = time.perf_counter()
        point_count = sub_out.size

    elif test_type in ["chunk", "multichunk"]:
        ## list the chunk boundary indices
        cb_time = np.cumsum(np.concatenate([[0], arr.chunksizes["time"]]))
        cb_lat = np.cumsum(np.concatenate([[0], arr.chunksizes["lat"]]))
        cb_lon = np.cumsum(np.concatenate([[0], arr.chunksizes["lon"]]))

        ## get an index array of all chunks
        #cixs = np.stack(np.meshgrid(
        #    np.arange(arr.shape[0]),
        #    np.arange(arr.shape[1]),
        #    np.arange(arr.shape[2]),
        #    indexing="ij",
        #    ), axis=-1).reshape(-1,3)

        if test_type=="chunk":
            ## choose a random chunk
            #ixc = rng.integers(cixs.shape[0])
            ixc_0 = rng.integers(cb_time.size-1)
            ixc_1 = rng.integers(cb_lat.size-1)
            ixc_2 = rng.integers(cb_lon.size-1)

            cslc = [
                slice(cb_time[ixc_0], cb_time[ixc_0+1]),
                slice(cb_lat[ixc_1], cb_lat[ixc_1+1]),
                slice(cb_lon[ixc_2], cb_lon[ixc_2+1]),
                ]
            if debug:
                print(f"Extracting chunk {cslc}")
            ## record time to reference and download subset
            t0_load = time.perf_counter()
            sub_out = arr[*cslc].load().to_numpy()
            tf_load = time.perf_counter()
            point_count = sub_out.size

        if test_type=="multichunk":
            ## choose multiple random chunks w/o replacement
            #ixcs = rng.choice(cixs, size=test_settings["nchunks"],
            #    axis=0, replace=False)
            ixc_0 = rng.integers(0, cb_time.size-1, test_settings["nchunks"])
            ixc_1 = rng.integers(0, cb_lat.size-1, test_settings["nchunks"])
            ixc_2 = rng.integers(0, cb_lon.size-1, test_settings["nchunks"])
            cslcs = [(
                slice(cb_time[ixc_0[i]], cb_time[ixc_0[i]+1]),
                slice(cb_lat[ixc_1[i]], cb_lat[ixc_1[i]+1]),
                slice(cb_lon[ixc_2[i]], cb_lon[ixc_2[i]+1]),
                ) for i in range(test_settings["nchunks"])
                ]
            if debug:
                print(f"Extracting chunks: {cslcs}")
            ## record the total time and
            point_count = 0
            t0_load = time.perf_counter()
            for cslc in cslcs:
                sub_out = arr[*cslc].load().to_numpy()
                point_count += sub_out.size
            tf_load = time.perf_counter()

    return {
        "point_count":int(point_count),
        "time_start":t0_init,
        "dt_init":tf_init-t0_init,
        "dt_load":tf_load-t0_load,
        }

def collect_benchmark_result(cur_args, cur_results, json_path):
    """
    Adds new results from run_benchmark to a json file.
    Don't multiprocess over this method!!
    """
    if json_path.exists():
        results = json.load(json_path.open("r"))
    else:
        results = {}

    if cur_args["test_type"] not in results.keys():
        results[cur_args["test_type"]] = {}
    if cur_args["var_label"] not in results[cur_args["test_type"]].keys():
        results[cur_args["test_type"]][cur_args["var_label"]] = {
            "point_count":[],
            "time_start":[],
            "dt_init":[],
            "dt_load":[],
            "test_kwargs":[],
            "seed":[],
            }
    tmp_res_dict = {
        **cur_results,
        "test_kwargs":cur_args["test_kwargs"],
        "seed":cur_args["seed"],
        }
    for k,v in tmp_res_dict.items():
        results[cur_args["test_type"]][cur_args["var_label"]][k].append(v)
    print("Finished benchmark:",
        cur_args["test_type"],
        cur_args["var_label"],
        len(results[cur_args["test_type"]][cur_args["var_label"]]["seed"]),
        )

    json.dump(results, json_path.open("w"), indent=2)
    return results

if __name__=="__main__":
    out_zarr_path = Path("/rtmp/mdodson/nldas3_chunk_benchmarking.zarr")

    ## switchboard
    print_table = False
    download_new_subset = False
    load_chunk_variations = False
    run_benchmarks = True

    ## table printing settings
    full_grid_shape = (6500, 11700, 8400)
    dtype_size_bytes = 4

    ## subset zarr storage settings
    sub_time_slice=slice("2014-01-01", "2018-12-31")
    sub_lat_slice=slice(2500, 3500)
    sub_lon_slice=slice(7200, 9000)
    times_per_load=64
    sub_acquire_var="Tair"
    sub_out_chunks=(1, 500, 900)
    sub_out_dtype = np.float32

    ## benchmark settings
    run_benchmark_tests = ["pixel", "timestep", "chunk", "multichunk"]
    benchmark_iterations = 64
    random_seed = 7221750
    zarr_url = "s3://nasa-waterinsight/.test/nldas3_chunk_benchmarking.zarr"
    #zarr_url = out_zarr_path.as_posix()
    benchmark_var = "Tair"
    multi_chunk_range = (2, 13)
    #json_out_path = Path("nldas3_chunk_bench_results_local.json")
    json_out_path = Path("nldas3_chunk_bench_results.json")
    nprocs = 24
    #nprocs = 1

    ## daily
    #'''
    chunking_cands = [
        #ChunkConfig(500, 900, 1), ## disabled since default
        ChunkConfig(nlat=325, nlon=650, ntime=1),
        ChunkConfig(nlat=500, nlon=300, ntime=1),
        ChunkConfig(nlat=260, nlon=260, ntime=1),
        ChunkConfig(nlat=130, nlon=260, ntime=1),

        ChunkConfig(nlat=500, nlon=900, ntime=8),
        ChunkConfig(nlat=325, nlon=650, ntime=8),
        ChunkConfig(nlat=500, nlon=300, ntime=8),
        ChunkConfig(nlat=260, nlon=260, ntime=8),
        ChunkConfig(nlat=130, nlon=260, ntime=8),

        ChunkConfig(nlat=500, nlon=900, ntime=24),
        ChunkConfig(nlat=325, nlon=650, ntime=24),
        ChunkConfig(nlat=500, nlon=300, ntime=24),
        ChunkConfig(nlat=260, nlon=260, ntime=24),
        ChunkConfig(nlat=130, nlon=260, ntime=24),
        ]
    #'''

    ## hourly
    '''
    chunking_cands = [
        ChunkConfig(nlat=500, nlon=900, ntime=6),
        ChunkConfig(nlat=250, nlon=300, ntime=6),
        ChunkConfig(nlat=130, nlon=130, ntime=6),

        ChunkConfig(nlat=500, nlon=900, ntime=24),
        ChunkConfig(nlat=250, nlon=300, ntime=24),
        ChunkConfig(nlat=130, nlon=130, ntime=24),

        ChunkConfig(nlat=500, nlon=900, ntime=48),
        ChunkConfig(nlat=250, nlon=300, ntime=48),
        ChunkConfig(nlat=130, nlon=130, ntime=48),
        ]
    '''

    """ ------------------( END NORMAL CONFIGURATION )-----------------  """

    if print_table:
        ## print a markdown table of the chunk configuration information
        col_labels = ["lat", "lon", "time", "size/chunk (MB)",
            "N<sub>t</sub>", "N<sub>xy</sub>", "N<sub>xy</sub>/N<sub>t</sub>"]
        print(" | ".join(col_labels))
        print(" | ".join(["---" for _ in range(len(col_labels))]))
        for cc in chunking_cands:
            ncy,ncx,nct = cc.chunk_layout(*full_grid_shape)
            print(" | ".join(map(str, [
                cc.nlat, cc.nlon, cc.ntime,
                cc.chunk_size_mb(dtype_size_bytes),
                nct, ncy*ncx, f"{ncy*ncx/nct:.3f}"
                ])))

    """ initialize the zarr store and load the initial subset """

    if download_new_subset:
        assert not out_zarr_path.exists()
        nldas3_subset_to_zarr(
            time_slice=sub_time_slice,
            lat_slice=sub_lat_slice,
            lon_slice=sub_lon_slice,
            times_per_load=times_per_load,
            acquire_var=sub_acquire_var,
            out_zarr_path=out_zarr_path,
            out_chunks=sub_out_chunks,
            out_dtype=sub_out_dtype,
            )

    """ update the zarr store with variations on chunk configuration """

    if load_chunk_variations:
        ds = xr.open_zarr(out_zarr_path)
        x = ds["Tair-1.500.900"].load().to_numpy()
        for cc in chunking_cands:
            clayout = (cc.ntime, cc.nlat, cc.nlon)
            tmp_label = sub_acquire_var + "-" + ".".join(map(str,clayout))
            tmp_array = dask.array.from_array(x, chunks=clayout)
            ds = xr.Dataset({tmp_label:(("time", "lat", "lon"), tmp_array)})
            ds.to_zarr(out_zarr_path, mode="a", compute=True)
            print(f"Loaded {clayout}")

    """ run benchmarks and store results """

    if run_benchmarks:
        rng = np.random.default_rng(random_seed)
        bench_runs = [
            (cc.as_tuple(),tl)
            for cc in chunking_cands
            for tl in run_benchmark_tests
            for _ in range(benchmark_iterations)
            ]
        rng.shuffle(bench_runs)
        pprint(bench_runs)

        args = [{
            "zarr_url":zarr_url,
            "test_type":tl,
            "var_label":benchmark_var+"-"+".".join(map(str, cctup)),
            "test_kwargs":[ ## only include if multichunk test
                {},{"nchunks":int(rng.integers(*multi_chunk_range))}
                ][int(tl=="multichunk")],
            "seed":random_seed+i,
            "debug":False,
            } for i,(cctup,tl) in enumerate(bench_runs)]


        results = {}
        if nprocs>1:
            assert not json_out_path.exists(), json_out_path
            with mp.Pool(nprocs) as pool:
                for a,r in pool.imap_unordered(mp_run_benchmark, args):
                    collect_benchmark_result(
                        cur_args=a,
                        cur_results=r,
                        json_path=json_out_path
                        )
        else:
            assert not json_out_path.exists(), json_out_path
            for a,r in map(mp_run_benchmark, args):
                collect_benchmark_result(
                    cur_args=a,
                    cur_results=r,
                    json_path=json_out_path,
                    )
