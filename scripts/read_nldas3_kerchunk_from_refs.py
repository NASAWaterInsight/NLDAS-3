"""
This script demonstrates how to read NLDAS-3 hourly data from kerchunk-style
refs stored here: s3://nasa-waterinsight/virtual/nldas3_hourly.parq

This approach enables you to load the metadata needed to declare an xarray
Dataset that *looks* like it contains the entire period of record, but which
downloads the data itself lazily only once you have defined a subset and
called the `.load()` method.
"""
import fsspec
import xarray as xr
import numpy as np
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def animate_geo_raster(data, lats, lons, times=None, plot_spec={}):
    """
    Creates a matplotlib animation from a 3D numpy array.

    :@param data: 3d array shaped (time, lat, lon)
    :@param lats: 1d array of equirectangular latitude values (lat,)
    :@param lats: 1d array of equirectangular longitude values (lon,)
    :@param times: Optional list of time strings to apply to the title
        if a "{time}" format string is included
    """
    ps = {
        "origin":"lower", "cmap":"magma", "aspect":"auto",
        "cb_label":"Value","x_label":"Latitude", "y_label":"Longitude",
        "title":"", "frame_interval":200,
        "projection": ccrs.PlateCarree(), "transform": ccrs.PlateCarree(),
        "draw_borders": True, "draw_states": True,
        "border_edgecolor":"white", "border_facecolor":"none",
        "border_linewidth":2, "vmin":None, "vmax":None,
        }
    ps.update(plot_spec)

    fig, ax = plt.subplots(subplot_kw={'projection': ps.get("projection")})

    # Calculate spatial extent [left, right, bottom, top]
    extent = [lons.min(), lons.max(), lats.min(), lats.max()]

    # Add cartopy state and national borders
    if ps.get("draw_borders"):
        ax.add_feature(
            cfeature.BORDERS,
            edgecolor=ps.get("border_edgecolor"),
            facecolor=ps.get("border_facecolor"),
            linewidth=ps.get("border_linewidth"),
            )
    if ps.get("draw_states"):
        ax.add_feature(
            cfeature.STATES,
            edgecolor=ps.get("border_edgecolor"),
            facecolor=ps.get("border_facecolor"),
            linewidth=ps.get("border_linewidth"),
            )

    # Initialize the plot with the first time step
    im = ax.imshow(
            data[0],
            extent=extent,
            origin=ps.get("origin"),
            cmap=ps.get("cmap"),
            aspect=ps.get("aspect"),
            transform=ps.get("transform"),
            vmin=ps.get("vmin"),
            vmax=ps.get("vmax"),
            )

    fig.colorbar(im, ax=ax, label=ps.get("cb_label"))
    ax.set_xlabel(ps.get("x_label"))
    ax.set_ylabel(ps.get("y_label"))

    def update(frame):
        im.set_data(data[frame])
        tmp_title = ps.get("title")
        if not times is None:
            tmp_title = tmp_title.format(time=times[frame])
        ax.set_title(tmp_title)
        return [im]

    # Create the animation
    ani = animation.FuncAnimation(
        fig, update, frames=data.shape[0],
        interval=ps.get("frame_interval"), blit=True
        )

    return ani

if __name__=="__main__":
    ## hurricane sandy landfall in maryland
    time_slice = slice("2012-10-28", "2012-10-30")
    lon_slice = slice(-78.5,-73.5)
    lat_slice = slice(36, 41)
    get_var = "Rainf"

    download_new = True
    animate = True
    animation_dpi = 80

    buffer_npz_path = Path("test_nldas3_hourly.npz")
    out_animation_path = Path("test_nldas3_hourly.gif")

    if download_new:
        ## Set up a reference file system based on the chunk refs stored in
        ## the parquet file. This only loads the metadata needed to create
        ## the impression of a zarr store on the local machine.
        ## This essentially abstracts away the difference between individual
        ## netCDF files on the s3 bucket by mapping zarr chunks to a
        ## combination of URLs and byte offsets/lengths of netCDF chunks
        ## under each URL
        ref_fs = fsspec.filesystem(
            "reference",
            fo="s3://nasa-waterinsight/virtual/nldas3_hourly.parq",
            remote_protocol="s3",
            asynchronous=True,
            remote_options={"asynchronous":True},
            lazy=True
            )

        ## create a zarr dataset object based on the virtual references.
        ds = xr.open_zarr(
                ref_fs.get_mapper(""),
                consolidated=False,
                decode_times=True,
                )

        ## use data coordinates to identify a subset of the data to retrieve.
        sub = ds.sel(
            time=time_slice,
            lon=lon_slice,
            lat=lat_slice,
            )

        ## Get the user-defined subset of data. Chunks aren't read from the
        ## s3 bucket until .load() is called.
        np.savez(
            file=buffer_npz_path.as_posix(),
            subarr=sub[get_var].load(),
            sublat=sub.lat.load(),
            sublon=sub.lon.load(),
            subtime=sub.time.load(),
            )

    if animate:
        sub_npz = np.load(buffer_npz_path.as_posix())
        ani = animate_geo_raster(
            data=sub_npz["subarr"],
            lats=sub_npz["sublat"],
            lons=sub_npz["sublon"],
            times=[str(t) for t in sub_npz["subtime"]],
            plot_spec={
                "frame_interval":100,
                "title":f"NLDAS-3 {get_var}" + "  {time}",
                "cb_label":get_var,
                "cmap":"nipy_spectral",
                "vmin":0,
                "vmax":20,
                "border_edgecolor":"gray",
                }
            )
        ani.save(out_animation_path.as_posix(), dpi=animation_dpi)
