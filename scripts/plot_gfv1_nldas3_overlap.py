"""
Plot the number of chunks overlapped by each GFv1 polygon given the raster
created by create_nldas3_chunk_polygons.py and the adjacency matrix calculated
by calc_gfv1_nldas3_overlap.py
"""
import numpy as np
import netCDF4 as nc
import pickle as pkl
import boto3
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Polygon

from plot_polygons import plot_geo_ints

if __name__=="__main__":
    data_dir = Path("data")
    fig_dir = Path("figures")
    nldas3_param_path = data_dir.joinpath("nldas3_params.nc")

    #gfv1_raster_pkl_path = data_dir.joinpath("nldas3_gfv1_land.pkl")
    #adj_npz_path = data_dir.joinpath("adjacency_gfv1_nldas3_land.npz")
    #out_png_overlap_path = fig_dir.joinpath("overlap_gfv1_nldas3_land.png")

    gfv1_raster_pkl_path = data_dir.joinpath("nldas3_gfv1_all.pkl")
    adj_npz_path = data_dir.joinpath("adjacency_gfv1_nldas3_all.npz")
    out_png_overlap_path = fig_dir.joinpath("overlap_gfv1_nldas3_all.png")

    oob_val_gfv1 = -1
    oob_val_nldas3 = 65535
    oob_val_out_land = -1 ## oob wrt gfv1 but valid nldas3 pixel
    oob_val_out_both = -2 ## oob wrt gfv1 and nldas3
    oob_color_land = "#1b1d26"
    oob_color_both = "black"
    cmap_name = "rainbow"

    ## download the parameter file if it doesn't exist already
    if not nldas3_param_path.exists():
        s3 = boto3.client("s3")
        s3.download_file(
            "nasa-waterinsight",
            "NLDAS3/static/NLDAS-3_dominant-soil-vegetation.nc",
            nldas3_param_path.as_posix(),
            )

    ## extract geo coords and land mask from the parameter file
    with nc.Dataset(nldas3_param_path, "r") as param_ds:
        nldas3_lats = param_ds["lat"][...]
        nldas3_lons = param_ds["lon"][...]
        ## class 14 corresponds to water
        nldas3_land_mask = ~(param_ds["Soiltype_inst"][...] == 14)
        ## construct shapely polygons and index slices for each NLDAS-3 chunk

    ## load the adjacency matrix and info
    adj = np.load(adj_npz_path, allow_pickle=True)

    ## count nonzero over chunk axis to get chunks per gfv1 polygon
    pchunks = np.count_nonzero(adj["adj_gfv1_nldas3"], axis=1)

    ## get a mapping from polygon coords to chunk overlap counts
    pcoords_to_count = {
        int(c):int([v,oob_val_out_land][int(c==oob_val_gfv1)])
        for c,v in zip(adj["poly_coords"], pchunks)
        }
    f_pcoords_to_count = np.vectorize(pcoords_to_count.get)

    ## load the GFv1 polygon raster
    praster,_,_ = pkl.load(gfv1_raster_pkl_path.open("rb"))

    ## map polygon indices to their counts
    ccounts = f_pcoords_to_count(praster.ravel()).reshape(praster.shape)

    ## where OOB for both, assign a new number
    m_oob_both = (~nldas3_land_mask) & (ccounts == oob_val_out_land)
    ccounts[m_oob_both] = oob_val_out_both

    ## make a mapping between unique ints and their colors
    max_count = np.amax(ccounts)
    unq_count = np.unique(ccounts)
    cmap = plt.get_cmap(cmap_name)
    colors = {
        **{v:cmap(v/max_count) for v in unq_count},
        oob_val_out_land:oob_color_land,
        oob_val_out_both:oob_color_both,
        }

    ## plot the raster-based chunk inclusions of unmasked pixels
    plot_geo_ints(
        int_data=ccounts,
        lat=nldas3_lats,
        lon=nldas3_lons,
        shapes=None,
        geo_bounds=None,
        latlon_ticks=True,
        int_labels=None,
        fig_path=out_png_overlap_path,
        cbar_ticks=True,
        colors=colors,
        show=False,
        plot_spec={
            "title":"Number of NLDAS-3 chunks intersected by GFv1 polygons",
            "title_fontsize":16,
            "cbar_label":"NLDAS-3 chunks count (-2,-1 -> OOB)",
            "cbar_orient":"horizontal",
            "cbar_pad":.1,
            "cbar_shrink":.9,
            "cartopy_feats":["borders", "states"],
            "cbar_disable":False,
            "origin":"lower",
            "tick_frequency":500,
            "tick_rotation":45,
            "border_linewidth":1,
            "dpi":500,
            }
        )
