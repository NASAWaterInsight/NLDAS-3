import numpy as np
import netCDF4 as nc
import shapely
import pickle as pkl
import geopandas as gpd
import botocore
import boto3
from time import perf_counter
from shapely.strtree import STRtree
from shapely.wkt import loads
from pathlib import Path
from osgeo import ogr,osr

def get_bounding_latlon_slice(lat, lon, lat_bounds=None, lon_bounds=None):
    """
    Calculate minimum spanning pixel index bounds for the provided latitude
    and longitude arrays given optional coordinate constraints

    :@param lat: 2d array of latitude values
    :@param lon: 2d array of longitude values
    :@param lat_bounds: 2-tuple (min, max) bounds to apply to the domain
    :@param lon_bounds: 2-tuple (min, max) bounds to apply to the domain

    :@return: 2-tuple of slices (slice_y, slice_x) extracting a rectangle
        around the valid domain (inclusive wrt the provided bounds).
    """
    assert lat.shape==lon.shape and lon.ndim==2
    ## establish the bounding box for analysis
    if lat_bounds is None:
        ymin,ymax = np.amin(lat),np.amax(lat)
    else:
        ymin,ymax = lat_bounds
    if lon_bounds is None:
        xmin,xmax = np.amin(lon),np.amax(lon)
    else:
        xmin,xmax = lon_bounds
    ## determine the 2d subgrid bounding box given the provided bounds
    m_valid = (lat >= ymin) & (lat <= ymax) & (lon >= xmin) & (lon <= xmax)
    assert np.any(m_valid), \
        "provided latlon bounds are out of range of the provided coord arrays"
    m_valid_y = np.any(m_valid, axis=1)
    m_valid_x = np.any(m_valid, axis=0)
    slcy = slice(np.argmax(m_valid_y),
            m_valid_y.size - np.argmax(m_valid_y[::-1]))
    slcx = slice(np.argmax(m_valid_x),
            m_valid_x.size - np.argmax(m_valid_x[::-1]))
    return slcy,slcx

def get_poly_raster(latitudes, longitudes, gdb_file:Path, gdb_layer,
    lat_bounds=None, lon_bounds=None, gdb_fields:list=None,
    return_subgrid_slices=False, debug=False):
    """
    Given latitude and longitude coordinate arrays and a gpd file, return
    an integer array assigning each pixel to the polygon that contains it,
    maintaining metadata about the polygons from the gpd file.
    Optionally provide latitude and longitude bounds to subset the grid.

    :@param lat: 2d array of latitude values in the domain
    :@param lon: 2d array of longitude values in the domain
    :@param gdb_file: gpd file containing polygons within the provided
        lat/lon domain.
    :@param gdb_layer: layer within the gpd file to extract
    :@param lat_bounds: optional (min,max) bounds for returned array
    :@param lon_bounds: optional (min,max) bounds for returned array
    :@param gdb_fields: List of strings matching the names of auxiliary
        fields in the gpd database to return alongside the raster.
    :@param return_subgrid_slices: Boolean; if True, also returns a 2-tuple of
        slices (yslice, xslice) that extract the subgrid of the provided lat
        and lon arrays conforming the the provided bounds

    :@return: 2-tuple (poly_ints, metadata). poly_ints is an array of integer
        values shaped identically to the latitude and longitude arrays, such
        that the integers indicate which polygon each pixel falls within.
        metadata is a list of dicts that is equal in length to the number of
        unique values in poly_ints, such that poly_ints's values provide
        the index of the corresponding polygon dictionary. Each dict contains
        at least one field "poly_idx" providing the integer of that polygon
        with respect to the original gpd file, but may contain additional
        fields as specified by gpd file. If return_subgrid_slices is
        True, returns 3-tuple like:
        (poly_ints:np.array, metadata:list, (yslice:slice, xslice:slice))
    """
    if debug:
        print(f"{perf_counter():.3f} Reading gpd file ")
    lat,lon = latitudes,longitudes
    ## extract the polygons from the gpd file
    gdf = gpd.read_file(gdb_file, layer=gdb_layer).to_crs(epsg=4326)
    colkeys = []
    if not gdb_fields is None:
        for k in gdb_fields:
            assert k in gdf.keys(), f"Not found in gpd fields: {k}"
            colkeys.append(k)

    ## retain only polygongs that intersect the overall lat/lon bounding box
    polys = []

    ## establish the bounding box for analysis
    if lat_bounds is None:
        ymin,ymax = np.amin(lat),np.amax(lat)
    else:
        ymin,ymax = lat_bounds
    if lon_bounds is None:
        xmin,xmax = np.amin(lon),np.amax(lon)
    else:
        xmin,xmax = lon_bounds
    bbox = shapely.geometry.Polygon([
        (xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

    ## determine the 2d subgrid bounding box given the provided bounds
    slcy,slcx = get_bounding_latlon_slice(lat, lon, lat_bounds, lon_bounds)

    ## subset the coordinate arrays
    lat = lat[slcy,slcx]
    lon = lon[slcy,slcx]

    ## make a shapely point for each coordinate combination
    flat_lat = lat.ravel()
    flat_lon = lon.ravel()
    points = [shapely.geometry.Point(x, y) for x, y in zip(flat_lon, flat_lat)]

    ## Subset the polygons to only those which intersecet the bounding box
    poly_ixs,poly_ids,polygons = zip(*[
        (i,id(p),p)
        for i,p in enumerate(gdf.geometry.values) if p.intersects(bbox)
        ])
    id_to_ix = dict(zip(poly_ids,poly_ixs))

    if debug:
        print(f"{perf_counter():.3f} Initializing STR Tree ")
    ## make an STR tree of the polygons so that it's efficient to rule out
    ## inclusion of pixels that are strictly outside the minimum bounding
    ## rectangle. See linked document:
    ## https://ia600709.us.archive.org/13/items/nasa_techdoc_19970016975/19970016975.pdf
    tree = STRtree(polygons)

    if debug:
        print(f"{perf_counter():.3f} Grouping by polygons ")
    ## For each of the points, see if it is in any of the polygon's MBR
    ## by querying the STR tree. Then do a refined check to see which of the
    ## polygons actually contain it.
    poly_raster = np.full(len(points), -1, dtype=int)
    for rix,pt in enumerate(points):
        if int(shapely.__version__[0])==1:
            ## tree only contains polygons from subset so must convert to the
            ## polygon indeces wrt the gpd file ordering if version 1
            cand_polys = tree.query(pt)
            cand_pixs = [id_to_ix[id(p)] for p in cand_polys]
        else:
            ## otherwise query returns the indeces wrt the input polys
            cand_poly_subset_ixs = tree.query(pt)
            cand_polys = [polygons[ix] for ix in cand_poly_subset_ixs]
            cand_pixs = [poly_ixs[ix] for ix in cand_poly_subset_ixs]
        ## use the new polygon indeces, not the ones from the gpd file.
        ## the original gpd file indeces will be returned in the metadata
        #for pix,poly in enumerate(cand_polys):
        for pix,poly in zip(cand_pixs,cand_polys):
            if poly.contains(pt):
                poly_raster[rix] = pix
                break

    ## extract the requested auxiliary column data from the polygons, and
    ## convert the int values from the original polygon indeces to contiguous
    ## values starting at 0, with -1 still representing masked values
    unq_pixs = np.unique(poly_raster)
    if -1 in unq_pixs:
        unq_pixs = np.delete(unq_pixs, 0) ## -1 should always be 0 index
    metadata = [{"poly_idx":pix, **{k:gdf[k][pix] for k in colkeys}}
        for i,pix in enumerate(unq_pixs)]
    val_to_ix = {v:ix for ix,v in enumerate(unq_pixs)}
    val_to_ix[-1] = -1
    poly_raster = np.vectorize(val_to_ix.get)(poly_raster)

    if return_subgrid_slices:
        return poly_raster.reshape(lat.shape),metadata,(slcy,slcx)
    return poly_raster.reshape(lat.shape),metadata

if __name__=="__main__":
    data_dir = Path("data")
    gfv1_gdb_path = data_dir.joinpath("nhm/GFv1.1.gdb.zip")
    nldas3_path = data_dir.joinpath("nldas3_params.nc")
    out_pkl_path = data_dir.joinpath("nldas3_gfv1.pkl")

    ## download the parameter file if it doesn't exist already
    if not nldas3_path.exists():
        boto_cfg = botocore.client.Config(signature_version=botocore.UNSIGNED)
        s3 = boto3.client("s3", config=boto_cfg)
        s3.download_file(
            "nasa-waterinsight",
            "NLDAS3/static/NLDAS-3_dominant-soil-vegetation.nc",
            nldas3_path.as_posix(),
            )

    ## extract geo coords and land mask from the parameter file
    with nc.Dataset(nldas3_path, "r") as param_ds:
        nldas3_lats = param_ds["lat"][...]
        nldas3_lons = param_ds["lon"][...]
        ## class 14 corresponds to water
        nldas3_land_mask = ~(param_ds["Soiltype_inst"][...] == 14)

    lats,lons = np.meshgrid(nldas3_lats, nldas3_lons, indexing="ij")

    poly_raster,metadata,slcs = get_poly_raster(
        latitudes=lats,
        longitudes=lons,
        gdb_file=gfv1_gdb_path,
        gdb_layer="nhru_v1_1",
        gdb_fields=["nhm_id", "hru_id_nat", "Shape_Length", "Shape_Area"],
        lat_bounds=None,
        lon_bounds=None,
        return_subgrid_slices=True,
        debug=True
        )
    pkl.dump((poly_raster, metadata, slcs), out_pkl_path.open("wb"))

    exit(0)
    hru_meta,hru_geom = zip(*[(dict(g),g.geometry) for g in hru])
    print(dir(hru_geom))
