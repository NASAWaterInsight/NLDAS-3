"""
Script for generating files containing geographic polygons associated with
each NLDAS-3 chunk, including index slices for that polygon on the 1km domain.
"""
import json
import shutil
import numpy as np
import boto3
import netCDF4 as nc
import shapely
#from shapely.strtree import STRtree
#from shapely.wkt import loads
from pathlib import Path
from osgeo import ogr,osr

def create_geojson_from_shapely(geojson_path:Path, polygons:list, meta:list,
        meta_types:dict={}):
    """
    Creates a geojson file with a polygon FeatureCollection assigning each
    polygon a unique metadata dict

    :@param geojson_path: Path to geojson file to generate
    :@param polygons: List of shapely Polygon objects to store
    :@param meta: Equal-sized list of dicts providing metadata for each poly.
    """
    '''
    ## all metadata entries must have same keys
    meta_keys = list(meta[0].keys())
    meta_out = {
        k:meta_types.get(k, str)(m[k])
        for k in meta[0].keys()
        for m in meta
        }
    '''

    features = []
    for p,m in zip(polygons, meta):
        features.append({
            "type":"Feature",
            "geometry":shapely.geometry.mapping(p),
            "properties":{k:meta_types.get(k, str)(m[k]) for k in m.keys()},
            })
    with open(geojson_path, "w") as f:
        json.dump({
            "type": "FeatureCollection",
            "features": features
            }, f)
    return geojson_path

def create_gdb_from_shapely(gdb_dir:Path, layer_name:str,
        polygons:list, meta:list, meta_types:dict={}):
    """
    Creates a File Geodatabase with a single polygon layer ass.

    :@param gdb_dir: Path to the output .gdb folder.
    :@param layer_name: Name of the feature class inside the GDB.
    :@param polygons: List of shapely.geometry.Polygon objects.
    :@param meta: List of dicts containing attributes for each polygon.
    :@param meta_types: Optional dict mapping metadata fields to OFT types.
        If a field is not provided, all types default to string
    """
    driver = ogr.GetDriverByName("OpenFileGDB")
    ds = driver.CreateDataSource(gdb_dir.as_posix())

    ## define Spatial Reference (e.g., WGS84 / EPSG:4326)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    layer = ds.CreateLayer(layer_name, srs, ogr.wkbPolygon)

    ## create fields dynamically based on the keys of the first metadata dict
    for k,v in meta[0].items():
        # Defaulting to string type for simplicity; adjust OGR types as needed
        field_defn = ogr.FieldDefn(k, meta_types.get(k, ogr.OFTString))
        layer.CreateField(field_defn)

    layer_defn = layer.GetLayerDefn()

    for p,m in zip(polygons, meta):
        feature = ogr.Feature(layer_defn)
        for k,v in m.items():
            feature.SetField(k, str(v))

        ## convert shapely Polygon to OGR Geometry via wkb
        ogr_geom = ogr.CreateGeometryFromWkb(p.wkb)
        feature.SetGeometry(ogr_geom)

        ## commit feature to layer
        layer.CreateFeature(feature)
        feature = None

    ds = None ## close and save data source
    return gdb_dir

def get_slice_bounds(npx, chunk_size):
    res = npx % chunk_size
    nslc = npx // chunk_size + int(res != 0)
    slc_0 = np.arange(nslc) * chunk_size
    slc_f = (np.arange(nslc) + 1) * chunk_size
    if res != 0:
        slc_f[-1] = slc_f[-2] + res
    return slc_0,slc_f

def get_chunk_polygons(latitudes:np.array, longitudes:np.array,
        chunk_shape:tuple, pixel_resolution:tuple, valid_mask=None,
        return_invalid_polygons=False):
    """
    Given an equirectangular grid of latitudes and longitudes and a chunk
    shape, returns shapely polygons and index slices associated with each
    chunk containing any masked values.

    :@param latitudes: 1d monotonically increasing array of latitude values
        corresponding to each grid cell
    :@param latitudes: 1d monotonically increasing array of longitude values
        corresponding to each grid cell
    :@param chunk_shape: 2-tuple providing the number of pixels per 2d chunk
        along the (lat, lon) axes.
    :@param pixel_resolution: 2-tuple providing the size in degrees of pixels
        along the (lat, lon) axes.
    :@param valid_mask: 2d bool array shaped (latitudes.size, longitudes.size)
        that True for valid data points. If all mask values in a chunk are
        False, that chunk's slices and polygons will not be returned.
        Defaults to all pixels valid.
    :@param return_invalid_polygons: If True, also returns polygons for
        chunks that contain no valid points, marking them as such in metadata.

    :@return: 2-tuple (chunk_polys, chunk_meta)
        `chunk_polys`: an identically sized list of shapely Polygon objects
            corresponding in order to each chunk slices
        `chunk_meta`: list of dicts containing metadata for each polygon,
            including their slice bounds and whether they contain any valid
            land points.
    """
    ## get the index bounds associated with each chunk slice
    assert latitudes.ndim == 1 and longitudes.ndim== 1
    nlats = latitudes.size
    nlons = longitudes.size
    slc_lat_0,slc_lat_f = get_slice_bounds(nlats, chunk_shape[0])
    slc_lon_0,slc_lon_f = get_slice_bounds(nlons, chunk_shape[1])

    ## list all chunk combos in 2d as an array shaped (lat|lon, nchunks)
    slc_0 = np.stack(
        np.meshgrid(slc_lat_0, slc_lon_0, indexing="ij"),
        axis=0,
        ).reshape(2,-1)
    ## subtract 1 so that final references the last *included* pixel index
    slc_f = np.stack(
        np.meshgrid(slc_lat_f-1, slc_lon_f-1, indexing="ij"),
        axis=0,
        ).reshape(2,-1)
    ## indeces of chunks' 2d layout (in terms of chunks not pixels)
    cixs = np.stack(
        np.meshgrid(
            np.arange(slc_lat_0.shape[0]),
            np.arange(slc_lon_0.shape[0]),
            indexing="ij",
            ),
        axis=0,
        ).reshape(2,-1)

    ## get the latlon indeces of the outer extremes of each chunk polygon
    lat_0 = np.round(latitudes[slc_0[0]] - pixel_resolution[0] / 2, 5)
    lat_f = np.round(latitudes[slc_f[0]] + pixel_resolution[0] / 2, 5)
    lon_0 = np.round(longitudes[slc_0[1]] - pixel_resolution[1] / 2, 5)
    lon_f = np.round(longitudes[slc_f[1]] + pixel_resolution[1] / 2, 5)

    ## if a mask is provided and the user doesn't want polygons with no valid
    ## points, restrict the returned slices & polys.
    has_valid_land_points = []
    chunk_ixy,chunk_ixx = [],[]
    chunk_meta = []
    chunk_polys = []
    for i in range(slc_0.shape[-1]):
        has_valid_points = True
        tmp_slc = (slice(slc_0[0,i], slc_f[0,i]+1),
            slice(slc_0[1,i], slc_f[1,i]+1))
        ## if mask is provided and there are no valid points in this chunk,
        ## either skip it or note as such in the metadata, depending on user
        if not valid_mask is None and not np.any(valid_mask[*tmp_slc]):
            if return_invalid_polygons:
                has_valid_points = False
            else:
                continue

        ## make a shapely polygon for this chunk
        tmp_poly = shapely.geometry.Polygon([
            (lon_0[i], lat_0[i]), (lon_f[i], lat_0[i]),
            (lon_f[i], lat_f[i]), (lon_0[i], lat_f[i]),
            ])
        chunk_polys.append(tmp_poly)

        ## collect metadata
        chunk_meta.append({
            "has_valid_points":has_valid_points,
            "lat_slice_start":tmp_slc[0].start,
            "lat_slice_stop":tmp_slc[0].stop,
            "lon_slice_start":tmp_slc[1].start,
            "lon_slice_stop":tmp_slc[1].stop,
            "lat_chunk_ix":cixs[0,i],
            "lon_chunk_ix":cixs[1,i],
            })

    return chunk_polys,chunk_meta

if __name__=="__main__":
    data_dir = Path("data")

    nldas3_path = data_dir.joinpath("nldas3_params.nc")
    #out_gdb_dir = data_dir.joinpath("nldas3_chunks_land.gdb")
    #out_geojson_path = data_dir.joinpath("nldas3_chunks_land.geojson")
    #out_npz_path = data_dir.joinpath("nldas3_chunks_land.npz")
    out_gdb_dir = data_dir.joinpath("nldas3_chunks_all.gdb")
    out_geojson_path = data_dir.joinpath("nldas3_chunks_all.geojson")
    out_npz_path = data_dir.joinpath("nldas3_chunks_all.npz")

    nldas3_chunk_shape = (500, 900) ## pixels (lat, lon)
    nldas3_px_res = (.01,.01) ## degrees (lat, lon)
    return_invalid_polygons = False
    overwrite_gdb = True
    overwrite_geojson = True
    overwrite_npz = True

    """ ----( end typical configuration )---- """

    ## check whether generated files exist
    if out_gdb_dir.exists():
        if overwrite_gdb:
            shutil.rmtree(out_gdb_dir)
        else:
            raise ValueError(
                f"gdb directory exists: {out_gdb_dir.as_posix()}",
                "\nset overwrite_gdb to True to overwrite automatically.")
    else:
        out_gdb_dir.mkdir()
    if out_geojson_path.exists():
        if overwrite_geojson:
            out_geojson_path.unlink()
        else:
            raise ValueError(
                f"geojson exists: {out_geojson_path.as_posix()}\nset",
                "overwrite_geojson to True to overwrite automatically.")
    if out_npz_path.exists():
        if overwrite_npz:
            out_npz_path.unlink()
        else:
            raise ValueError(
                f"npz file exists: {out_npz_path.as_posix()}\nset",
                "overwrite_npz to True to overwrite automatically.")

    ## download the parameter file if it doesn't exist already
    if not nldas3_path.exists():
        s3 = boto3.client("s3")
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
        ## construct shapely polygons and index slices for each NLDAS-3 chunk
        chunk_polys,chunk_meta = get_chunk_polygons(
            latitudes=nldas3_lats,
            longitudes=nldas3_lons,
            chunk_shape=nldas3_chunk_shape,
            ## Use the water surface type to define a land mask
            valid_mask=nldas3_land_mask,
            pixel_resolution=nldas3_px_res,
            return_invalid_polygons=return_invalid_polygons,
            )

    ## generate a gdb file containing the chunk slices and polygons
    create_gdb_from_shapely(
            gdb_dir=out_gdb_dir,
            layer_name="nldas3_chunks",
            polygons=chunk_polys,
            meta=chunk_meta,
            meta_types={
                "has_land_types":ogr.OFTBinary,
                "lat_slice_start":ogr.OFTInteger64,
                "lat_slice_stop":ogr.OFTInteger64,
                "lon_slice_start":ogr.OFTInteger64,
                "lon_slice_stop":ogr.OFTInteger64,
                "lat_chunk_ix":ogr.OFTInteger64,
                "lon_chunk_ix":ogr.OFTInteger64,
                }
            )

    ## generate a geojson with the same chunk slice and polygon data
    create_geojson_from_shapely(
        geojson_path=out_geojson_path,
        polygons=chunk_polys,
        meta=chunk_meta,
        meta_types={
            "has_land_points":bool,
            "lat_slice_start":int,
            "lat_slice_stop":int,
            "lon_slice_start":int,
            "lon_slice_stop":int,
            "lat_chunk_ix":int,
            "lon_chunk_ix":int,
            },
        )

    ## generate a npz file mapping each pixel to its chunk by an index
    ## matching the polygon feature index in both the geojson and gdb files.
    ## consolidate poly and slice info into a single dict per chunk
    chunk_info = [
        {**gjf["properties"], "geometry":gjf["geometry"]["coordinates"]}
        for gjf in json.load(out_geojson_path.open("r"))["features"]
        ]

    ## declare an array covering the nldas-3 domain and set the value of each
    ## chunk to its index in the info list
    chunk_poly_mask = np.full(nldas3_land_mask.shape,  65535, dtype=np.uint16)
    for i,m in enumerate(chunk_meta):
        tmp_slc_lat = slice(m["lat_slice_start"], m["lat_slice_stop"])
        tmp_slc_lon = slice(m["lon_slice_start"], m["lon_slice_stop"])
        chunk_poly_mask[tmp_slc_lat, tmp_slc_lon] = i

    ## store a compressed numpy file with the
    np.savez_compressed(
        out_npz_path,
        chunk_masks=chunk_poly_mask,
        chunk_info=chunk_info,
        )
