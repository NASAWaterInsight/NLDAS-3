import pystac
import icechunk
import s3fs
import xarray as xr
from typing import Union, List, Optional
from shapely.geometry import shape, box
from datetime import datetime
import pandas as pd
import logging
import dask

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Benchmark:
    def __init__(
        self,
        collection: Union[pystac.ItemCollection, icechunk.Repository],
        start_date: str = None,
        end_date: str = None,
        latitude: Optional[slice] = None,
        longitude: Optional[slice] = None,
        fs: Union[s3fs.S3FileSystem, None] = None
    ):
        self.collection = collection
        self.start_date = pd.to_datetime(start_date) if start_date else None
        self.end_date = pd.to_datetime(end_date) if end_date else None
        self.latitude = latitude
        self.longitude = longitude
        self.fs = fs

    def point_intersects_item_geometry(self, item: pystac.Item) -> bool:
        """Check if a box intersects with a STAC item's geometry"""
        item_shape = shape(item.geometry)
        
        if self.latitude and self.longitude:
            bbox = box(self.longitude.start, self.latitude.start, self.longitude.stop, self.latitude.stop)
        else:
            bbox = box(-169, 7, -52, 72)
        return item_shape.intersects(bbox)

    def datetimes_overlap(self, item: pystac.Item) -> bool:
        """Check if the item's datetime range overlaps with the benchmark's datetime range"""
        item_start_datetime = pd.to_datetime(item.properties["start_datetime"])
        item_end_datetime = pd.to_datetime(item.properties["end_datetime"])
        return self.start_date <= item_end_datetime and self.end_date >= item_start_datetime
        
    def search_for_assets(self) -> List[str]:
        """Search for assets that intersect with the given coordinates and / or datetime range"""
        if isinstance(self.collection, pystac.ItemCollection):
            filtered_items = []
            for item in self.collection:
                in_bounds, in_time = True, True
                if self.latitude is not None and self.longitude is not None:
                    in_bounds = self.point_intersects_item_geometry(item)
                if self.start_date and self.end_date:
                    in_time = self.datetimes_overlap(item)
                if in_bounds and in_time:
                    filtered_items.append(item)

            netcdf_hrefs = []
            for item in filtered_items:
                for _, asset in item.assets.items():
                    if asset.media_type in ['application/netcdf', 'application/x-netcdf']:
                        netcdf_hrefs.append(asset.href)
            
            return netcdf_hrefs
        return []

class Timeseries(Benchmark):
    def get_timeseries(self, variable: str) -> xr.DataArray:
        """Get timeseries data for the specified parameters"""
        logger.info(f"Starting timeseries generation for variable: {variable}")
        
        if isinstance(self.collection, pystac.ItemCollection):
            logger.info("Processing STAC ItemCollection")
            assets = self.search_for_assets()
            logger.info(f"Found {len(assets)} matching assets")
            
            logger.info("Opening and combining datasets...")
            dataset = xr.open_mfdataset(
                [self.fs.open(asset) for asset in assets], 
                chunks={}
            )
            logger.info("Dataset opened successfully")
                
        elif isinstance(self.collection, icechunk.Repository):
            logger.info("Processing Icechunk Repository")
            session = self.collection.readonly_session(branch="main")
            logger.info("Opening dataset from Icechunk...")
            dataset = xr.open_dataset(
                session.store, 
                consolidated=False, 
                zarr_format=3, 
                engine="zarr"
            )
            logger.info("Dataset opened successfully")
        else:
            raise ValueError("Unsupported collection type")

        # slice
        logger.info(f"Slicing data for time range: {self.start_date} to {self.end_date}")
        data_subset = dataset[variable].sel(
            time=slice(self.start_date, self.end_date), 
            lat=self.latitude, 
            lon=self.longitude
        )
        
        # load
        logger.info("Loading data into memory...")
        data_subset = data_subset.load()
        logger.info("Data loaded successfully")

        # calculate
        logger.info("Calculating mean across lat/lon dimensions...")
        result = data_subset.mean(["lat", "lon"])
        logger.info("Timeseries generation complete")
        
        return result

