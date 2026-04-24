# NLDAS-3 Data Variables

## Forcing Variables

| Long Name | Key | Units |
| --- | --- | --- |
| Near-surface air temperature | Tair | K |
| Near-surface specific humidity | Qair | kg kg<sup>-1</sup> |
| Surface pressure | PSurf | Pa |
| Northward wind | Wind\_N | m s<sup>-1</sup> |
| Eastward wind | Wind\_E | m s<sup>-1</sup> |
| Downward longwave radiation at the surface | LWdown | W m<sup>-2</sup> |
| Downward shortwave radiation at the surface | SWdown | W m<sup>-2</sup> |
| Total precipitation rate | Rainf | kg m<sup>-2</sup> |

## Model Output Variables

| Variable | Short Name | Units |
| --- | --- | --- |
| Surface net downward shortwave flux | SWnet | W m<sup>-2</sup> |
| Surface net downward longwave flux | LWnet | W m<sup>-2</sup> |
| Surface upward latent heat flux | Qle | W m<sup>-2</sup> |
| Surface upward sensible heat flux | Qh | W m<sup>-2</sup> |
| Downward heat flux in soil | Qg | W m<sup>-2</sup> |
| Snowfall rate (frozen) | Snowf | kg m<sup>-2</sup> s<sup>-1</sup> |
| Rainfall rate (liquid) | Rainf | kg m<sup>-2</sup> s<sup>-1</sup> |
| Total evapotranspiration | Evap | kg m<sup>-2</sup> s<sup>-1</sup> |
| Surface runoff amount | Qs | kg m<sup>-2</sup> s<sup>-1</sup> |
| Subsurface runoff amount | Qsb | kg m<sup>-2</sup> s<sup>-1</sup> |
| Surface temperature | AvgSurfT | K |
| Daily minimum surface temperature | AvgSurfT\_min | K |
| Daily maximum surface temperature | AvgSurfT\_max | K |
| Liquid water content of surface snow | SWE | kg m<sup>-2</sup> |
| Snow depth | SnowDepth | m |
| Surface snow area fraction | SnowFrac | [-] |
| Soil moisture | SoilMoist | m3 m<sup>-3</sup> |
| Soil temperature | SoilTemp | K |
| Potential evapotranspiration | PotEvap | kg m<sup>-2</sup> s<sup>-1</sup> |
| Vapor pressure deficit | VPD | Pa |
| Vegetation transpiration | TVeg | kg m<sup>-2</sup> s<sup>-1</sup> |
| Bare soil evaporation | ESoil | kg m<sup>-2</sup> s<sup>-1</sup> |
| Total canopy water storage | CanopInt | kg m<sup>-2</sup> |
| Water table depth | WaterTableD | m |
| Terrestrial water storage | TWS | mm |
| Groundwater storage | GWS | mm |
| Gross primary productivity | GPP | g m<sup>-2</sup> s<sup>-1</sup> |
| Net primary productivity | NPP | g m<sup>-2</sup> s<sup>-1</sup> |
| Net ecosystem exchange | NEE | g m<sup>-2</sup> s<sup>-1</sup> |
| Leaf area index | LAI | [-] |

## Routing Variables

| Variable | Short Name | Units |
| --- | --- | --- |
| Streamflow | Streamflow | m<sup>3</sup> s<sup>-1</sup> |
| River Depth | RiverDepth | m |
| Flooded fraction | FloodedFrac | [-] |
| Surface water elevation | SurfElev | m |
| Surface water storage | SWS | mm |

## Static Variables

| Variable | Short Name | Units |
| --- | --- | --- |
| Land Use and Vegetation | surface\_class | int |
| Soil Texture Category | soil\_class | int |
| Surface Slope | slope | m m<sup>-1</sup> |
| Surface Aspect | aspect | radians |
| Latitude | latitude | degrees |
| Longitude | longitude | degrees |
