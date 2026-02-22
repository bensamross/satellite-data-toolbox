import xrspatial.zonal as zonal
import geocube
from dask.distributed import Client
import geopandas as gpd
import sys
sys.path.append('..')
import utilities

# 1. Start a Dask client for parallel computing
client = Client()

# 5. Define Zones (Example: Using a geojson/gdf to create zone raster)
gdf = gpd.read_file('../data/inputs/h3.gpkg', layer='h3_elliott_river')
gdf = gdf.iloc[:1]

# 2. Search STAC
resource = utilities.load_resource("../resources/dea-ga_s2bm_ard_3.yaml")

url = resource["url"]
sensor_name = resource["name"]
bands = resource["bands"]
print(f"Bands to be collected: {bands}")

bounds = gdf.to_crs("EPSG: 4326").total_bounds.tolist()

time_range = "2025-12-01/2025-12-31"
data = utilities.get_data_from_stac(
    url=url,
    bounds=bounds,
    sensor_name=sensor_name,
    sensor_bands=bands,
    time_range=time_range)

# 4. Calculate NDVI
# red = data.sel(band="red")
# nir = data.sel(band="nir")
# ndvi = (nir - red) / (nir + red)


zones_raster = geocube.api.core.make_geocube(vector_data=gdf, like=data.isel(time=0))
# Assume 'zones_raster' is created here

# 6. Compute Zonal Stats
stats = zonal.stats(zones=zones_raster, values=data.mean(dim="time"))
print(stats.compute()) # Output as a pandas DataFrame
