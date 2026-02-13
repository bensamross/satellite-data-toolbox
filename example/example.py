import geopandas as gpd
import sys
sys.path.append("..") # this is only required when the imports are a level above the current file, typically not required
import utilities
import zonal_statistics
import combineCSV

gdf = gpd.read_file('./hexgrids_example.gpkg', layer='tesselated_10ha_hexagons_on_bribie_island')

# This fetches the STAC API URL from the resource metadata.
url = utilities.fetch_resource_metadata("../resources.json")['url']

# This fetches the name of the first sensor defined in the resource metadata.
sensor_name = utilities.fetch_resource_metadata("../resources.json")['sensors'][1]['name']

# This defines which bands to fetch from the STAC API based on the first sensor's band definitions.
bands = list(utilities.fetch_resource_metadata("../resources.json")['sensors'][1]['bands'][0].values())

# Bounds must be in EPSG 4326 for the STAC API search.
bounds = gdf.to_crs('EPSG:4326').total_bounds.tolist()

# This searches for data within the bounding box of the gdf and within the specified time range.
data = utilities.get_data_from_stac(url, bounds, sensor_name, bands, time_range="2025-01-01/2025-12-31")

# This resamples the fetched data to monthly frequency.
data_monthly = utilities.resample_stac_data_to_data_monthly(data)

# zonalStatistics.compute_zonal_stats_bands_vectorized(
#     data_monthly=data_monthly,
#     gdf=gdf,
#     key_column_name='GRID_ID',
#     bands=bands,
#     output_dir="./example_outputs",
#     overwrite=True)

combineCSV.compile_csvs(
    output_dir="./example_outputs",
    pattern="BANDS*.csv",
    combined_filename="combined.csv",
    key_column_name='GRID_ID',
    recursive=False,
    verbose=False)