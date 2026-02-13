import geopandas as gpd
import tqdm
import utilities
import zonal_statistics
import combineCSV

# gdf = gpd.read_file('./inputs/hexgrids.gpkg', layer='tesselated_10ha_hexagons_within_10km_of_plantation')
# gdf = gpd.read_file('./inputs/bomgrid.gpkg', layer='bomgrid')
gdf = gpd.read_file('./inputs/h3.gpkg', layer='h3_elliott_river')
# gdf = gdf[gdf['level'] == 10] # limit the search polygons to level 10 only. At Elliott River there are 4153 polygons at this level

# On the laptop this runs at 16:47 per polygon, so for 64 polygons this will take 17:37:50.

# Load resource configuration - just specify which file you want to use
resource = utilities.load_resource("./resources/dea-ga_s2bm_ard_3.yaml")

url = resource["url"]
sensor_name = resource["name"]
bands = resource["bands"]
print(f"Bands to be collected: {bands}")

# Bounds must be in EPSG 4326 for the STAC API search.
bounds = gdf.to_crs('EPSG:4326').total_bounds.tolist()

# This searches for data within the bounding box of the gdf and within the specified time range.
data = utilities.get_data_from_stac(url, bounds, sensor_name, bands, time_range="2016-01-01/2025-12-31")
print(f"Time range 2016-01-01/2025-12-31") # TODO: make this dynamic

# This resamples the fetched data to monthly frequency.
data_monthly = utilities.resample_stac_data_to_data_monthly(data)
print(f"Raw data size {utilities.calculate_data_size_in_gb(data):.2g} GB, resampled to monthly size {utilities.calculate_data_size_in_gb(data_monthly):.2g} GB") # round the values to 2 significant figures


################################
# Do this function to limit computer resource usage but create the data slower
# level8_gdf = gdf[gdf['level'] == 8].iloc[1:]
# level10_gdf = gdf[gdf['level'] == 10]

# for idx, h3_level8 in tqdm.tqdm(level8_gdf.iterrows(), total=len(level8_gdf), desc="Processing H3 level 8 hexagons"):
#     # Filter level 10 hexagons that are within this level 8 hexagon
#     mask = level10_gdf.within(h3_level8.geometry)
#     child_hexagons = level10_gdf[mask]
    
#     if child_hexagons.empty:
#         continue
    
#     zonal_statistics.compute_zonal_stats_bands_vectorized(
#         data_monthly=data_monthly,
#         gdf=child_hexagons,
#         key_column_name='GRID_ID',
#         bands=bands,
#         output_dir="./outputs/hexagons_elliott_river",
#         overwrite=True
#     )
################################
# Do this function to just get a bunch of data done regardless of the computational implications
level10_gdf = gdf[gdf['level'] == 10]

zonal_statistics.compute_zonal_stats_bands_vectorized(
    data_monthly=data_monthly,
    gdf=level10_gdf,
    key_column_name='GRID_ID',
    bands=bands,
    output_dir="./outputs/hexagons_elliott_river",
    overwrite=True
)
################################

# zonal_statistics.compute_zonal_stats_bands_vectorized(
#     data_monthly=data_monthly,
#     gdf=gdf.iloc[:128],  # Process only the first 64 hexagons for this example
#     key_column_name='GRID_ID',
#     bands=bands,
#     output_dir="C:/Users/rossb/Documents/code/my_code/satellite-data-toolbox/outputs/elliott_river",
#     overwrite=True
#     )

# zonal_statistics.compute_zonal_stats_bands(
#     data_monthly=data_monthly,
#     gdf=gdf.iloc[:64],  # Process the remaining hexagons
#     key_column_name='GRID_ID',
#     bands=bands,
#     output_dir="./outputs/hexagons_elliott_river",
#     overwrite=True
# )

# combineCSV.compile_csvs(
#     output_dir="./outputs/hexagons_elliott_river",
#     pattern="BANDS*.csv",
#     combined_filename="combined.csv",
#     key_column_name='GRID_ID',
#     recursive=False,
#     verbose=False)