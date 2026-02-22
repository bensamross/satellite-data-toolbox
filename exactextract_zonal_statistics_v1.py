import utilities
import exactextract
import geopandas as gpd


# Define the target area
gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
gdf = gdf[gdf['level'] == 8].iloc[:4]
gdf = gdf.to_crs("EPSG: 32756")
print(gdf.head())


# Fetch STAC data
resource = utilities.load_resource("resources/dea-ga_s2bm_ard_3.yaml")

url = resource["url"]
sensor_name = resource["name"]
# bands = resource["bands"]
bands = ["nbart_nir_1", "nbart_red"]
print(f"Bands to be collected: {bands}")
bounds = gdf.to_crs("EPSG: 4326").total_bounds.tolist()

time_range = "2025-12-01/2025-12-31"
stac_data = utilities.get_data_from_stac(
    url=url,
    bounds=bounds,
    sensor_name=sensor_name,
    sensor_bands=bands,
    time_range=time_range)

# stac_data_monthly = stac_data.resample(time="1ME").median()

print(f"Raw data size {utilities.calculate_data_size_in_gb(stac_data):.2g} GB") #, monthly resampled data size {utilities.calculate_data_size_in_gb(stac_data_monthly):.2g} GB")
print(stac_data)

# Calculate zonal statistics using exactextract
for time in stac_data['time']:
    print(time['time'])
    print(
        exactextract.exact_extract(
            stac_data.sel(time=time['time'])[bands],
            gdf,
            ['min', 'mean'],
            output="pandas"
        )
    )