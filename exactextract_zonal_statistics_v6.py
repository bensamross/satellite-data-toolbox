import utilities
import exactextract
import geopandas as gpd
import pandas as pd
from dask.distributed import Client, LocalCluster
import tqdm

time_ranges = [
    # "2026-01-01/2026-12-31",
    "2025-01-01/2025-12-31",
    "2024-01-01/2024-12-31",
    "2023-01-01/2023-12-31",
    "2022-01-01/2022-12-31",
    "2021-01-01/2021-12-31",
    "2020-01-01/2020-12-31",
    "2019-01-01/2019-12-31",
    "2018-01-01/2018-12-31",
    "2017-01-01/2017-12-31"]

def calculate():

    for time_range in tqdm.tqdm(time_ranges, desc='Time ranges'):
        print(f"Calculating zonal stats for time period: {time_range}")

        # Define the target area
        gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
        gdf_level5 = gdf[gdf['level'] == 5]
        # gdf_level7 = gdf[gdf['level'] == 7]
        gdf_level10 = gdf[gdf['level'] == 10]

        # NOTE: There are about 173 level 10 polygons in each level 7 polygon
        intersection = gpd.overlay(gdf_level10, gdf_level5, how='intersection')
        intersection.drop(columns=["within0km_2", "within10km_2"], inplace=True) # Drop some problematic columns
        intersection.rename(columns={"GRID_ID_1": "GRID_ID", "level_1": "level"}, inplace=True) #  rename columns from the join event
        # intersection = intersection[intersection["level"] == 10].copy() # TODO: This has since been disabled as I don't think it will matter??

        # Fetch STAC data
        # resource = utilities.load_resource("resources/dea-ga_s2bm_ard_3.yaml")
        resource = utilities.load_resource("resources/pc-sentinel-2-l2a.yaml")
        # resource = utilities.load_resource("resources/pc-landsat-c2-l2.yaml")

        url = resource["url"]
        sensor_name = resource["name"]
        bands = resource["bands"]
        print(f"Bands to be collected: {bands}")
        bounds = intersection.to_crs("EPSG: 4326").total_bounds.tolist()

        # time_range = "2024-01-01/2024-12-31"
        stac_data = utilities.get_data_from_stac(
            url=url,
            bounds=bounds,
            sensor_name=sensor_name,
            sensor_bands=bands,
            time_range=time_range
        )

        # Ensure dask-backed chunks (tune sizes later)
        # stac_data = stac_data.chunk({"time": 1, "x": 1024, "y": 1024})

        print(f"Raw data size {utilities.calculate_data_size_in_gb(stac_data):.2g} GB")
        print(f'Time steps: {len(stac_data.time)}')
        print(stac_data)

        cluster = LocalCluster(n_workers=2, threads_per_worker=2)
        client = Client(cluster)
        print(client)

        intersection = intersection.to_crs("EPSG: 32756") # sentinel 2
        # intersection = intersection.to_crs("EPSG: 32656") # landsat 8

        df_output = pd.DataFrame()


        for time in tqdm.tqdm(stac_data['time'], desc='Time steps'): # For each time step, which means the raster is loaded in memory one at a time, which is good for memory management
            for level5_id in tqdm.tqdm(intersection['GRID_ID_2'].unique(), total=len(intersection['GRID_ID_2'].unique()), desc='Level 5 polygons'):
                subset = intersection[intersection['GRID_ID_2'] == level5_id]
                # print(level7_id, time['time'])
                df = exactextract.exact_extract(
                    rast=stac_data.sel(time=time['time'])[bands],
                    vec=subset,
                    ops=["mean"],
                    strategy="raster-sequential",
                    output="pandas",
                    include_cols=["GRID_ID"],
                    progress=False
                )
                df["time"] = pd.to_datetime(time.values)
                df_output = pd.concat([df_output, df], ignore_index=True)
                # print(df)

        df_output.to_csv(f"zonal_stats_v6_{time_range.replace('/', '_')}.csv", index=False)

if __name__ == "__main__":
    calculate()

# for January to December 2025 there are 86 time steps
# 38 total level 5 polygons over this time range takes 32m:54s

# for January to December 2024 there are 88 time steps
# 38 total level 5 polygons over this time range takes 35m:15s

# 31m:51s for January 2026 to now