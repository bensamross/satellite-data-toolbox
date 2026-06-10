# from scipy import cluster
import utilities
import exactextract
import geopandas as gpd
import pandas as pd
from dask.distributed import Client, LocalCluster
import tqdm
import csv
import os
import traceback

time_ranges = [
    "2026-01-01/2026-01-31",
    # "2026-01-01/2026-12-31",
    # "2025-01-01/2025-12-31",
    # "2024-01-01/2024-12-31",
    # "2023-01-01/2023-12-31",
    # "2022-01-01/2022-12-31",
    # "2021-01-01/2021-12-31",
    # "2020-01-01/2020-12-31",
    # "2019-01-01/2019-12-31",
    # "2018-01-01/2018-12-31",
    # "2017-01-01/2017-12-31"
    ]

def _append_failure_row(failure_csv_path: str, row: dict):
    fieldnames = [
        "stage",
        "time_range",
        "acquisition_time",
        "level5_id",
        "error",
        "traceback"
    ]
    file_exists = os.path.exists(failure_csv_path)
    with open(failure_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def calculate():

    cluster = LocalCluster(n_workers=1, threads_per_worker=1)
    client = Client(cluster)
    print(client)

    failure_csv_path = r"data/zonal_stats_v8_failures.csv"
    retry_rows = []

    for time_range in tqdm.tqdm(time_ranges, desc='Time ranges'):
        try:
            print(f"Calculating zonal stats for time period: {time_range}")

            # Define the target area
            # gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
            # gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3')
            gdf = gpd.read_file('../data/inputs/h3_joined.gpkg', layer='joined_layer')

            for zone in gdf['zone'].unique():
                print(f"Processing zone {zone}")
                zone_gdf = gdf[gdf['zone'] == zone].copy()
                if zone == 55:
                    zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32755") # sentinel 2 zone 55
                elif zone == 56:
                    zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32756") # sentinel 2 zone 56
                else:
                    raise ValueError(f"Unsupported zone: {zone}")

                # Manual filter to the first few level 5 polygons for testing
                zone_gdf_reprojected = zone_gdf_reprojected[zone_gdf_reprojected['GRID_ID_level5'].isin(zone_gdf_reprojected['GRID_ID_level5'].unique()[:4])]
                
                # Fetch STAC data
                # resource = utilities.load_resource(r"resources/dea-ga_s2bm_ard_3.yaml")
                resource = utilities.load_resource(r"../resources/pc-sentinel-2-l2a.yaml")
                # resource = utilities.load_resource("resources/pc-landsat-c2-l2.yaml")

                url = resource["url"]
                sensor_name = resource["name"]
                bands = resource["bands"]
                print(f"Bands to be collected: {bands}")
                bounds = zone_gdf_reprojected.to_crs("EPSG: 4326").total_bounds.tolist()

                stac_data = utilities.get_data_from_stac(
                    url=url,
                    bounds=bounds,
                    sensor_name=sensor_name,
                    sensor_bands=bands,
                    time_range=time_range
                )

                print(f"Raw data size {utilities.calculate_data_size_in_gb(stac_data):.2g} GB")
                print(f'Time steps: {len(stac_data.time)}')
                print(stac_data)

                df_output = pd.DataFrame()

                for time in tqdm.tqdm(stac_data['time'], desc='Time steps'):
                    try:
                        time_value = pd.to_datetime(time.values)

                        for level5_id in tqdm.tqdm(
                            zone_gdf_reprojected['GRID_ID_level5'].unique(),
                            total=len(zone_gdf_reprojected['GRID_ID_level5'].unique()),
                            desc='Level 5 polygons'
                        ):
                            try:
                                subset = zone_gdf_reprojected[zone_gdf_reprojected['GRID_ID_level5'] == level5_id]
                                df = exactextract.exact_extract(
                                    rast=stac_data.sel(time=time['time'])[bands],
                                    vec=subset,
                                    ops=["mean"],
                                    strategy="raster-sequential",
                                    output="pandas",
                                    include_cols=["GRID_ID_level10"],
                                    progress=False
                                )
                                df["acquisition_time"] = time_value
                                df_output = pd.concat([df_output, df], ignore_index=True)

                            except Exception as e:
                                print(f"Error processing level 5 polygon {level5_id} at time step {time}: {e}")
                                row = {
                                    "stage": "level5_polygon",
                                    "time_range": time_range,
                                    "acquisition_time": str(time_value),
                                    "level5_id": str(level5_id),
                                    "error": str(e),
                                    "traceback": traceback.format_exc()
                                }
                                _append_failure_row(failure_csv_path, row)
                                retry_rows.append({
                                    "time_range": time_range,
                                    "acquisition_time": str(time_value),
                                    "level5_id": str(level5_id)
                                })

                        df_output.to_csv(f"data/zonal_stats_v8_{time_range.replace('/', '_')}.csv", index=False)

                    except Exception as e:
                        print(f"Error processing time step {time}: {e}")
                        row = {
                            "stage": "time_step",
                            "time_range": time_range,
                            "acquisition_time": str(pd.to_datetime(time.values)) if "time" in locals() else "",
                            "level5_id": "",
                            "error": str(e),
                            "traceback": traceback.format_exc()
                        }
                        _append_failure_row(failure_csv_path, row)

        except Exception as e:
            print(f"Error processing time range {time_range}: {e}")
            row = {
                "stage": "time_range",
                "time_range": time_range,
                "acquisition_time": "",
                "level5_id": "",
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            _append_failure_row(failure_csv_path, row)

    # Build retry queue from failed level-5 operations
    if retry_rows:
        retry_df = pd.DataFrame(retry_rows).drop_duplicates()
        retry_df.to_csv(r"data/zonal_stats_v8_retry_queue.csv", index=False)
        print(f"Retry queue written to zonal_stats_v8_retry_queue.csv with {len(retry_df)} items")

    print(f"Failure log written to {failure_csv_path}")

if __name__ == "__main__":
    calculate()

# for January to December 2025 there are 86 time steps
# 38 total level 5 polygons over this time range takes 32m:54s

# for January to December 2024 there are 88 time steps
# 38 total level 5 polygons over this time range takes 35m:15s

# 31m:51s for January 2026 to now
# 2025 has 85 time steps