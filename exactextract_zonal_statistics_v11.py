# The general premise in this version
# loop through the h3_level5 geometries
# for each geometry, search for STAC items over a time period
# for each time step, perform zonal stats for the h3_level10 geometries within the current h3_level5 geometry
# append the data to a dataframe and write to csv at the end of each STAC time instance because thos code will be run regularly and fetch new STAC items as they occur

import utilities
import exactextract
import geopandas as gpd
import pandas as pd
from dask.distributed import Client, LocalCluster
import tqdm
import csv
import os
import traceback

verbose = True
dry_run = True # Export STAC metadata only
# resource = utilities.load_resource(r"resources/dea-ga_s2bm_ard_3.yaml")
# resource = utilities.load_resource(r"resources/pc-sentinel-2-l2a.yaml")
# resource = utilities.load_resource("resources/pc-landsat-c2-l2.yaml")
# resource = utilities.load_resource(r"resources/dea-ga_ls8c_ard_3.yaml")
resource = utilities.load_resource(r"resources/dea-ga_ls7e_ard_3.yaml")
# resource = utilities.load_resource(r"resources/dea-ga_ls5t_ard_3.yaml")

url = resource["url"]
sensor_name = resource["name"]
bands = resource["bands"]
time_ranges = [
    # "2026-01-01/2026-01-31",
    # "2026-01-01/2026-12-31",
    # "2025-01-01/2025-01-31",
    # "2025-01-01/2025-12-31",
    # "2024-01-01/2024-12-31",
    # "2023-01-01/2023-12-31",
    # "2022-01-01/2022-12-31",
    # "2021-01-01/2021-12-31",
    # "2020-01-01/2020-12-31",
    # "2019-01-01/2019-12-31",
    # "2018-01-01/2018-12-31",
    # "2017-01-01/2017-12-31", # Sentinel 2 starts sometime in 2017 resolution 10m

    # Landsat 8 starts sometime in April 2013 resolution 15m
    # "2017-01-01/2017-12-31",
    # "2016-01-01/2016-12-31",
    # "2015-01-01/2015-12-31",
    # "2014-01-01/2014-12-31",

    # Landsat 7 starts sometime in August 1999 resolution 15m
    "2014-01-01/2014-01-31",
    # "2013-01-01/2013-12-31",
    # "2012-01-01/2012-12-31",
    # "2011-01-01/2011-12-31",
    # "2010-01-01/2010-12-31",
    # "2009-01-01/2009-12-31",
    # "2008-01-01/2008-12-31",
    # "2007-01-01/2007-12-31",
    # "2006-01-01/2006-12-31",
    # "2005-01-01/2005-12-31",
    # "2004-01-01/2004-12-31",
    # "2003-01-01/2003-12-31",
    # "2002-01-01/2002-12-31",
    # "2001-01-01/2001-12-31",
    # "2000-01-01/2000-12-31",
    # "1999-01-01/1999-12-31",

    # Landsat 5 starts sometime in September 1987 resolution 30m
    # "1999-01-01/1999-12-31",
    # "1998-01-01/1998-12-31",
    # "1997-01-01/1997-12-31",
    # "1996-01-01/1996-12-31",
    # "1995-01-01/1995-12-31",
    # "1994-01-01/1994-12-31",
    # "1993-01-01/1993-12-31",
    # "1992-01-01/1992-12-31",
    # "1991-01-01/1991-12-31",
    # "1990-01-01/1990-12-31",
    # "1989-01-01/1989-12-31",
    # "1988-01-01/1988-12-31",
    # "1987-01-01/1987-12-31"
    ]

# Define the target area
# gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
# gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3')
gdf = gpd.read_file('data/inputs/h3_joined.gpkg', layer='joined_layer')

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

    # Setup a local Dask cluster with 1 worker and 2 threads per worker (adjust as needed based on your system's resources) but since the operations are small and numerous and we are writing to csv then less workers tend to be better
    cluster = LocalCluster(n_workers=1, threads_per_worker=2)
    client = Client(cluster)
    print(client)

    failure_csv_path = r"data/zonal_stats_v11_failures.csv"
    retry_rows = []
    failure_count = 0

    for time_range in tqdm.tqdm(time_ranges, desc='Human defined time ranges'):
        try:
            if verbose:
                print(f"Calculating zonal stats for time period: {time_range}")
            df_output = pd.DataFrame() # Initialise an empty dataframe to store results for the current time range            

            for zone in gdf['zone'].unique():
                if verbose:
                    print(f"Processing zone {zone}")
                zone_gdf = gdf[gdf['zone'] == zone].copy()
                if zone == 55:
                    # zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32755") # sentinel 2 zone 55
                    zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32655") # landsat 8 zone 55
                elif zone == 56:
                    # zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32756") # sentinel 2 zone 56
                    zone_gdf_reprojected = zone_gdf.to_crs("EPSG: 32656") # landsat 8 zone 56
                else:
                    raise ValueError(f"Unsupported zone: {zone}")

                ################################
                
                # Manual filter to the first few level 5 polygons for testing
                # zone_gdf_reprojected = zone_gdf_reprojected[zone_gdf_reprojected['GRID_ID_level5'].isin(zone_gdf_reprojected['GRID_ID_level5'].unique()[:4])]
                
                ################################
                
                for level5_id in tqdm.tqdm(zone_gdf_reprojected['GRID_ID_level5'].unique(), desc='Level 5 polygons'):
                    # Fetch STAC data for the current level 5 polygon bounds within the current time range from the list above
                    bounds = zone_gdf_reprojected[zone_gdf_reprojected['GRID_ID_level5'] == level5_id].to_crs("EPSG: 4326").total_bounds.tolist() # get bounds for the current level 5 polygon (even though it's an aggregate of multiple level 10 polygons, the bounds should be the same for all level 10 polygons within it so we can just use the level 5 bounds for STAC searching)
                    stac_data = utilities.get_data_from_stac(
                        url=url,
                        bounds=bounds,
                        sensor_name=sensor_name,
                        sensor_bands=bands,
                        time_range=time_range
                    )

                    if verbose:
                        # print(f"Bands to be collected: {bands}")
                        print(f"Raw data size {utilities.calculate_data_size_in_gb(stac_data):.2g} GB")
                        # print(f'Time steps: {len(stac_data.time)}')
                        print(stac_data)

                    for time in tqdm.tqdm(stac_data['time'], desc=f'STAC time steps for {level5_id}'):
                        # For each STAC time step, perform zonal stats for the level 10 polygons within the current level 5 polygon and append the results to the output dataframe

                        try:
                            time_value = pd.to_datetime(time.values)
                            
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
                            df["sensor_name"] = sensor_name
                            df_output = pd.concat([df_output, df], ignore_index=True)

                        # TODO: commented out because it's redundant and should be moved elsewhere
                        # except Exception as e:
                        #     if verbose:
                        #         print(f"Error processing level 5 polygon {level5_id} at time step {time}: {e}")
                        #     row = {
                        #         "stage": "level5_polygon",
                        #         "time_range": time_range,
                        #         "acquisition_time": str(time_value),
                        #         "level5_id": str(level5_id),
                        #         "error": str(e),
                        #         "traceback": traceback.format_exc()
                        #     }
                        #     _append_failure_row(failure_csv_path, row)
                        #     failure_count += 1
                        #     retry_rows.append({
                        #         "time_range": time_range,
                        #         "acquisition_time": str(time_value),
                        #         "level5_id": str(level5_id)
                        #     })

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
                            failure_count += 1

            df_output.to_csv(f"data/zonal_stats_v11_{time_range.replace('/', '_')}_{sensor_name}.csv", index=False)

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
            failure_count += 1

    # Build retry queue from failed level-5 operations
    if retry_rows:
        retry_df = pd.DataFrame(retry_rows).drop_duplicates()
        retry_df.to_csv("data/zonal_stats_v11_retry_queue.csv", index=False)
        print(f"Retry queue written to zonal_stats_v11_retry_queue.csv with {len(retry_df)} items")

    if failure_count > 0:
        print(f"Failure log written to {failure_csv_path}")
    else:
        print("No failures")

if __name__ == "__main__":
    calculate()

# for January to December 2025 there are 86 time steps
# 38 total level 5 polygons over this time range takes 32m:54s

# for January to December 2024 there are 88 time steps
# 38 total level 5 polygons over this time range takes 35m:15s

# 31m:51s for January 2026 to now
# 2025 has 85 time steps