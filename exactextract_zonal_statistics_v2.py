import utilities
import exactextract
import geopandas as gpd
import numpy as np
import pandas as pd
from dask import delayed, compute
from dask.distributed import Client, LocalCluster
import time


def calculate():
    start_time = time.perf_counter()

    # Define the target area
    gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
    selecting_larger_polygons = gdf[gdf['level'] == 8].iloc[:2] # just pick the first two
    larger_polygon = selecting_larger_polygons.geometry.unary_union  # merge into a single geometry
    gdf = gdf[gdf.geometry.within(larger_polygon)] # select only the polygons within the larger one
    gdf = gdf[gdf['level'] == 10] # reduce again to just the level 10 polygons
    gdf = gdf[gdf['within0km'] == 1] # reduce again ton only those that intersect the plantation
    # gdf = gdf.iloc[:64] # reduce again to only the first 65 of that subset
    print(gdf.head(), gdf.shape)

    # Fetch STAC data
    # resource = utilities.load_resource("resources/dea-ga_s2bm_ard_3.yaml")
    # resource = utilities.load_resource("resources/pc-sentinel-2-l2a.yaml")
    resource = utilities.load_resource("resources/pc-landsat-c2-l2.yaml")

    url = resource["url"]
    sensor_name = resource["name"]
    bands = resource["bands"]
    print(f"Bands to be collected: {bands}")
    bounds = gdf.to_crs("EPSG: 4326").total_bounds.tolist()

    time_range = "2025-01-01/2025-12-31"
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

    # --- Dask cluster (local prototype; swap to your real cluster later) ---
    cluster = LocalCluster(n_workers=1, threads_per_worker=2)
    client = Client(cluster)
    print(client)

    n_polygon_batches = 8
    polygon_batches = [idx for idx in np.array_split(gdf.index.to_numpy(), n_polygon_batches) if len(idx) > 0]

    def run_one_batch(time_value, poly_idx):
        gdf_batch = gdf.loc[poly_idx, ["geometry", "GRID_ID"]]
        df = exactextract.exact_extract(
            rast=stac_data.sel(time=time_value)[bands],
            vec=gdf_batch,
            ops=["mean"],
            strategy="raster-sequential",
            output="pandas",
            include_cols=["GRID_ID"],
            progress=True
        )
        df["time"] = pd.to_datetime(time_value)
        return df
    
    # gdf = gdf.to_crs("EPSG: 32756") # sentinel 2
    gdf = gdf.to_crs("EPSG: 32656") # landsat 8

    tasks = []
    for t in stac_data.time.values:
        for batch_idx in polygon_batches:
            tasks.append(delayed(run_one_batch)(t, batch_idx))

    results = compute(*tasks)  # parallel execution across workers
    zonal_stats = pd.concat(results, ignore_index=True)

    zonal_stats.to_csv("zonal_stats_2025.csv", index=False)

    print(zonal_stats.head())
    print(f"Rows: {len(zonal_stats)}")

    elapsed = time.perf_counter() - start_time
    minutes, seconds = divmod(elapsed, 60)
    print(f"\nTotal runtime: {int(minutes)}m {seconds:.2f}s ({elapsed:.2f}s)")

if __name__ == "__main__":
    calculate()