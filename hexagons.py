import geopandas as gpd
import utilities
import zonal_statistics
# import combineCSV

def main():
    gdf = gpd.read_file('./data/inputs/h3.gpkg', layer='h3_elliott_river')
    gdf.to_crs('EPSG:4326', inplace=True)

    resource = utilities.load_resource("./resources/dea-ga_s2bm_ard_3.yaml")

    url = resource["url"]
    sensor_name = resource["name"]
    bands = resource["bands"]
    print(f"Bands to be collected: {bands}")

    bounds = gdf.to_crs('EPSG:4326').total_bounds.tolist()

    time_range = "2017-01-01/2025-12-31"
    data = utilities.get_data_from_stac(
        url=url,
        bounds=bounds,
        sensor_name=sensor_name,
        sensor_bands=bands,
        time_range=time_range)
    print(f"Time range {time_range}")

    data_monthly = utilities.resample_stac_data_to_data_monthly(data)
    print(f"Raw data size {utilities.calculate_data_size_in_gb(data):.2g} GB, resampled to monthly size {utilities.calculate_data_size_in_gb(data_monthly):.2g} GB")

    if utilities.calculate_data_size_in_gb(data_monthly) > 8:
        proceed = input(f"Data size is large ({utilities.calculate_data_size_in_gb(data_monthly):.2g} GB). Proceed with zonal statistics calculation? (y/n): ")
        if proceed.lower() != 'y':
            print("Exiting.")
            return

    ################################
    # Establish a Dask cluster for parallel processing
    dask_client = utilities.establishDaskCluster()

    level10_gdf = gdf[gdf['level'] == 10] # process a smaller subset for a trial run

    zonal_statistics.compute_zonal_stats_bands_vectorized(
        data_monthly=data_monthly,
        gdf=level10_gdf,
        key_column_name='GRID_ID',
        bands=bands,
        output_dir="./data/outputs/hexagons_elliott_river",
        overwrite=True,
        use_dask=True,
        dask_client=dask_client,
        batch_size=256,  # Adjust based on your memory availability
    )

    # zonal_statistics.compute_zonal_stats_bands(
    #     data_monthly=data_monthly,
    #     gdf=level10_gdf,
    #     key_column_name='GRID_ID',
    #     bands=bands,
    #     output_dir="./data/outputs/hexagons_elliott_river_test",
    #     overwrite=True,
    #     use_dask=True,
    #     dask_client=dask_client,
    #     batch_size=50,  # Adjust based on your memory availability
    # )

    # Clean up when done
    dask_client.close()

    # combineCSV.compile_csvs(
    #     output_dir="./outputs/hexagons_elliott_river",
    #     pattern="BANDS*.csv",
    #     combined_filename="combined.csv",
    #     key_column_name='GRID_ID',
    #     recursive=False,
    #     verbose=False)

    ################################

if __name__ == '__main__':
    main()