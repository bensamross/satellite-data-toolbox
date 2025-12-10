import utilities
from tqdm.auto import tqdm
import shapely
import pandas as pd
import re
from pathlib import Path
import numpy as np
import rioxarray
import xarray as xr
import geopandas as gpd
from pystac_client import Client as StacClient
from odc.stac import stac_load
import argparse
import logging
import sys
import pathlib

def compute_zonal_stats_bands(
    data_monthly,
    gdf,
    key_column_name,
    bands,
    output_dir="outputs",
    overwrite=True,
    ):
    """
    High-throughput zonal stats writer (batch mode).

    Ensures consistent column order: key_column_name,time,<band>_mean,...
    """
    
    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Pre-build canonical stat column names (order stable)
    stat_columns = [f"{b}_mean" for b in bands]
    canonical_header = [key_column_name, "time"] + stat_columns

    print(f"Starting zonal statistics computation for {len(gdf)} features")
    print(f"Output directory: {output_dir}")
    print(f"Bands: {bands}")

    processed_count = 0
    error_count = 0
    no_data_count = 0

    for idx, feat in tqdm(gdf.iterrows(), total=len(gdf)):
        key_value = feat[key_column_name]
        if pd.isna(key_value):
            print(f"Feature {key_value} missing {key_column_name} - skipping")
            continue

        out_path = output_dir / f"BANDS_{key_value}.csv"
        
        # Check if file already exists (for resuming interrupted runs)
        # TODO: ~~Dependent on a parameter to enable/disable overwrite~~
        # TODO: If the file exists then check it for the specified time period and append missing data only
        if overwrite is False and out_path.exists():
            print(f"File already exists for {key_column_name} (row {idx}) - skipping")
            processed_count += 1
            continue

        geom = [shapely.geometry.mapping(feat.geometry)]

        try:
            clipped = data_monthly[bands].rio.clip(geom, gdf.crs.to_string(), drop=True)

            mean_ds = clipped.mean(dim=("y", "x"))
            df_poly = mean_ds.to_dataframe().reset_index()
            df_poly.rename(columns={b: f"{b}_mean" for b in bands}, inplace=True)

            # Add key_column_name first so ordering works
            df_poly[key_column_name] = key_value
            df_poly.sort_values("time", inplace=True)

            if df_poly.empty:
                print(f"No data found for {key_column_name} (row {idx})")
                no_data_count += 1
                continue

            # Reindex columns to canonical header
            # Ensure all stat columns present (fill missing with NaN if any)
            for col in canonical_header:
                if col not in df_poly.columns:
                    df_poly[col] = np.nan
            df_poly = df_poly[canonical_header]

            df_poly.to_csv(out_path, index=False, header=True, mode="w")
            processed_count += 1

        except Exception as e:
            # Check if it's a "no data in bounds" error
            if "No data found in bounds" in str(e):
                no_data_count += 1
                # Silently skip - this is expected for geometries outside data coverage
                continue
            else:
                error_count += 1
                print(f"Error processing {key_column_name} (row {idx}): {e}")
                continue

    print(f"Zonal statistics complete. Processed: {processed_count}, Errors: {error_count}, No data: {no_data_count}")

def compute_zonal_stats_bands_vectorized(
    data_monthly,
    gdf,
    key_column_name,
    bands,
    output_dir="outputs",
    overwrite=True
    ):
    """
    WARNING: This function is experimental.
    Vectorized zonal stats (time-first approach) - more efficient memory usage.
    
    Processes all features for each time step, reducing clipping overhead.
    """

    print("WARNING: This function is experimental.")
    logging.warning("Using experimental vectorized zonal stats function.")
    
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Pre-allocate storage for all features - ensure no NaN keys
    valid_keys = gdf[key_column_name].dropna().unique()
    
    # Check for existing files if overwrite is False
    if not overwrite:
        existing_keys = []
        for key in valid_keys:
            out_path = output_dir / f"BANDS_{key}.csv"
            if out_path.exists():
                existing_keys.append(key)
        
        if existing_keys:
            print(f"Skipping {len(existing_keys)} features with existing files (overwrite=False)")
            valid_keys = [k for k in valid_keys if k not in existing_keys]
    
    results_dict = {key: [] for key in valid_keys}
    
    print(f"Processing {len(data_monthly.time)} time steps for {len(valid_keys)} features")
    print(f"Bands: {bands}")
    
    processed_count = 0
    error_count = 0
    no_data_count = 0
    skipped_count = 0
    
    # Loop through time steps instead of features
    for time_idx, time_val in tqdm(enumerate(data_monthly.time.values), total=len(data_monthly.time)):
        try:
            # Get data for this single time step - ensure it's loaded
            data_slice = data_monthly.isel(time=time_idx)[bands].compute()
            
            # Compute stats for all features at once
            for idx, feat in gdf.iterrows():
                key_value = feat[key_column_name]
                
                if pd.isna(key_value):
                    continue
                
                # Skip if this key was already processed
                if key_value not in results_dict:
                    continue
                
                geom = [shapely.geometry.mapping(feat.geometry)]
                
                try:
                    # Clip this feature for this time step
                    clipped = data_slice.rio.clip(geom, gdf.crs.to_string(), drop=True)
                    
                    if clipped.sizes.get('x', 0) == 0 or clipped.sizes.get('y', 0) == 0:
                        continue
                    
                    # Compute mean for each band
                    stats = {}
                    for b in bands:
                        mean_val = clipped[b].mean().values
                        stats[f"{b}_mean"] = float(mean_val) if not np.isnan(mean_val) else None
                    
                    stats['time'] = pd.Timestamp(time_val)
                    
                    results_dict[key_value].append(stats)
                    processed_count += 1
                    
                except Exception as e:
                    # Check if it's a "no data in bounds" error
                    if "No data found in bounds" in str(e):
                        no_data_count += 1
                        # Silently skip - this is expected for geometries outside data coverage
                        continue
                    else:
                        error_count += 1
                        if error_count < 5:  # Print first few errors for debugging
                            print(f"Error on feature {key_value}, time {time_val}: {e}")
                        continue
        
        except Exception as e:
            print(f"Error processing time step {time_idx}: {e}")
            continue
    
    # Write results to CSV files
    for key_value, records in results_dict.items():
        if not records:
            continue
        
        df = pd.DataFrame(records)
        df[key_column_name] = key_value
        df = df[[key_column_name, 'time'] + [c for c in df.columns if c not in [key_column_name, 'time']]]
        df.sort_values('time', inplace=True)
        
        out_path = output_dir / f"BANDS_{key_value}.csv"
        df.to_csv(out_path, index=False)
    
    skipped_count = len(gdf[key_column_name].dropna().unique()) - len(valid_keys)
    print(f"Complete. Processed: {processed_count}, Errors: {error_count}, No data: {no_data_count}, Skipped: {skipped_count}")

'''
def collectAllForestsIteratively(gpkg_path, output_base_dir, start_from_forest=None):
    """
    Process zonal statistics for all unique FOREST values in the geopackage.
    
    Parameters
    ----------
    gpkg_path : str or Path
        Path to the geopackage file
    output_base_dir : str or Path
        Base directory where output folders will be created
    start_from_forest : str or int, optional
        Forest number to start processing from (for resuming interrupted runs)
    """
    # Set up general logger for all forests processing
    logger = utilities.setup_logger(output_base_dir, forest_name=None)
    
    logger.info("Starting processing for ALL forests")
    logger.info(f"Input geopackage: {gpkg_path}")
    logger.info(f"Output base directory: {output_base_dir}")
    
    gpkg_path = Path(gpkg_path)
    gdf = gpd.read_file(gpkg_path, layer='cptpoly_p')
    # gdf = gpd.read_file(gpkg_path, layer='bomgrid')
    
    if 'FOREST' not in gdf.columns:
        error_msg = "GeoDataFrame must contain a FOREST column."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    unique_forests = gdf['FOREST'].dropna().unique()
    unique_forests.sort()
    
    # Sort forests for consistent processing order
    try:
        # Try to sort as integers if possible
        unique_forests = sorted(unique_forests, key=lambda x: int(x))
        logger.info("Sorted forests numerically")
    except (ValueError, TypeError):
        # Fall back to string sorting if not all values are numeric
        unique_forests = sorted(unique_forests, key=str)
        logger.info("Sorted forests alphabetically")
    
    logger.info(f"Found {len(unique_forests)} unique FOREST values: {list(unique_forests)}")
    
    # Handle resume functionality
    if start_from_forest is not None:
        # Convert start_from_forest to same type as forest values for comparison
        if pd.api.types.is_numeric_dtype(gdf['FOREST']):
            try:
                start_from_forest = int(start_from_forest)
            except ValueError:
                logger.error(f"Cannot convert start_from_forest '{start_from_forest}' to integer")
                raise
        
        # Find the index to start from
        try:
            start_index = unique_forests.index(start_from_forest)
            unique_forests = unique_forests[start_index:]
            logger.info(f"Resuming from FOREST='{start_from_forest}' (skipping {start_index} forests)")
        except ValueError:
            logger.warning(f"FOREST='{start_from_forest}' not found in unique values. Starting from beginning.")

    successful_forests = []
    failed_forests = []
    skipped_forests = []

    for i, forest in enumerate(unique_forests, 1):
        # Check if forest has already been processed (directory exists with CSV files)
        forest_output_dir = Path(output_base_dir) / str(forest)
        if forest_output_dir.exists():
            csv_files = list(forest_output_dir.glob("BANDS_*.csv"))
            if csv_files:
                logger.info(f"FOREST='{forest}' already processed ({len(csv_files)} CSV files found) - skipping")
                skipped_forests.append(forest)
                continue
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing FOREST {i}/{len(unique_forests)}: '{forest}'")
        logger.info(f"{'='*50}")
        
        try:
            # Set up individual logger for this forest
            forest_logger = setup_logger(Path(output_base_dir) / str(forest), forest_name=forest)
            
            produceZonalStatisticsCSV(
                gpkg_path=gpkg_path,
                forest_name=forest,
                output_base_dir=output_base_dir,
                logger=forest_logger
            )
            successful_forests.append(forest)
            logger.info(f"Successfully completed FOREST='{forest}'")
            
        except Exception as e:
            failed_forests.append((forest, str(e)))
            logger.error(f"Error processing FOREST='{forest}': {e}")
            continue

    # Final summary
    logger.info(f"\n{'='*60}")
    logger.info("PROCESSING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total forests in list: {len(unique_forests)}")
    logger.info(f"Skipped (already processed): {len(skipped_forests)}")
    logger.info(f"Successfully processed: {len(successful_forests)}")
    logger.info(f"Failed: {len(failed_forests)}")
    
    if skipped_forests:
        logger.info(f"Skipped forests: {skipped_forests}")
    
    if successful_forests:
        logger.info(f"Successfully processed forests: {successful_forests}")
    
    if failed_forests:
        logger.info("Failed forests:")
        for forest, error in failed_forests:
            logger.info(f"  - {forest}: {error}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute zonal statistics for forest polygons from a geopackage.'
    )
    parser.add_argument(
        '--gpkg_path',
        type=str,
        required=True,
        help='Path to the geopackage file'
    )
    parser.add_argument(
        '--forest',
        type=str,
        required=True,
        help='Name of the forest to filter on (FOREST column), or "*" to process all forests'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='outputs',
        help='Base output directory (default: outputs)'
    )
    parser.add_argument(
        '--start_from',
        type=str,
        help='Forest number to start processing from (for resuming interrupted runs, only used with --forest "*")'
    )
    
    args = parser.parse_args()
    
    try:
        # Check if forest argument is wildcard for all forests
        if args.forest == '*':
            collectAllForestsIteratively(
                gpkg_path=args.gpkg_path,
                output_base_dir=args.output,
                start_from_forest=args.start_from
            )
        else:
            # Set up logger for single forest processing
            logger = setup_logger(Path(args.output) / str(args.forest), forest_name=args.forest)
            
            produceZonalStatisticsCSV(
                gpkg_path=args.gpkg_path,
                forest_name=args.forest,
                output_base_dir=args.output,
                logger=logger
            )
    except KeyboardInterrupt:
        print("\nScript interrupted by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
'''