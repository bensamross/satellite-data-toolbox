from tqdm.auto import tqdm
from shapely.geometry import mapping as shp_mapping
import pandas as pd
import re
from pathlib import Path
import numpy as np
import rioxarray
import xarray as xr
# from dask.distributed import Client as DaskClient, LocalCluster
# import warnings
import geopandas as gpd
from pathlib import Path
from pystac_client import Client as StacClient
import planetary_computer
from odc.stac import stac_load
import argparse
import logging
import sys
from datetime import datetime

def setup_logger(output_dir, forest_name=None):
    """
    Set up logging configuration to track script progress.
    
    Parameters
    ----------
    output_dir : str or Path
        Directory where log files will be saved
    forest_name : str, optional
        Forest name for specific logging, or None for general logging
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if forest_name:
        log_filename = f"zonal_stats_{forest_name}_{timestamp}.log"
    else:
        log_filename = f"zonal_stats_all_forests_{timestamp}.log"
    
    log_path = output_dir / log_filename
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)  # Also log to console
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_path}")
    
    return logger

def add(x, y):
    return x + y

def compute_zonal_stats_bands(
    data_monthly,
    gdf,
    bands,
    aoi_crs,
    output_dir="outputs",
    verbose=False,
    logger=None
    ):
    """
    High-throughput zonal stats writer (batch mode).

    Ensures consistent column order: unit_key,time,<band>_mean,...
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    unit_col_candidates = [c for c in gdf.columns if c.lower() == "unit_key"]
    if not unit_col_candidates:
        raise ValueError("GeoDataFrame must contain a UNIT_KEY column.")
    unit_col = unit_col_candidates[0]

    # Pre-build canonical stat column names (order stable)
    stat_columns = [f"{b}_mean" for b in bands]
    canonical_header = ["unit_key", "time"] + stat_columns

    logger.info(f"Starting zonal statistics computation for {len(gdf)} features")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Bands: {bands}")
    
    processed_count = 0
    error_count = 0
    no_data_count = 0

    for idx, feat in tqdm(gdf.iterrows(), total=len(gdf)):
        unit_key = feat[unit_col]
        if pd.isna(unit_key):
            logger.warning(f"Feature {idx} missing UNIT_KEY - skipping")
            continue

        out_path = output_dir / f"BANDS_row_{idx}.csv"
        
        # Check if file already exists (for resuming interrupted runs)
        if out_path.exists():
            logger.info(f"File already exists for {unit_key} (row {idx}) - skipping")
            processed_count += 1
            continue

        geom = [shp_mapping(feat.geometry)]

        try:
            clipped = data_monthly[bands].rio.clip(geom, aoi_crs, drop=True)

            mean_ds = clipped.mean(dim=("y", "x"))
            df_poly = mean_ds.to_dataframe().reset_index()
            df_poly.rename(columns={b: f"{b}_mean" for b in bands}, inplace=True)

            # Add unit_key first so ordering works
            df_poly["unit_key"] = unit_key
            df_poly.sort_values("time", inplace=True)

            if df_poly.empty:
                logger.warning(f"No data found for {unit_key} (row {idx})")
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
            
            if verbose or processed_count % 50 == 0:  # Log every 50 processed
                logger.info(f"Processed {unit_key} (row {idx}): {len(df_poly)} time records -> {out_path}")

        except Exception as e:
            error_count += 1
            logger.error(f"Error processing {unit_key} (row {idx}): {e}")
            continue

    logger.info(f"Zonal statistics complete. Processed: {processed_count}, Errors: {error_count}, No data: {no_data_count}")

# Establish a Dask cluster
# def establishDaskCluster(logger=None):
#     if logger is None:
#         logger = logging.getLogger(__name__)
        
#     logger.info("Establishing Dask cluster...")
#     warnings.filterwarnings('ignore')
#     xr.set_options(keep_attrs=True)
#     cluster = LocalCluster(
#         n_workers=4,
#         threads_per_worker=4,
#         memory_limit='2GB',  # Add a memory limit per worker
#     )
#     client = DaskClient(cluster)  # suppress logs
#     logger.info(f"Dask cluster established: {client}")
#     return client

def produceZonalStatisticsCSV(gpkg_path, forest_name, output_base_dir, logger=None):
    """
    Process zonal statistics for a specific FOREST from a geopackage.
    
    Parameters
    ----------
    gpkg_path : str or Path
        Path to the geopackage file
    forest_name : str or int
        Name/ID of the forest to filter on (FOREST column)
    output_base_dir : str or Path
        Base directory where output folders will be created
    logger : logging.Logger, optional
        Logger instance for tracking progress
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    logger.info(f"Starting processing for FOREST='{forest_name}'")
    
    # Read geopackage layer with FOREST filter
    gpkg_path = Path(gpkg_path)
    logger.info(f"Reading geopackage: {gpkg_path}")
    
    gdf = gpd.read_file(gpkg_path, layer='cptpoly_p')
    # gdf = gpd.read_file(gpkg_path, layer='bomgrid')
    logger.info(f"Loaded {len(gdf)} total features from geopackage")
    
    # Filter by FOREST column
    if 'FOREST' not in gdf.columns:
        error_msg = "GeoDataFrame must contain a FOREST column."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Convert forest_name to int if the FOREST column is numeric
    if pd.api.types.is_numeric_dtype(gdf['FOREST']):
        try:
            forest_name = int(forest_name)
            logger.info(f"Converted forest name to integer: {forest_name}")
        except ValueError:
            error_msg = f"FOREST column is numeric but '{forest_name}' cannot be converted to integer"
            logger.error(error_msg)
            print(error_msg)
            return
    
    gdf = gdf[gdf['FOREST'] == forest_name].copy()
    
    if len(gdf) == 0:
        warning_msg = f"No features found for FOREST='{forest_name}'"
        logger.warning(warning_msg)
        print(warning_msg)
        return
    
    aoi_crs = gdf.crs or 'EPSG:4326'
    logger.info(f"Filtered to {len(gdf)} features for FOREST='{forest_name}' | CRS: {gdf.crs}")

    # Calculate the     ounding box for the STAC search
    bbox = gdf.dissolve().total_bounds.tolist()
    logger.info(f"Bounding box: {bbox}")

    # Use Digital Earth Australia STAC catalog
    logger.info("Connecting to Digital Earth Australia STAC catalog...")
    catalog = StacClient.open("https://explorer.dea.ga.gov.au/stac")
    import odc.stac
    odc.stac.configure_rio(
        cloud_defaults=True,
        aws={"aws_unsigned": True},
    )

    # Build a query with the parameters above
    logger.info("Searching for Landsat-8 datasets...")
    results = catalog.search(
        bbox=bbox,
        collections=["ga_ls8c_ard_3"],
        datetime="2015-01-01/2025-06-30",
    )

    items = list(results.items())
    logger.info(f"Found {len(items)} Landsat-8 datasets")

    # Enhanced bands list for comprehensive plant health monitoring
    bands = ['nbart_blue', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'nbart_green']
    chunks = {'time': 1, 'x': 4096, 'y': 4096}
    
    logger.info(f"Loading data with bands: {bands}")
    data = stac_load(items=items, bands=bands, bbox=bbox, groupby='time', chunks=chunks)

    # aggregate to monthly using mean to reduce memory pressure
    logger.info("Aggregating data to monthly means...")
    data_monthly = data.resample(time='ME').mean()

    # First, let's update the data loading to include more bands
    logger.info(f"Data loaded successfully. Bands in dataset: {list(data_monthly.data_vars)}")

    # Create output directory based on forest name
    output_dir = Path(output_base_dir) / str(forest_name)
    logger.info(f"Output directory: {output_dir}")
    
    # Use the function to extract band values
    logger.info("Starting zonal statistics computation...")
    compute_zonal_stats_bands(
        data_monthly=data_monthly,
        gdf=gdf,
        bands=bands,
        aoi_crs=gdf.crs,
        output_dir=str(output_dir),
        verbose=False,
        logger=logger
    )
    
    logger.info(f"Completed processing for FOREST='{forest_name}'")

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
    logger = setup_logger(output_base_dir, forest_name=None)
    
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