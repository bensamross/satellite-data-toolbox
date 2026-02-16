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
import dask
from dask import delayed


def _process_single_feature(
    data_monthly,
    feat_geometry,
    feat_key_value,
    crs_string,
    bands,
    canonical_header,
    key_column_name,
    out_path,
):
    """
    Process a single feature's zonal statistics. Designed to be called
    directly or via dask.delayed.

    Returns a dict with status info: {"status": "ok"|"no_data"|"error", ...}
    """
    geom = [shapely.geometry.mapping(feat_geometry)]

    try:
        clipped = data_monthly[bands].rio.clip(geom, crs_string, drop=True)

        mean_ds = clipped.mean(dim=("y", "x"))
        df_poly = mean_ds.to_dataframe().reset_index()
        df_poly.rename(columns={b: f"{b}_mean" for b in bands}, inplace=True)

        df_poly[key_column_name] = feat_key_value
        df_poly.sort_values("time", inplace=True)

        if df_poly.empty:
            return {"status": "no_data", "key": feat_key_value}

        for col in canonical_header:
            if col not in df_poly.columns:
                df_poly[col] = np.nan
        df_poly = df_poly[canonical_header]

        df_poly.to_csv(out_path, index=False, header=True, mode="w")
        return {"status": "ok", "key": feat_key_value}

    except Exception as e:
        if "No data found in bounds" in str(e):
            return {"status": "no_data", "key": feat_key_value}
        else:
            return {"status": "error", "key": feat_key_value, "error": str(e)}


def compute_zonal_stats_bands(
    data_monthly,
    gdf,
    key_column_name,
    bands,
    output_dir="outputs",
    overwrite=True,
    use_dask=False,
    dask_client=None,
    batch_size=50,
):
    """
    High-throughput zonal stats writer (batch mode).

    Ensures consistent column order: key_column_name,time,<band>_mean,...

    Parameters
    ----------
    data_monthly : xarray.Dataset
        Monthly resampled STAC data.
    gdf : GeoDataFrame
        Polygons to compute zonal stats for.
    key_column_name : str
        Column name used as the unique identifier for each feature.
    bands : list of str
        Band names to compute statistics for.
    output_dir : str or Path
        Directory to write CSV files to.
    overwrite : bool
        If False, skip features that already have a CSV on disk.
    use_dask : bool
        If True, use dask.delayed for parallel processing.
    dask_client : dask.distributed.Client, optional
        An existing Dask client. If use_dask=True and this is None,
        tasks run with the default threaded scheduler.
    batch_size : int
        Number of features to submit per batch when using Dask.
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
    if use_dask:
        print(f"Dask parallel processing enabled (batch_size={batch_size})")

    crs_string = gdf.crs.to_string()

    processed_count = 0
    error_count = 0
    no_data_count = 0
    skipped_count = 0

    # Build list of features to process
    tasks = []
    for idx, feat in gdf.iterrows():
        key_value = feat[key_column_name]
        if pd.isna(key_value):
            print(f"Feature at row {idx} missing {key_column_name} - skipping")
            skipped_count += 1
            continue

        out_path = output_dir / f"BANDS_{key_value}.csv"

        if overwrite is False and out_path.exists():
            processed_count += 1
            continue

        tasks.append((feat.geometry, key_value, str(out_path)))

    if not use_dask:
        # --- Sequential mode (original behaviour) ---
        for geometry, key_value, out_path_str in tqdm(tasks, desc="Processing features"):
            result = _process_single_feature(
                data_monthly=data_monthly,
                feat_geometry=geometry,
                feat_key_value=key_value,
                crs_string=crs_string,
                bands=bands,
                canonical_header=canonical_header,
                key_column_name=key_column_name,
                out_path=out_path_str,
            )
            if result["status"] == "ok":
                processed_count += 1
            elif result["status"] == "no_data":
                no_data_count += 1
            else:
                error_count += 1
                print(f"Error processing {key_column_name}={result['key']}: {result.get('error', 'unknown')}")
    else:
        # --- Dask parallel mode ---
        # Process in batches to control memory and provide progress feedback
        total_batches = (len(tasks) + batch_size - 1) // batch_size

        for batch_idx in tqdm(range(total_batches), desc="Processing batches"):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, len(tasks))
            batch = tasks[batch_start:batch_end]

            delayed_results = []
            for geometry, key_value, out_path_str in batch:
                d = delayed(_process_single_feature)(
                    data_monthly=data_monthly,
                    feat_geometry=geometry,
                    feat_key_value=key_value,
                    crs_string=crs_string,
                    bands=bands,
                    canonical_header=canonical_header,
                    key_column_name=key_column_name,
                    out_path=out_path_str,
                )
                delayed_results.append(d)

            # Compute the batch — uses dask_client if available, else default scheduler
            if dask_client is not None:
                futures = dask_client.compute(delayed_results)
                results = dask_client.gather(futures)
            else:
                results = dask.compute(*delayed_results)

            for result in results:
                if result["status"] == "ok":
                    processed_count += 1
                elif result["status"] == "no_data":
                    no_data_count += 1
                else:
                    error_count += 1
                    print(f"Error processing {key_column_name}={result['key']}: {result.get('error', 'unknown')}")

    print(f"Zonal statistics complete. Processed: {processed_count}, Errors: {error_count}, No data: {no_data_count}, Skipped: {skipped_count}")


def _process_single_feature_vectorized(
    data_slice,
    feat_geometry,
    feat_key_value,
    crs_string,
    bands,
    canonical_header,
    key_column_name,
    time_val,
    out_path,
):
    """
    Process a single feature for a single time step. Appends one row to its CSV.
    Designed to be called directly or via dask.delayed.
    """
    geom = [shapely.geometry.mapping(feat_geometry)]

    try:
        clipped = data_slice.rio.clip(geom, crs_string, drop=True)

        if clipped.sizes.get('x', 0) == 0 or clipped.sizes.get('y', 0) == 0:
            return {"status": "no_data", "key": feat_key_value}

        row = {key_column_name: feat_key_value, 'time': pd.Timestamp(time_val)}
        for b in bands:
            mean_val = clipped[b].mean().values
            row[f"{b}_mean"] = float(mean_val) if not np.isnan(mean_val) else None

        row_df = pd.DataFrame([row])[canonical_header]
        row_df.to_csv(out_path, index=False, header=False, mode="a")

        return {"status": "ok", "key": feat_key_value}

    except Exception as e:
        if "No data found in bounds" in str(e):
            return {"status": "no_data", "key": feat_key_value}
        else:
            return {"status": "error", "key": feat_key_value, "error": str(e)}


def compute_zonal_stats_bands_vectorized(
    data_monthly,
    gdf,
    key_column_name,
    bands,
    output_dir="outputs",
    overwrite=True,
    use_dask=False,
    dask_client=None,
    batch_size=50,
):
    """
    WARNING: This function is experimental.
    Vectorized zonal stats (time-first approach) - more efficient memory usage.

    Processes all features for each time step, writing results to CSV
    incrementally to reduce memory usage and protect against crashes.

    Parameters
    ----------
    data_monthly : xarray.Dataset
        Monthly resampled STAC data.
    gdf : GeoDataFrame
        Polygons to compute zonal stats for.
    key_column_name : str
        Column name used as the unique identifier for each feature.
    bands : list of str
        Band names to compute statistics for.
    output_dir : str or Path
        Directory to write CSV files to.
    overwrite : bool
        If False, skip features that already have a CSV on disk.
    use_dask : bool
        If True, use dask.delayed for parallel processing of features
        within each time step.
    dask_client : dask.distributed.Client, optional
        An existing Dask client. If use_dask=True and this is None,
        tasks run with the default threaded scheduler.
    batch_size : int
        Number of features to submit per batch when using Dask.
    """

    print("WARNING: This function is experimental.")
    logging.warning("Using experimental vectorized zonal stats function.")

    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Pre-build canonical stat column names (order stable)
    stat_columns = [f"{b}_mean" for b in bands]
    canonical_header = [key_column_name, "time"] + stat_columns

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

    # Write CSV headers for all valid keys upfront (overwrite mode)
    csv_paths = {}
    for key in valid_keys:
        out_path = output_dir / f"BANDS_{key}.csv"
        csv_paths[key] = out_path
        pd.DataFrame(columns=canonical_header).to_csv(out_path, index=False, mode="w")

    crs_string = gdf.crs.to_string()

    print(f"Processing {len(data_monthly.time)} time steps for {len(valid_keys)} features")
    if use_dask:
        print(f"Dask parallel processing enabled (batch_size={batch_size})")

    processed_count = 0
    error_count = 0
    no_data_count = 0
    skipped_count = 0

    # Build feature list once (avoids rebuilding every time step)
    feature_list = []
    for idx, feat in gdf.iterrows():
        key_value = feat[key_column_name]
        if pd.isna(key_value) or key_value not in csv_paths:
            continue
        feature_list.append((feat.geometry, key_value, str(csv_paths[key_value])))

    # Loop through time steps
    for time_idx, time_val in tqdm(
        enumerate(data_monthly.time.values),
        total=len(data_monthly.time),
        desc="Processing time steps",
    ):
        try:
            data_slice = data_monthly.isel(time=time_idx)[bands].compute()
        except Exception as e:
            print(f"Error loading time step {time_idx}: {e}")
            continue

        if not use_dask:
            # --- Sequential mode ---
            for geometry, key_value, out_path_str in feature_list:
                result = _process_single_feature_vectorized(
                    data_slice=data_slice,
                    feat_geometry=geometry,
                    feat_key_value=key_value,
                    crs_string=crs_string,
                    bands=bands,
                    canonical_header=canonical_header,
                    key_column_name=key_column_name,
                    time_val=time_val,
                    out_path=out_path_str,
                )
                if result["status"] == "ok":
                    processed_count += 1
                elif result["status"] == "no_data":
                    no_data_count += 1
                else:
                    error_count += 1
                    if error_count < 5:
                        print(f"Error on feature {result['key']}, time {time_val}: {result.get('error')}")
        else:
            # --- Dask parallel mode (batched within each time step) ---
            total_batches = (len(feature_list) + batch_size - 1) // batch_size

            for batch_idx in range(total_batches):
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, len(feature_list))
                batch = feature_list[batch_start:batch_end]

                delayed_results = []
                for geometry, key_value, out_path_str in batch:
                    d = delayed(_process_single_feature_vectorized)(
                        data_slice=data_slice,
                        feat_geometry=geometry,
                        feat_key_value=key_value,
                        crs_string=crs_string,
                        bands=bands,
                        canonical_header=canonical_header,
                        key_column_name=key_column_name,
                        time_val=time_val,
                        out_path=out_path_str,
                    )
                    delayed_results.append(d)

                if dask_client is not None:
                    futures = dask_client.compute(delayed_results)
                    results = dask_client.gather(futures)
                else:
                    results = dask.compute(*delayed_results)

                for result in results:
                    if result["status"] == "ok":
                        processed_count += 1
                    elif result["status"] == "no_data":
                        no_data_count += 1
                    else:
                        error_count += 1
                        if error_count < 5:
                            print(f"Error on feature {result['key']}, time {time_val}: {result.get('error')}")

    # Remove empty CSV files (header-only, no data rows)
    empty_count = 0
    for key_value, out_path in csv_paths.items():
        df_check = pd.read_csv(out_path)
        if df_check.empty:
            out_path.unlink()
            empty_count += 1

    skipped_count = len(gdf[key_column_name].dropna().unique()) - len(valid_keys)
    print(
        f"Complete. Processed: {processed_count}, Errors: {error_count}, "
        f"No data: {no_data_count}, Skipped: {skipped_count}, Empty (removed): {empty_count}"
    )