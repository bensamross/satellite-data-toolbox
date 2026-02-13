import json
import pystac_client
import odc.stac
import logging
import fiona
import yaml
from pathlib import Path

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
    
    from datetime import datetime
    import sys
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if forest_name:
        log_filename = f"zonal_stats_{forest_name}_{timestamp}.log"
    else:
        log_filename = f"zonal_stats_all_forests_{timestamp}.log"
    
    log_path = output_dir / log_filename
    
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

def load_resource(resource_file: str) -> dict:
    """
    Load a single resource YAML file and return its contents.
    
    Parameters
    ----------
    resource_file : str
        Path to a resource YAML file (e.g., './resources/dea-ga_s2bm_ard_3.yaml')
    
    Returns
    -------
    dict
        Dictionary with keys: url, name, common_name, bands
    """
    resource_path = Path(resource_file)
    if not resource_path.exists():
        raise FileNotFoundError(f"Resource file not found: {resource_file}")
    
    with resource_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Keep for backwards compatibility
def fetch_resource_metadata(resources_file: str = "resources.yaml"):
    """
    Deprecated: Use load_resource() instead for the new single-file format.
    """
    resources_path = Path(resources_file)
    if resources_path.suffix in (".yaml", ".yml"):
        with resources_path.open("r") as file:
            data = yaml.safe_load(file)
    else:
        with resources_path.open("r") as file:
            data = json.load(file)
    
    return data

def get_catalog_item(url: str):
    """
    Returns a STAC catalog item from the given URL. Modifies the request if the URL is for Planetary Computer because it requires signing.

    Parameters
    ----------
    url : str
        STAC API URL
    
    Returns
    -------
    pystac_client.Client
        STAC catalog client
    """
    if "planetarycomputer.microsoft.com" in url:
        import planetary_computer # typically planetary computer is slow compared to Digital Earth Australia
        catalog = pystac_client.Client.open(url, modifier=planetary_computer.sign_inplace)
    else:
        catalog = pystac_client.Client.open(url)

    # odc.stac.configure_rio(
    #     cloud_defaults=True,
    #     aws={"aws_unsigned": True},
    # )
    
    return catalog

def list_asset_values(catalog, collections: list[str]) -> list[str]:
    results = catalog.search(
        collections=collections,
        max_items=1)
    
    items = results.item_collection()
    return list(items[0].assets.values())

def get_string_for_relative_date(num_days_in_past: int):
    # TODO: implement a function to get data relative to today's date, so when this function is run on an automated schedule it always fetches the most recent data
    # TODO: perhaps this can be replaced with the ./progress_tracker.py functions later on
    
    # start = datetime.now() - timedelta(days=num_days_in_past)
    # end = datetime.now()

    # return f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
    # eg return "2020-01-01/2025-01-07"
    return None

def get_data_from_stac(url: str, bounds: list, sensor_name: list, sensor_bands: list, time_range: str):
    """
    Example: utilities.get_data_from_stac(url, sensor_name, bands, time_range="2020-01-01/2025-01-07")
    """
    catalog = pystac_client.Client.open(url)

    odc.stac.configure_rio(
        cloud_defaults=True,
        aws={"aws_unsigned": True},
    )

    # bbox = [140.0, -38.0, 145.0, -35.0]  # [min_lon, min_lat, max_lon, max_lat]

    results = catalog.search(
        bbox=bounds,
        collections=[sensor_name],
        datetime=time_range,
    )

    # bands = ['nbart_blue', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'nbart_green']
    data_chunks = {'time': 1, 'x': 4096, 'y': 4096} # if the computer can handle it then typically larger chunks for the dask system is better because there will be less loading to and from memory

    fetched_items = list(results.items())
    # len(items), items[0] if items else None

    data = odc.stac.stac_load(items=fetched_items, bands=sensor_bands, bbox=bounds, groupby='time', chunks=data_chunks)

    return data

def resample_stac_data_to_data_monthly(data):
    """
    Take the raw data from the STAC API and resample to monthly frequency using median aggregation.

    Parameters
    ----------
    data : xarray.Dataset
        STAC data with time dimension

    Returns
    -------
    xarray.Dataset
        Dataset resampled to monthly frequency
    """
    data_monthly = data.resample(time='1ME').median()
    return data_monthly

def resample_stac_data_to_least_cloudy_monthly(data, cloud_band: str):
    """
    Select the least cloudy image for each month instead of aggregating.
    
    Parameters
    ----------
    data : xarray.Dataset
        STAC data with time dimension
    cloud_band : str, optional
        Name of cloud quality band (e.g., 'fmask', 'qa_pixel'). 
        If None, uses simple approach based on data availability.
    
    Returns
    -------
    xarray.Dataset
        Dataset with one image per month (least cloudy)
    """
    def select_least_cloudy(group):
        if cloud_band and cloud_band in group.data_vars:
            # Lower cloud values = clearer image
            cloud_counts = group[cloud_band].sum(dim=['x', 'y'])
            least_cloudy_idx = cloud_counts.argmin().item()
        else:
            # Fallback: select image with highest valid data (fewest NaNs)
            valid_count = group.count(dim=['x', 'y']).to_array().sum(dim='variable')
            least_cloudy_idx = valid_count.argmax().item()
        
        return group.isel(time=least_cloudy_idx)
    
    data_least_cloudy = data.groupby('time.month').map_blocks(
        select_least_cloudy, 
        dtype=data.dtype
    )
    
    return data_least_cloudy

def calculate_data_size_in_gb(data):
    size_gb = data.nbytes / (1024 ** 3)
    return size_gb

def calculate_ndvi(data, nir_band: str, red_band: str):
    # The general premise is that we'll calculate some band indexes AFTER the data is loaded into xarray. So for the most part we just fetch the raw band values and then dump them. Use this function to calculate NDVI from the loaded data if you seriously care about performance and data saving. But this risks not being able to calculate indexes on-the-fly later like SAVI, EVI, etc.
    ndvi = (data[nir_band] - data[red_band]) / (data[nir_band] + data[red_band])
    return ndvi

def list_all_layers_in_geopackage(gpkg_path: str) -> list[str]:
    layers = fiona.listlayers(gpkg_path)
    return layers

def list_fields_in_layer(gpkg_path: str, layer_name: str) -> list[str]:
    with fiona.open(gpkg_path, layer=layer_name) as layer:
        fields = list(layer.schema['properties'].keys())
    return fields

def list_rows_in_layer(gpkg_path: str, layer_name: str) -> int:
    with fiona.open(gpkg_path, layer=layer_name) as layer:
        row_count = len(layer)
    return row_count

def is_layer_row_count_large(row_count: int, threshold: int = 8192) -> bool:
    return row_count > threshold

# Establish a Dask cluster, typically this is handled automatically but you can define the cluster to have more control
def establishDaskCluster(logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)
        
    logger.info("Establishing Dask cluster...")
    warnings.filterwarnings('ignore')
    xr.set_options(keep_attrs=True)
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=4,
        memory_limit='2GB',  # Add a memory limit per worker
    )
    client = DaskClient(cluster)  # suppress logs
    logger.info(f"Dask cluster established: {client}")
    return client

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

def progress_monitor():
    # TODO: implement a progress monitor for long-running tasks and the ability to resume from last successful step
    pass

def check_for_missing_data():
    # TODO: implement a function to check for missing data in the output CSVs and reprocess only those areas
    pass