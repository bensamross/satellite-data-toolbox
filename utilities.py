import json
import pystac_client
import odc.stac
import logging
import fiona
import yaml
import xarray as xr
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

    if "planetarycomputer.microsoft.com" in url:
        import planetary_computer
        catalog = pystac_client.Client.open(url, modifier=planetary_computer.sign_inplace)
    else:
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
    data_chunks = {'time': 1, 'x': 1024, 'y': 1024}

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

def get_layer_crs(gpkg_path: str, layer_name: str) -> str:
    with fiona.open(gpkg_path, layer=layer_name) as layer:
        crs = layer.crs
    return crs

def is_layer_row_count_large(row_count: int, threshold: int = 8192) -> bool:
    return row_count > threshold

# Establish a Dask cluster, typically this is handled automatically but you can define the cluster to have more control
def establishDaskCluster(logger=None):
    from dask.distributed import Client as DaskClient, LocalCluster
    import warnings
    warnings.filterwarnings('ignore')

    if logger is None:
        logger = logging.getLogger(__name__)
        
    logger.info("Establishing Dask cluster...")
    xr.set_options(keep_attrs=True)
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=4,
        memory_limit='8GB',  # Add a memory limit per worker
    )
    client = DaskClient(cluster)  # suppress logs
    logger.info(f"Dask cluster established: {client}")
    return client

def progress_monitor():
    # TODO: implement a progress monitor for long-running tasks and the ability to resume from last successful step
    pass

def check_for_missing_data():
    # TODO: implement a function to check for missing data in the output CSVs and reprocess only those areas
    pass