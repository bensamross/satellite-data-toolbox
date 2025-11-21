import json
import pystac_client
# import planetary_computer
import odc.stac

def fetch_resource_metadata():
    """
    Fetch resource metadata from a local JSON file and return the sensor information to be fed into the STAC catalog search API.
    
    Example: get_catalog_item(fetch_resource_metadata()['url'])

    """
    with open("resources.json", "r") as file:
        data = json.load(file)

        # Search for an item containing the name "Digital Earth Australia"
        result = next((item for item in data['stac_catalogs'] if "Digital Earth Australia" in item.get('name', '')), None)

        # if result:
        #     # Search for a sensor with the name "ga_ls8c_ard_3"
        #     sensor = next((sensor for sensor in result['sensors'] if sensor.get('name') == "ga_ls8c_ard_3"), None)
        #     return sensor
        
        return result

def get_catalog_item(url: str):
    catalog = pystac_client.Client.open(url)

    # catalog = pystac_client.Client.open('https://planetarycomputer.microsoft.com/api/stac/v1/', modifier=planetary_computer.sign_inplace)

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

def get_data_from_stac(url: str, sensor_name: list, sensor_bands: list, time_range: str):
    """
    Example: utilities.get_data_from_stac(url, sensor_name, bands, time_range="2020-01-01/2025-01-07")
    """
    catalog = pystac_client.Client.open(url)

    odc.stac.configure_rio(
        cloud_defaults=True,
        aws={"aws_unsigned": True},
    )

    bbox = [140.0, -38.0, 145.0, -35.0]  # [min_lon, min_lat, max_lon, max_lat]

    results = catalog.search(
        bbox=bbox,
        collections=[sensor_name],
        datetime=time_range,
    )

    # bands = ['nbart_blue', 'nbart_red', 'nbart_nir', 'nbart_swir_1', 'nbart_swir_2', 'nbart_green']
    data_chunks = {'time': 1, 'x': 4096, 'y': 4096}

    fetched_items = list(results.items())
    # len(items), items[0] if items else None

    data = odc.stac.stac_load(items=fetched_items, bands=sensor_bands, bbox=bbox, groupby='time', chunks=data_chunks)

    return data

def resample_stac_data_to_data_monthly(data):
    data_monthly = data.resample(time='1M').median()
    return data_monthly

def calculate_data_size_in_gb(data):
    size_gb = data.nbytes / (1024 ** 3)
    return size_gb

def calculate_ndvi(data, nir_band: str, red_band: str):
    # The general premise is that we'll calculate some band indexes AFTER the data is loaded into xarray. So for the most part we just fetch the raw band values and then dump them. Use this function to calculate NDVI from the loaded data if you seriously care about performance and data saving. But this risks not being able to calculate indexes on-the-fly later like SAVI, EVI, etc.
    ndvi = (data[nir_band] - data[red_band]) / (data[nir_band] + data[red_band])
    return ndvi