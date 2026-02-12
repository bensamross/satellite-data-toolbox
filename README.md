# raster-data-collection

The three primary sources of satellite data used in this code comes from Landsat, Sentinel-2, and Worldview. These are used because they are free and very accessible. All three tools have been bundled together here so that by specifying one area of interest you can extract data from these 3x datastores simultaneously.

For copernicus processing credits and quotas see the dashboard here: [https://shapps.dataspace.copernicus.eu/dashboard/#/](https://shapps.dataspace.copernicus.eu/dashboard/#/)

# To organise...
Links

- https://github.com/brazil-data-cube/code-gallery/blob/a9fe99d822f73b69db663f3c2fd60f8f2a4afd09/jupyter/Python/stac/stac-introduction.ipynb
- https://github.com/brazil-data-cube/code-gallery/blob/a9fe99d822f73b69db663f3c2fd60f8f2a4afd09/jupyter/Python/stac/stac-image-processing.ipynb
- https://github.com/brazil-data-cube/code-gallery/blob/master/jupyter/Python/stac/stac-aws-introduction.ipynb
- https://carpentries-incubator.github.io/geospatial-python/instructor/05-access-data.html
- https://medium.com/rotten-grapes/download-sentinel-data-within-seconds-in-python-8cc9a8c3e23c
- https://www.geopythontutorials.com/notebooks/xarray_zonal_stats.html
- https://github.com/aws-samples/aws-sentinel2-smsl-notebook/blob/main/geospatial_imagery_analysis.ipynb
- https://registry.opendata.aws/tag/agriculture/usage-examples/
- https://knowledge.dea.ga.gov.au/notebooks/How_to_guides/Downloading_data_with_STAC/
- https://www.streambatch.io/knowledge/ndvi-from-sentinel-2-imagery-using-stac
- https://colab.research.google.com/github/spatialthoughts/geopython-tutorials/blob/main/notebooks/xarray_zonal_stats.ipynb
- https://xarray-spatial.readthedocs.io/en/latest/user_guide/zonal.html
- https://github.com/opendatacube/odc-stac

# Sentinel Hub
The Sentinel Hub is a set of RESTful APIs that provide access to various satellite imagery archives. It allows you to access raw satellite data, rendered images, statistical analysis, and other features.

The Sentinel Hub has the ability to automatically archive, process in real-time, and distribute remote sensing data and related Earth Observation products.

By utilizing APIs, you can retrieve satellite data and process it for your specific area of interest and time range from the complete archives in just a few seconds.

## Processing API
The Processing API is the most frequently used API in Sentinel Hub. It generates images using satellite data for a user-specified area of interest, time range, processing, and visualization.

This API offers the easiest way to process satellite data, generate personalized visual representations of satellite images and maps using processed data, or download raw data.

[https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Process.html](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Process.html)

## Catalogue API
Sentinel Hub Catalogue is a powerful API for searching through satellite data archives. It implements the STAC Specification, which is a standardized method of describing geospatial information.

This tool can be used to explore the accessibility of any data collection that has been imported into Sentinel Hub.

[https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Catalog.html](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Catalog.html)

## Batch Processing API
Batch Processing API allows you to retrieve data for large regions and/or extended timeframes for any data collection supported by Sentinel Hub, including your own data. Rather than returning the data immediately in a request response, it delivers it to the object storage specified in the request.

[https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Batch.html](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Batch.html)

## Statistical API
The Statistical API allows you to obtain statistics that are calculated based on satellite imagery, without the requirement of downloading images. When you send a request to the Statistical API, you can define your area of interest, time frame, evalscript, and the statistical measures that need to be calculated. The result is a json file with the requested set of statistics for your area of interest.

[https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Statistical.html](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Statistical.html)

## Batch Statistical API
The Batch Statistical API allows you to request statistics for multiple polygons simultaneously and/or for longer aggregations, similar to the Statistical API. A typical scenario for using this API would be to calculate statistics for all parcels in a country.

In beta release

[https://documentation.dataspace.copernicus.eu/APIs.html](https://documentation.dataspace.copernicus.eu/APIs.html)

## Bring your own COG API
Bring Your Own COG API (or shortly "BYOC") enables you to import your own data in Sentinel Hub and access it just like any other data you are used to. In order to use it, you need a Copernicus Service account and the data have to be stored in a COG format on an S3 bucket.

BYOC API

[https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Byoc.html](https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Byoc.html)

## Sentinel Hub Services Dashboard
Your user dashboard allows you to overview your account information, processing requests and credits usage statistics. It also includes the Configuration Utility for creating service endpoints that you can use for OGC API requests and for embedding Copernicus Data in GIS software as a WMS/WMTS/WFS/WCS service. Under User Settings, it is possible to create and maintain OAuth clients for authentication within the Sentinel Hub API services.

[https://shapps.dataspace.copernicus.eu/dashboard/#/](https://shapps.dataspace.copernicus.eu/dashboard/#/)

## Requests Builder
Requests Builder is a user-friendly graphical interface designed for easy access to satellite imagery using the Sentinel Hub API system without handling commands, scripting language, and OAuth clients. It is a powerful tool for making API requests and the fastest way to get an image from Sentinel Hub APIs and also an efficient way to prepare requests that you can embed in your own code. Getting a response takes just a few clicks, and the resulting code is displayed for further use.

Please note that you have to login to access the requests builder interface.

[https://shapps.dataspace.copernicus.eu/requests-builder](https://shapps.dataspace.copernicus.eu/requests-builder)

## QGIS data integration
The Sentinel Hub QGIS plugin allows direct integration of imagery from the Copernicus Data Space Ecosystem into your desktop QGIS software, powered by the Sentinel Hub API system.

The plugin handles search by date and location, filtering by cloud cover and time range mosaicking. A wide range of visualizations is offered by default, and custom configurations are also supported. Direct imagery download from QGIS is also available. The QGIS plugin provides most of the functionality of the Copernicus Browser directly within open-source GIS software.

[https://plugins.qgis.org/plugins/SentinelHub/](https://plugins.qgis.org/plugins/SentinelHub/)

## How to establish a persistent dask processing cluster

In one terminal window type `dask scheduler`

And in another window type `dask worker tcp://127.0.0.1:8786 --nworkers 2 --nthreads 2 --memory-limit 2GB`

Then connect to the scheduler with the following
```python
scheduler_address = "tcp://10.226.68.22:8786"
client = DaskClient(scheduler_address)
```

instead of
```python
def establishDaskCluster():
    warnings.filterwarnings('ignore')
    xr.set_options(keep_attrs=True)
    cluster = LocalCluster(
        n_workers=2,
        threads_per_worker=2,
        memory_limit='2GB',  # Add a memory limit per worker
    )
    client = DaskClient(cluster)  # suppress logs
    return client

# Establish Dask client
client = establishDaskCluster()
```

### Index calculations
```python
def ndvi(nir: np.ndarray, red: np.ndarray) -> np.ndarray:
    """
    Calculate Normalized Difference Vegetation Index (NDVI).
    NDVI = (NIR - Red) / (NIR + Red)
    """
    return (nir - red) / (nir + red + 1e-10)  # small epsilon avoids division by zero

def evi(nir: np.ndarray, red: np.ndarray, blue: np.ndarray,
        G: float = 2.5, C1: float = 6.0, C2: float = 7.5, L: float = 1.0) -> np.ndarray:
    """
    Calculate Enhanced Vegetation Index (EVI).
    EVI = G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L)
    """
    return G * ((nir - red) / (nir + (C1 * red) - (C2 * blue) + L + 1e-10))
```

Default constants:
G = 2.5
L = 1
C₁ = 6
C₂ = 7.5

## The new plan

## How to run the python scripts
`python zonalStatistics.py --gpkg_path *.gpkg --forest 283 --output outputs/`

`python zonalStatistics.py --gpkg_path *.gpkg --forest * --output outputs/ --start_from 16`

`python hexagons.py` 

## Notes
> To pull sentinel-1 data from the 24/11/2025 storm event:
    - item_url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-1-rtc/items/S1A_IW_GRDH_1SDV_20251122T191330_20251122T191355_061994_07C133_rtc" (for fraser coast before)
    - item_url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-1-rtc/items/S1A_IW_GRDH_1SDV_20251122T191355_20251122T191420_061994_07C133_rtc" (for beerburrum before)
> To pull sentinel-1 data from the 21/11/2025 storm event:
    - item_url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2B_MSIL2A_20251124T000219_R030_T56JMS_20251124T013222" (for fraser coast before)
    - item_url = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/sentinel-2-l2a/items/S2B_MSIL2A_20251124T000219_R030_T56JMR_20251124T013222" (for beerburrum before)
> Include cloud mask information in the metadata so we can exclude clouded areas from the mean/median monthly summary which might skew the results