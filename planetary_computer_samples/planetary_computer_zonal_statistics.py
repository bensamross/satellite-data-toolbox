import numpy as np

import planetary_computer
import pystac_client
import stackstac
import rasterio.features

import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from xrspatial.multispectral import true_color

from xrspatial.classify import equal_interval
from xrspatial.zonal import stats as zonal_stats
from xrspatial.zonal import crosstab as zonal_crosstab
import odc.stac

# from dask.distributed import Client
# client = Client()


catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
    modifier=planetary_computer.sign_inplace,
)

bounds = (-98.00080760573508, 32.99921674609716, -96.9991860639418, 34.000729644613706)

sentinel_search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=bounds,
    datetime="2020-07-01/2020-07-07",
    query={
        "eo:cloud_cover": {
            "lt": 10,
        }
    },
)

# sentinel_items = sentinel_search.item_collection()
fetched_items = list(sentinel_search.items())
# len(items), items[0] if items else None

data_chunks = {'time': 1, 'x': 4096, 'y': 4096}
assets=["B02", "B03", "B04", "B08"]

sentinel_data = odc.stac.stac_load(items=fetched_items, bands=assets, bbox=bounds, groupby='time', chunks=data_chunks)

# Compute NDVI
ndvi = ((sentinel_data["B08"] - sentinel_data["B04"]) / (sentinel_data["B08"] + sentinel_data["B04"])).persist()

# Then compute some zonal stats
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
    modifier=planetary_computer.sign_inplace,
)

bounds = (-98.00080760573508, 32.99921674609716, -96.9991860639418, 34.000729644613706)

landcover_search = catalog.search(collections=["io-lulc"], bbox=bounds)
landcover_items = landcover_search.item_collection()

landcover_data = (
    stackstac.stack(
        landcover_items,
        epsg=3857,
        bounds_latlon=bounds,
        dtype="int64",
        rescale=False,
        fill_value=0,
        chunksize=2048,
        resolution=100,
    )
    .pipe(stackstac.mosaic, nodata=0)
    .squeeze()
    .rename("Landcover")
    .persist()
)

landcover_labels = dict(
    enumerate(landcover_data.coords["label:classes"].item()["classes"])
)

quantile_stats = (
    zonal_stats(
        zones=landcover_data,
        values=ndvi,
        stats_funcs=["mean", "max", "min", "count"],
    )
    .compute()
    .set_index("zone")
    .rename(landcover_labels)
)

print(quantile_stats)