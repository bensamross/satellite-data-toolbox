from dask.distributed import Client as DaskClient, LocalCluster
import warnings
import xarray as xr

warnings.filterwarnings('ignore')
xr.set_options(keep_attrs=True)
cluster = LocalCluster(
    n_workers=4,
    threads_per_worker=4,
    memory_limit='2GB',  # Add a memory limit per worker
)
client = DaskClient(cluster)  # suppress logs

print(client)
print(f"Dask dashboard link: {client.dashboard_link}")