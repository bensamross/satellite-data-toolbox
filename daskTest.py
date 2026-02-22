from dask.distributed import Client as DaskClient, LocalCluster
import warnings
import xarray as xr
import time
import multiprocessing

warnings.filterwarnings('ignore')
xr.set_options(keep_attrs=True)

def main():
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=4,
        memory_limit='2GB',
    )
    client = DaskClient(cluster)
    print(client)
    print(f"Dask dashboard link: {client.dashboard_link}")

    # simple test to ensure workers accept work
    fut = client.submit(lambda x: x + 1, 41)
    print("task result:", fut.result(timeout=10))

    # keep the cluster alive briefly so you can open the dashboard
    time.sleep(30)

    client.close()
    cluster.close()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except Exception:
        pass
    main()