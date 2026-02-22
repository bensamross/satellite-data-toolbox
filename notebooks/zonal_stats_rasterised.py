import numpy as np
import xarray as xr
import rioxarray
from geocube.api.core import make_geocube
from rasterio.features import rasterize
from shapely.geometry import mapping as shp_mapping
import geopandas as gpd
import pandas as pd

def zonal_stats_rasterized(data: xr.DataArray, gdf: gpd.GeoDataFrame, id_col: str) -> pd.DataFrame:
    """
    Efficient zonal statistics by rasterizing polygons onto the data grid.
    
    Parameters
    ----------
    data : xr.DataArray
        The raster data (e.g. NDVI) with dims (time, y, x).
    gdf : gpd.GeoDataFrame
        Polygons with a unique identifier column.
    id_col : str
        Column in gdf to use as zone labels.
    
    Returns
    -------
    pd.DataFrame
        Zonal means with columns: time, <id_col>, value.
    """
    # Ensure CRS match
    gdf_reproj = gdf.to_crs(data.rio.crs)
    
    # Create a numeric zone ID for rasterization
    gdf_reproj = gdf_reproj.copy()
    gdf_reproj['_zone_id'] = np.arange(1, len(gdf_reproj) + 1, dtype=np.int32)
    
    # Build transform from the data's spatial coords
    transform = data.rio.transform()
    out_shape = (data.sizes['y'], data.sizes['x'])
    
    # Rasterize: burn zone IDs into a 2D array
    shapes = [(geom, zid) for geom, zid in zip(gdf_reproj.geometry, gdf_reproj['_zone_id'])]
    zone_raster = rasterize(
        shapes,
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype=np.int32,
    )
    
    # Create an xarray DataArray for the zones (same y, x coords)
    zones = xr.DataArray(
        zone_raster,
        dims=['y', 'x'],
        coords={'y': data.y, 'x': data.x},
        name='zone',
    )
    
    # If data is dask-backed, compute zones eagerly (it's small)
    # but keep the actual data lazy
    if data.chunks is not None:
        # Flatten approach: much more memory-friendly than groupby on dask
        # Process per time-step in parallel via dask.delayed
        import dask
        
        @dask.delayed
        def _stats_one_timestep(data_2d, zone_raster_np, n_zones):
            """Compute mean per zone for a single 2D slice."""
            vals = data_2d.values if hasattr(data_2d, 'values') else data_2d
            results = np.full(n_zones, np.nan, dtype=np.float64)
            for z in range(1, n_zones + 1):
                mask = zone_raster_np == z
                if mask.any():
                    zone_vals = vals[mask]
                    valid = zone_vals[np.isfinite(zone_vals)]
                    if len(valid) > 0:
                        results[z - 1] = np.nanmean(valid)
            return results
        
        n_zones = len(gdf_reproj)
        tasks = []
        times = data.time.values
        
        for t in range(len(times)):
            slice_2d = data.isel(time=t).compute()  # compute one slice at a time
            tasks.append(_stats_one_timestep(slice_2d, zone_raster, n_zones))
        
        results = dask.compute(*tasks)
        
        # Assemble DataFrame
        records = []
        id_values = gdf_reproj[id_col].values
        for i, t in enumerate(times):
            for j in range(n_zones):
                records.append({'time': t, id_col: id_values[j], 'value': results[i][j]})
        
        return pd.DataFrame(records)
    
    else:
        # Eager path: use np.bincount for maximum speed
        n_zones = len(gdf_reproj)
        zone_flat = zone_raster.ravel()
        id_values = gdf_reproj[id_col].values
        
        records = []
        for t in range(data.sizes['time']):
            data_flat = data.isel(time=t).values.ravel().astype(np.float64)
            valid = np.isfinite(data_flat) & (zone_flat > 0)
            
            sums = np.bincount(zone_flat[valid], weights=data_flat[valid], minlength=n_zones + 1)
            counts = np.bincount(zone_flat[valid], minlength=n_zones + 1)
            
            means = np.where(counts[1:] > 0, sums[1:] / counts[1:], np.nan)
            
            time_val = data.time.values[t]
            for j in range(n_zones):
                records.append({'time': time_val, id_col: id_values[j], 'value': means[j]})
        
        return pd.DataFrame(records)