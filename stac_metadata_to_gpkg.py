# python stac_metadata_to_gpkg.py --gpkg ./inputs/hexgrids.gpkg --layer tesselated_10ha_hexagons_within_10km_of_plantation --time 2025-01-01/2025-12-31 --prese="Digital Earth Australia" --out_gpkg ./stac.gpkg --out_layer stac

import argparse
import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, box
import pystac_client
from tqdm.auto import tqdm
import fiona

def extract_item_record(item):
    d = item.to_dict()
    props = d.get("properties", {}) or {}
    # primary datetime / acquired
    acquired = props.get("datetime") or props.get("acquired") or d.get("properties", {}).get("start_datetime") or d.get("properties", {}).get("end_datetime")
    # cloud cover
    cloud = props.get("eo:cloud_cover") or props.get("cloud_cover")
    # geometry (try geometry, fallback to bbox)
    geom_json = d.get("geometry") or d.get("bbox")
    if isinstance(geom_json, dict):
        geom = shape(geom_json)
    else:
        bbox = d.get("bbox")
        if bbox and len(bbox) == 4:
            minx, miny, maxx, maxy = bbox
            geom = box(minx, miny, maxx, maxy)
        else:
            geom = None

    # collect asset hrefs
    assets = {}
    for k, v in d.get("assets", {}).items():
        if isinstance(v, dict):
            assets[k] = v.get("href")
        else:
            assets[k] = v

    metadata = {
        "item_id": d.get("id"),
        "collection": d.get("collection") or d.get("collection_id"),
        "datetime": pd.to_datetime(acquired) if acquired else None,
        "acquired": pd.to_datetime(acquired) if acquired else None,
        "cloud_cover": float(cloud) if cloud is not None else None,
        "sun_elevation": props.get("view:sun_elevation") or props.get("sun_elevation"),
        "sun_azimuth": props.get("view:sun_azimuth") or props.get("sun_azimuth"),
        "platform": props.get("platform") or props.get("constellation") or props.get("mission"),
        "instruments": props.get("instruments"),
        "gsd": props.get("gsd") or props.get("eo:gsd"),
        "constellation": props.get("constellation"),
        "mission": props.get("mission"),
        "assets": assets,
        "stac_properties": props,
        "geometry": geom,
    }
    return metadata

def main():
    p = argparse.ArgumentParser(description="Fetch STAC item metadata for a geopackage layer and store into a geopackage layer.")
    p.add_argument("--gpkg", required=True, help="Input geopackage path (used to get AOI bounds).")
    p.add_argument("--layer", required=True, help="Layer name inside the geopackage to use as AOI.")
    p.add_argument("--time", required=True, help="Datetime range for STAC search, e.g. 2025-01-01/2025-12-31")
    p.add_argument("--resources", default="resources.json", help="resources.json path (used to obtain STAC URL and sensors).")
    p.add_argument("--stac_url", help="Optional override STAC API URL.")
    p.add_argument("--collection", help="Optional collection name (STAC collection id). If omitted the script will try to read sensors from resources.json.")
    p.add_argument("--preset", default="Digital Earth Australia", help="Name of preset STAC entry in resources.json to use (default: Digital Earth Australia).")
    p.add_argument("--out_gpkg", default=None, help="Output geopackage file to write metadata. Defaults to input geopackage.")
    p.add_argument("--out_layer", default="imagery_metadata", help="Output layer name in geopackage.")
    p.add_argument("--max_items", type=int, default=None, help="Limit number of items fetched (for testing).")
    p.add_argument("--overwrite", action="store_true", help="If set, overwrite existing geopackage layer by deleting the geopackage (WARNING: deletes all layers).")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    gpkg_path = Path(args.gpkg)
    if not gpkg_path.exists():
        logger.error(f"Input geopackage not found: {gpkg_path}")
        return

    # read AOI layer and compute bbox in EPSG:4326
    aoi = gpd.read_file(gpkg_path.as_posix(), layer=args.layer)
    if aoi.empty:
        logger.error("AOI layer is empty")
        return
    aoi_4326 = aoi.to_crs("EPSG:4326")
    bbox = list(aoi_4326.total_bounds)  # [minx, miny, maxx, maxy]
    logger.info(f"AOI bbox (EPSG:4326): {bbox}")

    stac_url = args.stac_url
    collection = args.collection

    # If not provided on the CLI, try to read presets from resources.json using the --preset name
    if not stac_url or not collection:
        try:
            with open(args.resources, "r") as rf:
                resources = json.load(rf)
            catalog = next((c for c in resources.get("stac_catalogs", []) if c.get("name") == args.preset), None)
            if catalog:
                if not stac_url:
                    stac_url = catalog.get("url")
                if not collection:
                    sensors = catalog.get("sensors") or []
                    for s in sensors:
                        if s.get("name"):
                            collection = s.get("name")
                            break
        except Exception:
            pass

    if not stac_url:
        logger.error("No STAC URL provided and unable to read from resources.json")
        return
    if not collection:
        logger.error("No collection name provided and unable to infer from resources.json/preset")
        return

    logger.info(f"Using STAC URL: {stac_url}")
    logger.info(f"Collection: {collection}")
    logger.info(f"Time range: {args.time}")

    catalog = pystac_client.Client.open(stac_url)
    search = catalog.search(bbox=bbox, collections=[collection], datetime=args.time)
    items = list(search.items())
    if args.max_items:
        items = items[: args.max_items]
    logger.info(f"Found {len(items)} items")

    records = []
    for item in tqdm(items, desc="Fetching STAC items", unit="item"):
        rec = extract_item_record(item)
        if rec["geometry"] is None:
            logger.debug(f"Skipping item without geometry: {rec.get('item_id')}")
            continue
        records.append(rec)

    if not records:
        logger.info("No metadata records extracted.")
        return

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

    out_gpkg = Path(args.out_gpkg) if args.out_gpkg else gpkg_path
    layer = args.out_layer

    # handle existing gpkg / layer
    existing_layers = fiona.listlayers(out_gpkg.as_posix()) if out_gpkg.exists() else []
    if layer in existing_layers:
        if args.overwrite:
            logger.warning("Overwrite requested: deleting existing geopackage file (this removes all layers).")
            out_gpkg.unlink()
            existing_layers = []
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            layer = f"{layer}_{ts}"
            logger.warning(f"Layer exists; writing to new layer: {layer}")

    # ensure JSON serialisable storage for complex columns
    gdf["assets"] = gdf["assets"].apply(lambda x: json.dumps(x) if x is not None else None)
    # Ensure stac_properties is a dict (it may already be dict or a JSON string)
    def _to_dict(v):
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        try:
            return json.loads(v)
        except Exception:
            return {}
    stac_props = gdf["stac_properties"].apply(_to_dict)
    # helper to sanitize column names
    def _safe_col(k: str) -> str:
        return k.replace(":", "_").replace(".", "_").replace("/", "_").replace(" ", "_")
    # standard properties to expose as columns (common STAC keys)
    standard_keys = [
        "eo:cloud_cover",
        "view:sun_elevation",
        "view:sun_azimuth",
        "platform",
        "instruments",
        "gsd",
        "eo:gsd",
        "constellation",
        "mission",
        "start_datetime",
        "end_datetime",
        "datetime",
    ]
    for k in standard_keys:
        col = _safe_col(k)
        gdf[col] = stac_props.apply(lambda p, k=k: p.get(k))
    # Add any remaining properties as prop_<key> (serialize non-scalars)
    all_keys = set().union(*[set(d.keys()) for d in stac_props])
    extra_keys = sorted(all_keys - set(standard_keys))
    for k in extra_keys:
        col = "prop_" + _safe_col(k)
        def _extract(p, key=k):
            v = p.get(key)
            if v is None:
                return None
            if isinstance(v, (str, int, float, bool)):
                return v
            try:
                return json.dumps(v)
            except Exception:
                return str(v)
        gdf[col] = stac_props.apply(_extract)
    # keep original stac_properties JSON for reference (string)
    gdf["stac_properties"] = stac_props.apply(lambda p: json.dumps(p) if p else None)
    # datetime columns are already pandas datetimes; geopackage will store as strings
    gdf["acquired"] = gdf["acquired"]

    logger.info(f"Writing {len(gdf)} records to {out_gpkg} layer={layer}")
    gdf.to_file(out_gpkg.as_posix(), layer=layer, driver="GPKG")
    logger.info("Write complete")

if __name__ == "__main__":
    main()