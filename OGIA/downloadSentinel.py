import openeo

# User parameters
BACKEND_URL = "https://openeo.dataspace.copernicus.eu"
COLLECTION_ID = "SENTINEL2_L2A"
BANDS = ["B04", "B08"]  # Red and NIR
SPATIAL_EXTENT = {
    "west": 152.8,
    "south": -27.7,
    "east": 153.0,
    "north": -27.5,
    "crs": "EPSG:4326"
}
TEMPORAL_EXTENT = ["2023-06-01", "2023-06-30"]
OUTPUT_FILE = "sentinel2_ndvi.tif"

def main():
    # Connect and authenticate (OIDC: interactive browser)
    conn = openeo.connect(BACKEND_URL)
    conn.authenticate_oidc()

    # Load collection
    cube = conn.load_collection(
        COLLECTION_ID,
        spatial_extent=SPATIAL_EXTENT,
        temporal_extent=TEMPORAL_EXTENT,
        bands=BANDS
    )

    # Calculate NDVI
    ndvi = cube.ndvi(nir="B08", red="B04")

    # Download result synchronously (for small areas)
    ndvi.download(OUTPUT_FILE, format="GTiff")
    print(f"Downloaded NDVI GeoTIFF to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()