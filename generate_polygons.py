# generate_polygons.py
# Creates a GeoPackage "polygons.gpkg" with ~8 random polygons along Queensland's east coast.
import random
import math
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon, box

random.seed(42)
np.random.seed(42)

# Define a suitable projected CRS for area calculations in meters
# GDA94 / MGA Zone 56 is appropriate for the Queensland coast
PROJECTED_CRS = "EPSG:28356"
MAX_AREA_HECTARES = 100
MAX_AREA_SQ_METERS = MAX_AREA_HECTARES * 10000

def random_polygon(cx, cy, min_r=0.01, max_r=0.05, n_vertices=None):
    if n_vertices is None:
        n_vertices = random.randint(6, 12)
    angles = np.sort(np.random.uniform(0, 2 * math.pi, n_vertices))
    radii = np.random.uniform(min_r, max_r, n_vertices)
    pts = [(cx + r * math.cos(a), cy + r * math.sin(a)) for a, r in zip(angles, radii)]
    poly = Polygon(pts)
    if not poly.is_valid:
        poly = poly.buffer(0)
    return poly

# Rough east-coast QLD bounding box in WGS84 (lon/lat)
# Covers coastline from Gold Coast up to Cape York
minx, miny, maxx, maxy = 146.5, -28.8, 153.8, -12.0
roi = box(minx, miny, maxx, maxy)

polys = []
attempts = 0
target = 8

while len(polys) < target and attempts < 500:
    attempts += 1
    # Bias longitudes towards the coast
    cx = random.uniform(148.5, 153.5)
    cy = random.uniform(-28.5, -13.5)
    poly = random_polygon(cx, cy)
    poly = poly.intersection(roi)

    if not poly.is_empty and poly.area > 0:
        # To check area accurately, we must use a projected CRS
        temp_gdf = gpd.GeoDataFrame([{"geometry": poly}], crs="EPSG:4326")
        temp_gdf_proj = temp_gdf.to_crs(PROJECTED_CRS)
        area_sq_meters = temp_gdf_proj.geometry.iloc[0].area

        if area_sq_meters <= MAX_AREA_SQ_METERS:
            polys.append(poly)

gdf = gpd.GeoDataFrame({"id": list(range(1, len(polys) + 1)), "geometry": polys}, crs="EPSG:4326")
gdf.to_file("polygons.gpkg", driver="GPKG", layer="polygons")