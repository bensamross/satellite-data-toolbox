import geopandas as gpd

gdf = gpd.read_file('data/inputs/h3.gpkg', layer='h3_elliott_river')
print(gdf.head(), gdf.shape)

gdf_level7 = gdf[gdf['level'] == 5]
print(gdf_level7.shape)

gdf_level10 = gdf[gdf['level'] == 10]
# print(gdf_level10.shape)

# Intersect the level 10 polygons with the level 8 polygons
# NOTE: There are about 1449 level 10 polygons in each level 5 polygon
# NOTE: There are about 32 level 10 polygons in each level 8 polygon
# NOTE: There are about 173 level 10 polygons in each level 7 polygon
intersection = gpd.overlay(gdf_level10, gdf_level7, how='intersection')


for level7_id in intersection['GRID_ID_2'].unique():
    print(level7_id)

