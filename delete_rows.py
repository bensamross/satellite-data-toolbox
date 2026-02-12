# Script to delete rows from a geopackage layer if they do not intersect the geometry of another input layer. Save the results back to the same layer, permanently eliminating the deleted rows.

import geopandas as gpd

def delete_non_intersecting_rows(input_gpkg, input_layer, reference_gpkg, reference_layer, output_gpkg, output_layer):
    # Load the input and reference layers
    input_gdf = gpd.read_file(input_gpkg, layer=input_layer)
    reference_gdf = gpd.read_file(reference_gpkg, layer=reference_layer)

    # Ensure both GeoDataFrames use the same CRS
    if input_gdf.crs != reference_gdf.crs:
        reference_gdf = reference_gdf.to_crs(input_gdf.crs)

    # Perform spatial join to find intersecting geometries
    intersecting_gdf = gpd.sjoin(input_gdf, reference_gdf, how='inner', op='intersects')

    # Drop duplicates to retain only original columns from input_gdf
    intersecting_gdf = intersecting_gdf[input_gdf.columns]

    # Save the result to a new geopackage
    intersecting_gdf.to_file(output_gpkg, layer=output_layer, driver='GPKG')

if __name__ == "__main__":
    input_gpkg = "inputs/hexgrids.gpkg"
    input_layer = "h3_level10_coastal"
    reference_gpkg = "inputs/hexgrids.gpkg"
    reference_layer = "plantation_background_10km_buffer"
    output_gpkg = "inputs/hexgrids.gpkg"
    output_layer = "h3_level10_coastal"

    delete_non_intersecting_rows(input_gpkg, input_layer, reference_gpkg, reference_layer, output_gpkg, output_layer)