There is a duck db cli or a python library that can be used to execute sql files and deal with a duckdb database. Perhaps the easiest approach is to use the cli since python can sometimes cause trouble and activating the libraries automatically can be tricky. Plus this process will move into a bash process which is suited to a cli.

Steps:

0. Fetch data
1. Load raw data into duckdb, the views will be automatically recalculated
2. Determine the new data and export that to a parquet file
3. Copy the parquet file somewhere