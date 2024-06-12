# DSML-NTUA-Big-Data

To run the code, you need to select one of the following available options:
1. 'A': Run Query 1 using DataFrames
2. 'A-SQL':  Query 1 using SQL API
3. 'B': Run Query 2 using DataFrames
4. 'B-RDD': Run Query 2 using RDDs
5. 'C': Run Query 3
6. 'D': Run Query 4 using DataFrames
7. 'D-RDD': Run Query 4 using RDDs
8. 'transform': To transform the file from .csv to parquet
9. 'merge': To merge the two data sets into one

Run command `spark-submit main.py <option>`, e.g: `spark-submit main.py D-RDD`
