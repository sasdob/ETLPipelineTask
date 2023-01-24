# PrizmaDeskTask

## Extraction
This ETL process extracts CSV files with transactions and postions data from SFTP Server. 

## Transfromation
Day_id is added to data and from each file name and timestamp in each row. 

## Load
Loads transformed data in PostgreeSQL database in two tables fct_transactions and fct_positions.
