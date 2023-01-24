# PrizmaDeskTask

## Extraction
This ETL process extracts CSV files with transactions and postions data from SFTP Server. 

## Transfromation
Day_id is added to data from each file name and timestamp in each row. 

## Load
Loads transformed data in PostgreeSQL database in two tables fct_transactions and fct_positions.

## Result

### Table fct_positions

![image](https://user-images.githubusercontent.com/118864207/214380667-f2f16746-c344-41bc-af9e-08b6f95b240b.png)


### Table fct_transactions

![image](https://user-images.githubusercontent.com/118864207/214380485-7b6313ed-a1ff-48b0-ac66-dab34bd2c47c.png)


