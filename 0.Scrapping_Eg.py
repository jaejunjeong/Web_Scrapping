#!/usr/bin/env python
# coding: utf-8

#!mamba install pandas==1.3.3 -y
#!mamba install requests==2.26.0 -y

## Imports
# Import any additional libraries you may need here
import glob
import pandas as pd
from datetime import datetime

# As the exchange rate fluctuates, we will download the same dataset to make marking simpler. This will be in the same format as the dataset you used in the last section
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv')

### JSON Extract Function
# This function will extract JSON files.
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


## Extract Function 
# Define the extract function that finds JSON file `bank_market_cap_1.json` and calls the function created above to extract data from them. 
# Store the data in a `pandas` dataframe. Use the following list for the columns

columns=['Name','Market Cap (US$ Billion)']
def extract():
    # Write your code here
    jsondata = pd.DataFrame(extract_from_json('bank_market_cap_1.json'))
    return jsondata

# Load the file 'exchange_rates.csv' as a dataframe and find the exchange rate for British pounds with the symbol 'GBP', store it in the variable 'exchange_rate',
# you will be asked for the number. (set the parameter index_col to 0)
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process, index_col= 0)
    return dataframe
csvdata = extract_from_csv('exchange_rates.csv')
exchange_rate = csvdata.loc['GBP','Rates']
exchange_rate


## Transform
# Using <code>exchange_rate</code> and the `exchange_rates.csv` file find the exchange rate of USD to GBP. Write a transform function that
# 1.  Changes the `Market Cap (US$ Billion)` column from USD to GBP
# 2.  Rounds the Market Cap (US$ Billion)\` column to 3 decimal places
# 3.  Rename `Market Cap (US$ Billion)` to `Market Cap (GBP$ Billion)
def transform(data_to_process):
    # Write your code here
    data_to_process['Market Cap (US$ Billion)'] = round(data_to_process['Market Cap (US$ Billion)'] * exchange_rate,3)
    data_to_process_new = data_to_process.rename(columns={'Market Cap (US$ Billion)' : 'Market Cap (GBP$ Billion)'})
    return data_to_process_new

## Load
# Create a function that takes a dataframe and load it to a csv named `bank_market_cap_gbp.csv`. Make sure to set `index` to `False`.
def load(address, data_to_process):
    data_to_process.to_csv(address)


## Logging Function
# Write the logging function log to log your data
def log(logtext):
    now = datetime.now()
    dateformat = "%H:%M:%S-%h-%d-%y"
    time = now.strftime(dateformat)
    with open("Final_logfile.txt","a") as f:
        f.write(logtext + " " + time + "\n")


## Running the ETL Process
# Log the process accordingly using the following <code>"ETL Job Started"</code> and <code>"Extract phase Started"
log("ETL Job Started")
log("Extract phase Started")

### Extract
# Use the function extract, and print the first 5 rows
jsondata = extract()
jsondata.head(5)

# Log the data as "Extract phase Ended"
log("Extract phase Ended")

### Transform
# Log the following  <code>"Transform phase Started"
log("Transform phase Started")

# Call the function here
transformed_data = transform(jsondata)
# Print the first 5 rows here
transformed_data.head(5)

# Write your code here
log("Transform phase Ended")

# Log the following `"Load phase Started"`.
log("Load phase Started")


# Call the load function
load("Final_Assignment.csv",transformed_data)


# Log the following `"Load phase Ended"`.


# Write your code here
log("Load phase Ended")
