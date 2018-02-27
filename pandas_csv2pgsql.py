#!/usr/bin/python3

import pandas as pd
from sqlalchemy import create_engine
# from sqlalchemy.types import String
# from sqlalchemy.types import Float

# Parameters - later can be defined with JSON or script arguments
# ==========
# ==========

working_directory = './'			    # working directory
input_csv_files = ['some_csv_file_1.csv','some_csv_file_2.csv']  #  

separator = ','
header_row = 0   # 0 means 1st line is header, use None (without quotes) for no header
quotes = '"'
quoting_mode = 0 # QUOTE_MINIMAL (0), QUOTE_ALL (1), QUOTE_NONNUMERIC (2) or QUOTE_NONE (3)

# ==========
# ==========

# DB connection definition
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgis') # dialect+driver://username:password@host:port/database

for csv_file in input_csv_files:
    # Table name - based on CSV file name
    table_name = csv_file[:-4]
    # Load data into pandas
    input_data = pd.read_csv(working_directory + "/" + csv_file, sep = separator, header = header_row, quotechar = quotes, quoting = quoting_mode)
    # convert bad column names
    new_column_names = []
    for i in input_data.columns:
        new_column_names.append(i.replace(' ','_').lower())
    input_data.columns = new_column_names
    # InputCsv to DB
    input_data.to_sql(table_name, engine, chunksize=10000, index = False)
