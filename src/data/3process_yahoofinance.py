# import helper functions from helper_functions.py (same folder)
import helper_functions as h

import pandas as pd
print("Ensure pandas version>= 1.1.3. You have version: {}".format(pd.__version__))

# 
# Load data 
# 

# get first file from file list
load_file = h.get_files('../../data/raw', name_contains="*yahoo*" , absolute_path=False)[0]
print(f"Load file {load_file}")
df_fin_raw = pd.read_csv(load_file, index_col=0)
print("Loaded file has {} unique firm ticker".format(df_fin_raw.index.nunique()))

#
# Preprocessing
#

# select most recent date
date_most_recent = df_fin_raw.groupby(df_fin_raw.index).asOfDate.max() 
print("Select most recent date for each firm like {}".format(date_most_recent[0]))
df_fin_recent = df_fin_raw[df_fin_raw.asOfDate.isin(date_most_recent)]

#
# Missings, clean data
#

# identify rows with many missings
rows_many_missings, _,_ = h.inspect_drop_rows_retain_columns(df_fin_recent, max_missing=4)
print("Drop rows:", rows_many_missings[1])

# select rows that cause at least 1 missing for certain columns
drop_rows = rows_many_missings[1]
# export names to keep track on droppings
h.make_csv(drop_rows, "yahoofinance_firms_excluded.csv", data_dir='../../data/interim')

# drop rows with many missings
df_fin_recent_clean = df_fin_recent.drop(drop_rows)
print(f"Exclude {len(drop_rows)} // {len(df_fin_recent_clean)} firms still available\n")

# inspect missings again
df_colmiss = h.inspect_missings(df_fin_recent_clean)

# drop columns with missings
df_fin_nomiss = df_fin_recent_clean.drop(df_colmiss.index, axis=1)

# crosscheck
h.inspect_missings(df_fin_nomiss)
print("Awesome!")

print("Financial data 1 row for 1 firm with shape:")
print(df_fin_nomiss.shape)

# 
# Reshape to long format and export
# 

# reshape into long format
df_fin_long = pd.melt(df_fin_nomiss, 
						id_vars=['asOfDate', 'periodType'], 
						var_name='financial_var', 
						value_name='financial_val', 
						ignore_index=False) 
# rename columns
df_fin_clean = df_fin_long.rename(columns={'asOfDate': 'date_financial', 'periodType': 'financial_interval'})
# export data
h.make_csv(df_fin_clean, 'yahoofinance_long.csv', '../../data/processed', header=True, index=True)
