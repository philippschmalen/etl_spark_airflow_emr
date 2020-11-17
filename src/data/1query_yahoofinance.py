# import helper functions from helper_functions.py (same folder)
import helper_functions as h

# install yahooquery API:
# pip install yahooquery
from yahooquery import Ticker

import pandas as pd
print("Ensure pandas version>= 1.1.3. You have version: {}".format(pd.__version__))


#
# Load query input (metadata) 
#


def load_query_input(input_path="../../data/raw", id_column='ticker', file_name_contains="*meta*", select_file_idx=0):
	"""Load input data for query from csv that contains firm identifiers called 'ticker'

	:param input_path: path for csv input data 
	:param id_column: name of id column, such as ticker 
	:param file_name_contains: input file name contains string such as *meta* or *.csv or *input.csv
	:param select_file_idx: index to select from listed files in specified path (returned by get_files)
	:return : firm tickers as input for yahooquery api

	"""

	# metadata file in ../../data/raw
	path_meta = h.get_files(input_path, name_contains=file_name_contains, absolute_path=False)[select_file_idx]
	print(f"Load from file: {path_meta}")

	# read in firm tickers from metadata  (serves as input for queries)
	df_meta = pd.read_csv(path_meta)

	# get ticker
	tickers = list(df_meta.loc[:,id_column].unique())

	print("There are {} firm tickers as input for queries:".format(len(tickers)))
	print(tickers) 

	return tickers

#
# Query Yahoo! Finance
#


def query_yahoofinance(input):
	"""Get financial data from Yahoo! Finance

	:param input: list of firm tickers for which data should be retrieved
	:return : None (store returned data as CSV)

	"""
	query = Ticker(input)

	print("Run query .all_financial_data()")

	try:
		# query all financial data
		df_fin_raw = query.all_financial_data()
		# store raw data
		stamp = h.timestamp_now()
		h.make_csv(df_fin_raw, stamp+'yahoofinance.csv', '../../data/raw', header=True, index=True)
	except Exception as e:
		print(e)

def main():
	# Load query input (metadata) 
	query_input = load_query_input()
	# Query Yahoo! Finance
	query_yahoofinance(query_input)

if __name__=='__main__':
	main()







