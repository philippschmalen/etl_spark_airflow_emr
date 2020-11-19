import pandas as pd
import numpy as np
from scripts.helper_functions import make_csv



def series_duplicate(s, times=2, axis=0, reset_index=True):
    """Create a dataframe by repeating series multiple times
    
    :param s: pandas series
    :param times: how often the series should be repeated 
    :param axis: repeat over rows (axis=0) or over columns (=1)
    :param reset_index: reset index to consecutive integer
    
    :return : pandas DataFrame or Series with repeated Series from args 
    """
    df_dup = pd.concat([s] * times, axis=axis)
    
    if reset_index:
        df_dup = df_dup.reset_index(drop=True)
    
    return df_dup 

def create_dummy_sequence(sequence_range=261, additional_strings='0.0'):
	"""Create sequence of strings which replace 0 returns in query_google_trends"""

	dummy_sequence = [str(i) for i in range(sequence_range)]
	for i in additional_strings:
		dummy_sequence.append(additional_strings)

	return dummy_sequence

def get_successful_sequence(df, dummy_sequence, select_column='date'):
	"""Load date series of a successful query"""

	# select complete date series (not in dummy_sequence)
	series_complete = pd.Series(df[select_column][~df[select_column].isin(dummy_sequence)].unique())

	return series_complete

def subset_dummy_sequence(df, dummy_sequence, select_column='date', id_column='keyword'):
	"""Select dummy sequence from df which contains successful sequences"""
	# select subset of df with date series with values dummy_sequence
	df_dummy_sequence = df[df.loc[:,select_column].isin(dummy_sequence)]

	return df_dummy_sequence
	



def main(path, query_file): 
	id_column = 'keyword'
	sequence_column = 'date'
	df = pd.read_csv(path+query_file, names=[sequence_column, id_column, 'search_interest'])

	dummy_sequence = create_dummy_sequence()
	successful_sequence = get_successful_sequence(df, dummy_sequence)
	df_dummy_sequence = subset_dummy_sequence(df, dummy_sequence)
	
	# simply expand successful_sequence to match n_rows of df with date == '0.0'
	n_repeat = df_dummy_sequence.loc[:,id_column].unique().shape[0]
	date_duplicate_series = series_duplicate(successful_sequence, n_repeat)	

	# ensure same index as df_dummy_sequence
	date_duplicate_series.index = df_dummy_sequence.index
	# replace date column
	df_dummy_sequence[sequence_column] = date_duplicate_series
	# insert back to original df
	df.loc[df_dummy_sequence.index,:] = df_dummy_sequence

	# export
	make_csv(df, f'{query_file}_preprocessed.csv', path, header=True)

if __name__ == '__main__':
	main()