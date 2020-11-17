from time import strftime, sleep
import sys 
import os
import pandas as pd
from glob import glob 
import numpy as np



def inspect_core_specifications(data, descriptives=False):
	"""Inspect data types, shape and descriptives
	
	:param data: pandas dataframe 
	:param descriptives: boolean, print descriptive statistics (default=False)
	:return: None
	"""	
	# check if data is list of dataframes
	if isinstance(data, list):
		for d in data:
			print('-'*40)
			print(d.info())
			
			if descriptives:
				print('-'*40)
				print(round(d.describe(include='all', percentiles=[])))
			
	else:
		print('-'*40)
		print(data.info())
		
		if descriptives:
			print('-'*40)
			print(round(data.describe(include='all', percentiles=[])))
	print('*'*40)
		

def inspect_missings(data, verbose=True):
	"""Inspect missings across rows and across columns
	
	Args 
		data: pandas dataframe 
		
	Returns
		:return : dataframe with info on column missings  
	"""
	if verbose:
		print("MISSINGS")
		print('-'*40)
		
	# check rows
	rows_all = data.shape[0]
	rows_nomiss = data.dropna().shape[0]

	rowmiss_count = rows_all - rows_nomiss
	rowmiss_share = rowmiss_count/rows_all*100

	if verbose:
		print("Any missing in any row: {}/{} ({} %)".format(rowmiss_count,rows_all, rowmiss_share))
		print()
	
	# check columns
	col_miss = [col for col in data.columns if data[col].isna().any()]
	# no missings for any column
	if not col_miss:
		print("No missings for any column.")
	else:
		# print share of missings for each column
		print("Return info on column missings")
		ds_colmiss = data.loc[:,col_miss].isna().sum()
		ds_colmiss_relative = data.loc[:,col_miss].isna().sum()/rows_all*100
		
		df_colmiss = pd.concat([ds_colmiss, ds_colmiss_relative], axis=1, keys=['missing_count', 'share'])\
						.sort_values("share", ascending=False)
		if verbose:
			print(df_colmiss)
			print('*'*40)

		return df_colmiss
		
	

	
def row_missing_count(df, top_n=None):
	"""Inspect absolute and relative missings across rows
	Args
		df: pandas DataFrame
		top_n: restrict output to top_n indices with most missings across columns 
	Return
		pandas dataframe with indices and their absolute and relative missings across columns
	
	"""
	
	df_colmiss_idx = df.T.isna().sum().sort_values(ascending=False)[:top_n]
	df_colmiss_idx_share = df_colmiss_idx/df.shape[1]
	
	return pd.concat([df_colmiss_idx, df_colmiss_idx_share], axis=1, keys=['missing_count', 'missing_share'])




def sleep_countdown(duration, print_step=1):
    """Sleep for certain duration and print remaining time in steps of print_step

	Input
		duration: duration of timeout (int)
		print_step: steps to print countdown (int)

	Return 
		None
	"""
    sys.stdout.write("\r Seconds remaining:")

    for remaining in range(duration, 0, -1):
        # display only steps
        if remaining % print_step == 0:
            sys.stdout.write("\r")
            sys.stdout.write("{:2d}".format(remaining))
            sys.stdout.flush()

        sleep(1)

    sys.stdout.write("\r Complete!\n")

def timestamp_now():
	"""Create timestamp string in format: yyyy/mm/dd-hh/mm/ss
		primaryliy used for file naming

	Input
		None

	Return
		String: Timestamp for current time

	"""

	timestr = strftime("%Y%m%d-%H%M%S")
	timestamp = '{}'.format(timestr)  

	return timestamp



def get_files(filepath='./', name_contains="", absolute_path=True, subdirectories=True):
  """List all files of directory filepath with their absolute filepaths
  
  Args
	filepath:	   string specifying folder
	name_containts: string with constraint on name, 
					for example all files ending with "*.py", or every file starting with '0': "0*"
	absolute_path:  return absolute paths of files or not
	subdirectories: include subdirectories or not  

  Return
	list: list with string elements of filenames 
  
  """

  all_files = []


  for (dirpath, dirnames, filenames) in os.walk(filepath):
	  
	# filenames specified
    if name_contains: 
        all_files.extend(glob(os.path.join(dirpath,name_contains)))

    elif not name_contains:
        all_files.extend(filenames)
	
	# exclude subdirectories, break loop 
    if not subdirectories:
        break
	# otherwise continue with loop, walking down the directory

  # get absolute path
    if absolute_path: 
        all_files_absolute = [os.path.abspath(f) for f in all_files]
        return all_files_absolute

    else:
        return all_files



def make_csv(x, filename, data_dir, append=False, header=False, index=False):
	'''Merges features and labels and converts them into one csv file with labels in the first column

		Input
			x: Data features
			file_name: Name of csv file, ex. 'train.csv'
			data_dir: The directory where files will be saved

		Return
			None: Create csv file as specified
	'''

	# create dir if nonexistent
	if not os.path.exists(data_dir):
		os.makedirs(data_dir)

	# make sure its a df
	x = pd.DataFrame(x)

	# export to csv
	if not append:
		x.to_csv(os.path.join(data_dir, filename), 
									 header=header, 
									 index=index)
	# append to existing
	else:
		x.to_csv(os.path.join(data_dir, filename),
									 mode = 'a',
									 header=header, 
									 index=index)		

	# nothing is returned, but a print statement indicates that the function has run
	print('Path created: '+str(data_dir)+'/'+str(filename))


def inspect_drop_rows_retain_columns(data, max_missing=3):
    """Dropping rows with many missings for certain columns 
        to keep columns
    :param data: dataframe
    :param max_missing: defines until which column-wise missing count should be iterated
    :return list with indices to drop, tuple of numpy arrays for plotting: Columns to keep vs. rows dropped (%)
    """
    # count missings per column, store in series
    count_column_missing = data.isna().sum(axis=0).sort_values(ascending=False)
    column_missing_names = count_column_missing.index

    # get array of missing count with unique values to loop over
    count_column_missing_unique = count_column_missing[count_column_missing >= 0].sort_values().unique()

    # define until which column-wise missing count should be iterated
    plot_x, plot_y = np.zeros(max_missing), np.zeros(max_missing)
    print("Trade off rows against columns\nDrop rows and modifiy original dataframe to keep more columns\n")
    print("i\tKeep Columns\tDrop Rows [%]\n"+'-'*40)
    
    # for comparisons:
    # raw df drop columns with missing, no modification 
    df_raw = data.dropna(axis=1)


    drop_rows = []

    for arr_idx, i in enumerate(count_column_missing_unique[:max_missing]):
        select_columns = count_column_missing[count_column_missing <= i].index

        # return indices (=ticker) where the 1 missing per column occurs
        indices_many_missings = list(data.loc[data[select_columns].isnull().any(1),:].index)
        drop_rows.append(indices_many_missings)
        
        ## compare modified vs. raw dropped missings
        # modified df 
        df_fin_mod = data.drop(indices_many_missings).dropna(axis=1)
        df_fin_nomiss = df_fin_mod

        ### Benefits of dropping x rows
        ## comparison of dropping and keeping
        # original 
        n_row_orig, n_col_orig = data.shape
        # modified & cleaned 
        n_row_mod, n_col_mod = df_fin_mod.shape
        # raw & cleaned
        n_row_nomod, n_col_nomod = df_raw.shape

        ## STATISTICS
        # dropped rows to modify df (count and %)
        n_dropped_rows_mod = len(indices_many_missings)
        pct_dropped_rows_mod = round(n_dropped_rows_mod/n_row_orig*100)

        # dropped cols for modified df (count and %)
        n_dropped_cols_mod = n_col_orig - n_col_mod
        pct_dropped_cols_mod = round(n_dropped_cols_mod/n_col_orig*100)

        # dropped cols for non-modified df (count and %)
        n_dropped_cols_nomod = n_col_orig - n_col_nomod
        pct_dropped_cols_nomod = round(n_dropped_cols_nomod/n_col_orig*100)

        # plot dropped rows against retained columns 
        # (x=100-pct_dropped_rows_mod, y=pct_dropped_rows_mod)
        plot_x[arr_idx], plot_y[arr_idx] = 100-pct_dropped_cols_mod, pct_dropped_rows_mod
        print("{}\t{}\t\t{}".format(i, plot_x[arr_idx], plot_y[arr_idx])) 
        

    return drop_rows, plot_x, plot_y