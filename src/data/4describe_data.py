import pandas as pd
import helper_functions as h


def print_df_specs(rel_path, name_contains):
	for file in h.get_files(rel_path, name_contains=name_contains, absolute_path=False):
		print(file,'\n','-'*40)	
		# print N, M, column names
		df = pd.read_csv(f"{rel_path}/{file}")
		print(f"N={df.shape[0]}, M={df.shape[1]}")

		print("\n{}\n{}".format(df.info(), '='*40))

		print("Columns:\n{}".format('-'*40))
		print(*df.columns, sep='\n')
		print('='*40)

if __name__ == '__main__':
	paths = ['../../data/raw', '../../data/processed']
	print_df_specs(paths[0], name_contains='*gtrends*')
	print_df_specs(paths[1], name_contains='*yahoo*')
