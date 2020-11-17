import pandas as pd 
import numpy as np 
from sys import version	

print(f"Python version: {version}")

query_return_length = 261
keywords = [i for i in range(20)]

# # create empty df with 0s
df_zeros = pd.DataFrame(np.zeros((query_return_length*len(keywords), 3)), columns=['date','keyword', 'search_interest'])
# replace 0s with keywords
df_zeros['keyword'] = np.repeat(keywords, query_return_length)

# replace 0s with sequence for date
df_zeros['date'] = np.tile(np.arange(query_return_length),len(keywords))

print(df_zeros.head())

