import pandas as pd
import numpy as np
import helper_functions as h 
from datetime import datetime


# load esg topics and firm names
df_topics = pd.read_csv('../../data/processed/esg_topics.csv')
df_sp500 = pd.read_csv('../../data/processed/firm_names.csv')

# expand firm names for each topic
df_sp500_expanded = df_sp500.iloc[np.repeat(np.arange(len(df_sp500)), len(df_topics))].reset_index(drop=True)

# expand topics for each firm
df_topics_expanded = df_topics.iloc[list(np.arange(len(df_topics)))*len(df_sp500)].reset_index(drop=True)

# generate keywords as a combintation of firm name + topic
keywords = pd.DataFrame({'keyword':[i+' '+j for j in df_sp500.firm_name_processed for i in df_topics.topic]})

# merge topics, firm names, and search terms into 1 df
df_query_input = pd.concat([df_topics_expanded, df_sp500_expanded, keywords], axis=1).reset_index(drop=True)
df_query_input['date_construct_keyword'] = datetime.today().strftime('%Y-%m-%d')

# store as csv
h.make_csv(df_query_input, 'keywords.csv', '../../data/interim',header=True)


