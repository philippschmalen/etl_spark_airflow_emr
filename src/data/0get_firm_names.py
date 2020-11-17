import pandas as pd
import re
from datetime import datetime
import helper_functions as h 



def get_firms_sp500():
    """Obtain S&P 500 listings from Wikipedia"""
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df_sp500 = table[0]
    
    return df_sp500


def regex_strip_legalname(raw_names):
    """Removes legal entity, technical description or firm type from firm name
    
    Input
        raw_names: list of strings with firm names
        
    Return
        list of strings: firm names without legal description 
    
    """
    
    pattern = r"(\s|\.|\,|\&)*(\.com|Enterprise|Worldwide|Int\'l|N\.V\.|LLC|Co\b|Inc\b|Corp\w*|Group\sInc|Group|Company|Holdings\sInc|\WCo(\s|\.)|plc|Ltd|Int'l\.|Holdings|\(?Class\s\w+\)?)\.?\W?"
    stripped_names = [re.sub(pattern,'', n) for n in raw_names]
    
    return stripped_names

# get firm S&P500 from Wikipedia
keep_columns = ['Symbol','Security', 'GICS Sector']
df_sp500_wiki = get_firms_sp500().loc[:,keep_columns]

# rename column, set ticker as index
df_sp500_wiki= df_sp500_wiki.rename(columns={'Symbol': 'ticker', 'Security': 'firm_name_raw', 'GICS Sector': 'sector'})

# process firm names (remove legal entity, suffix)
df_sp500_wiki['firm_name_processed'] = regex_strip_legalname(list(df_sp500_wiki.firm_name_raw))

# add retrieval date 
df_sp500_wiki['date_get_firmname'] = datetime.today().strftime('%Y-%m-%d')

# drop duplicate firm names 
df_sp500_wiki.drop_duplicates(subset='firm_name_processed', inplace=True)

h.make_csv(df_sp500_wiki, 'firm_names.csv', '../../data/interim',header=True)