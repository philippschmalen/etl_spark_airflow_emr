import pandas as pd
import helper_functions as h 
from datetime import datetime

topics_negative = ['scandal', 'greenwashing', 'corruption', 'fraud', 
                   'bribe', 'tax', 'forced', 'harassment', 'violation', 
                   'illegal', 'conflict', 'weapons', 'pollution',
                   'inequality', 'discrimination', 'sexism', 'racist', 
                   'intransparent', 'nontransparent', 'breach', 'lawsuit', 
                   'unfair', 'bad', 'problem', 'hate', 'issue', 'controversial', 
                  'strike', 'scam', 'trouble', 'controversy', 'mismanagement', 
                  'crisis', 'turmoil', 'shock', 'whistleblow', 'dispute']

topics_positive =  ['green', 'sustainable', 'positive', 'best', 'good', 
                    'social', 'charity', 'ethical', 'renewable', 'carbon neutral', 
                   'equitable', 'ecological', 'efficient', 'improve', 'cooperative', 
                   'beneficial', 'collaborative', 'productive', 'leader', 
                   'donate', 'optimal', 'favorable', 'desirable', 'resilient', 
                   'robust', 'reasonable', 'strong', 'organic']

print("Defined {} negative and {} positive topics".format(len(topics_negative), len(topics_positive)))


# create df with topics and label
df_topics_neg = pd.DataFrame({'topic':topics_negative, 'positive': 0})
df_topics_pos = pd.DataFrame({'topic':topics_positive, 'positive': 1})
df_topics = pd.concat([df_topics_neg, df_topics_pos]).reset_index(drop=True)

# add definition date 
df_topics['date_define_topic'] = datetime.today().strftime('%Y-%m-%d')

# export csv
h.make_csv(df_topics, 'esg_topics.csv', '../../data/interim',header=True)
