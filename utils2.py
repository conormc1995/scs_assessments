from utils import df_from_csv

import pandas as pd
from tabulate import tabulate



file_name = 'mixpanel_users_assess_scores.csv'
df_assess_fl = df_from_csv(file_name)

df_assess_fl['timestamp_first'] = pd.to_datetime(df_assess_fl['timestamp_first']) 
df_assess_fl['timestamp_last'] = pd.to_datetime(df_assess_fl['timestamp_last'])



def filter_by_dates(df, date1, date2):
    
    date1 = pd.to_datetime(date1)
    date2 = pd.to_datetime(date2)
    
    df = df[(df['timestamp_first'] >= date1) & (df['timestamp_last'] <= date2)]
    
    return df
    

def average_df(date1, date2, dropna=True):
    
    df = filter_by_dates(df_assess_fl, date1, date2)
    
    # get columns of interest
    df = df[['distinct_id', 'total_time', 'dep_diff','anx_diff', 'mood_diff', 
             'func_diff', 'wb_diff', 'dep_d', 'anx_d', 'mood_d', 'func_d', 'wb_d']]
    
    if dropna: df = df.dropna(axis=0)
    
    df1 = pd.DataFrame() # create empty pandas dataframe
    
    df1['num_users'] = [len(df_assess_fl)]
    
    df1['dep_d_avg'] = [df['dep_d'].mean()]
    df1['anx_d_avg'] = [df['anx_d'].mean()]
    df1['mood_d_avg'] = [df['mood_d'].mean()]
    df1['func_d_avg'] = [df['func_d'].mean()]
    df1['wb_d_avg'] = [df['wb_d'].mean()]
    
    return df1


def print_averages(date1, date2):

    df_avg = average_df(date1, date2)
    
    df_avg = df_avg.set_index([pd.Index([''])])
    
    print(tabulate(df_avg, headers='keys', tablefmt='psql'))
    

def get_mood_df(date1, date2):
    
    df = filter_by_dates(df_assess_fl, date1, date2)
    
    df_mood = df[['phq_mood_score_first', 'phq_mood_score_last']].dropna()
    
    df_mood['cat_first'] = df_mood['phq_mood_score_first'].apply(get_mood_cat)
    df_mood['cat_last'] = df_mood['phq_mood_score_last'].apply(get_mood_cat)
       
    df_mood['cat_change'] = df_mood['cat_first'] + ' to ' + df_mood['cat_last']
    
    return df_mood


def get_mood_cat(num):
    if num >= 0 and num <= 2:
        cat = 'Normal'
    elif num >= 3 and num <= 5:
        cat = 'Middle'
    elif num >= 6 and num <= 8:
        cat = 'Moderate'
    elif num >= 9 and num <= 12:
        cat = 'Severe'
    return cat


def mood_change(date1, date2):
    
    df_mood = get_mood_df(date1, date2)
    
    return df_mood['cat_change'].value_counts()
