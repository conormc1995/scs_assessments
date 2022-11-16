from utils import df_from_csv

import pandas as pd
from tabulate import tabulate



file_name = 'mixpanel_users_assess_scores.csv'
df_assess_fl = df_from_csv(file_name)

df_assess_fl['timestamp_first'] = pd.to_datetime(df_assess_fl['timestamp_first']) 
df_assess_fl['timestamp_last'] = pd.to_datetime(df_assess_fl['timestamp_last'])


def get_date_range():
    
    Min = df_assess_fl['timestamp_first'].min().date()
    Max = df_assess_fl['timestamp_first'].max().date()
    
    print('Data from ' + str(Min) + ' to '+ str(Max))


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
    
    df1['num users'] = [len(df)]
    
    df1['Depression'] = [df['dep_d'].mean()] # average of the difference divided by std
    df1['Anxiety'] = [df['anx_d'].mean()]
    df1['Mood'] = [df['mood_d'].mean()]
    df1['Funcionality'] = [df['func_d'].mean()]
    df1['Well Being'] = [df['wb_d'].mean()]
    
    return df1


def print_averages(date1, date2, item='all', title=''):

    if item=='all':
        print('                        Average of the difference divided by std')
        df_avg = average_df(date1, date2)
    elif item == 'dep':
        title = 'Depression'
        df_avg = average_df_item(date1, date2, 'dep')
    elif item == 'anx':
        title = 'Anxiety'
        df_avg = average_df_item(date1, date2, 'anx')
    elif item == 'mood':
        title = 'Mood'
        df_avg = average_df_item(date1, date2, 'mood')
    elif item == 'func':
        title = 'Functionality'
        df_avg = average_df_item(date1, date2, 'func')
    elif item == 'wb':
        title = 'Well Being'
        df_avg = average_df_item(date1, date2, 'wb')
    
    df_avg = df_avg.set_index([pd.Index([''])])
    
    if item != 'all':
        print('')
        print('       '+title+'  -  Averages from '+str(date1)+' to '+str(date2))
        print('')
    print(tabulate(df_avg, headers='keys', tablefmt='psql'))
    
    
def average_df_item(date1, date2, item, dropna=True):
    '''
    
    item: 'dep', 'anx', 'mood', 'func', 'wb'
    '''
    
    df = filter_by_dates(df_assess_fl, date1, date2)
    
    if item == 'dep':
        first = 'phq_dep_score_first'
        last = 'phq_dep_score_last'
    elif item == 'anx':
        first = 'phq_gad_score_first'
        last = 'phq_gad_score_last'
    elif item == 'mood':
        first = 'phq_mood_score_first'
        last = 'phq_mood_score_last'
    elif item == 'func':
        first = 'wsas_score_first'
        last = 'wsas_score_last'
    elif item == 'wb':
        first = 'wemwbs_score_first'
        last = 'wemwbs_score_last'
    
    # get columns of interest
    df = df[['distinct_id', 'total_time',
             first, last,
             item+'_diff', 
             item+'_d']]
    
    if dropna: df = df.dropna(axis=0)
    
    df1 = pd.DataFrame() # create empty pandas dataframe
    
    df1['num users'] = [len(df)]    
    
    df1['First'] = [df[first].mean()] # average of the first score
    df1['Last'] = [df[last].mean()] # average of the last score
    df1['Diff'] = [df[item+'_diff'].mean()] # average of the difference
    df1['Diff/std'] = [df[item+'_d'].mean()] # average of the difference divided by std
    
    return df1
      

def get_mood_df(date1, date2):
    
    df = filter_by_dates(df_assess_fl, date1, date2)
    
    df_mood = df[['phq_mood_score_first', 'phq_mood_score_last']].dropna()
    
    df_mood['cat_first'] = df_mood['phq_mood_score_first'].apply(get_mood_cat)
    df_mood['cat_last'] = df_mood['phq_mood_score_last'].apply(get_mood_cat)
       
    df_mood['cat_change'] = df_mood['cat_first'] + ' --> ' + df_mood['cat_last']
    
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

    print('---------------------------------------------')
    print('Mood change between '+str(date1)+' and '+str(date2))
    print('---------------------------------------------')
    
    df_mood = get_mood_df(date1, date2)
    
    return df_mood['cat_change'].value_counts()
