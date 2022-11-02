import pandas as pd
import datetime as dt
import os
import tabulate

from numpy import nan
from dicts import *


def csvs_to_df(csvs=csvs, desired_col=desired_col):
    '''
    Load csvs to pandas dataframe. Iterates over chunks to save memory use.
    
    :csvs: list with the path to the csvs files
    :desired_col: columns we are interested in
    '''
    
    df = pd.DataFrame()
    
    for csv in csvs:
        
        header = pd.read_csv(csv, nrows=0)
        cols_intersect = list(set(header) & set(desired_col))
        
        iter_csv = pd.read_csv(csv, 
                               iterator=True, 
                               chunksize=100000,
                               usecols=cols_intersect, 
                               low_memory=False)
        
        df0 = pd.concat([chunk for chunk in iter_csv])
        
        if len(df) == 0:
            df = df0.copy()        
        else:
            df = df.append(df0)
    return df


def process_df(df):
    
    df['timestamp'] = pd.to_datetime(df['time'])
    df.drop('time', axis=1, inplace=True)
    
    df.sort_values(by='timestamp', inplace=True, ascending=True) # sort by date
    df.reset_index(drop=True, inplace=True)
    
    df = move_col_to_first_pos(df, 'timestamp')
    
    return df


def move_col_to_first_pos(df, col_name):
    '''
    shift column 'col_name' to first position
    
    :df:
    :col_name:
    '''
    
    first_column = df.pop(col_name)
    df.insert(0, col_name, first_column)
    
    return df


def string_array_to_array(response_array, m=25):
    '''
    Convert string with response array into a list of responses. 
    Convert new list elements (which are strings) into integers.
    
    :response_array: string with response array
    :m: int with response array length (default to 25)
    :returns: list with response array formated
    '''
    
    if type(response_array) == float: # they are all NaN or none of them are
        response_list = [nan]*m # there are 25 answers to 25 questions (m=25)
    else:
        response_list = response_array.split(',') # split by the commas      
        response_list = [process_list_elem(elem) for elem in response_list]

    return response_list


def process_list_elem(elem):
    '''
    :elem: list element (string)
    '''
    
    try:
        new_elem = int(elem.replace("[","").replace("]","").replace("'",""))
    except:
        new_elem = elem.replace("[","").replace("]","").replace("'","")
        
    return new_elem


def get_df_assess(df, desired_col=assess_desired_col):
    '''
    :df: pandas dataframe
    '''
    df_assess = df[assess_desired_col].copy()
    
    df_assess = df_assess[df_assess['event']=='Complete Assessment']
    df_assess.drop('event', axis=1, inplace=True)
    
    
    df_assess['question_scores'] = df_assess['question_scores'].apply(lambda x: string_array_to_array(x))
    df_assess['question_labels'] = df_assess['question_labels'].apply(lambda x: string_array_to_array(x)) 
                                                      
    df_assess['category_scores'] = df_assess['category_scores'].apply(lambda x: string_array_to_array(x, m=3))
    df_assess['categories'] = df_assess['categories'].apply(lambda x: string_array_to_array(x, m=3))
    

    df_assess[var_names] = pd.DataFrame(df_assess['question_scores'].tolist(), index=df_assess.index)
    
    
    col_names = df_assess.columns
    
    df_assess = df_assess.assign(phq_gad_score = df_assess[['phq_gad_1', 'phq_gad_2']].mean(axis=1)*2)
    df_assess = df_assess.assign(phq_dep_score = df_assess[['phq_dep_1', 'phq_dep_2']].mean(axis=1)*2)
    df_assess = df_assess.assign(phq_mood_score = df_assess.phq_gad_score + df_assess.phq_dep_score)
    df_assess = df_assess.assign(wemwbs_score = df_assess[col_names[14:28]].mean(axis=1)*14)
    df_assess = df_assess.assign(wsas_score = df_assess[col_names[29:34]].mean(axis=1)*5)
    
 
    df_assess[scores] = pd.DataFrame(df_assess['category_scores'].tolist(), index=df_assess.index)
    
    df_assess['gs_mood_score'] = reverse(df_assess.gs_mood_score, 12)
    df_assess['gs_wsas_score'] = reverse(df_assess.gs_wsas_score, 40)
    df_assess['phq_mood_score'] = reverse(df_assess.phq_mood_score, 12)
    df_assess['phq_dep_score'] = reverse(df_assess.phq_dep_score, 12)
    df_assess['phq_gad_score'] = reverse(df_assess.phq_gad_score, 12)
    df_assess['wsas_score'] = reverse(df_assess.wsas_score, 40)
    
    df_assess.sort_values(by=['distinct_id','timestamp'], inplace=True, ascending=True) # sort by ID and date
    
    df_assess.reset_index(drop=True, inplace=True)
    
    return df_assess


def reverse (series, const):
    '''
    :series: pandas dataframe column
    :const: int
    '''
    return const - series


def save_pd_to_csv(df, file_name):
    '''
    Save dataframe to csv file
    
    :df:
    :file_name:
    '''
    path = os.path.join('csvs/processed', file_name)
    
    df.to_csv(path)
    
    
def df_from_csv(file_name):
    
    folder = 'csvs/processed'
    path = os.path.join(folder, file_name)
    
    return pd.read_csv(path, index_col=0, low_memory=False)
    
    
def get_first_last_assess(df_assess):
    '''
    Get first assessments, last assessments, and first and last for each uuid on a single line
    '''
    
    df_assess_first = df_assess.groupby('distinct_id', as_index=False).first()
    df_assess_last = df_assess.groupby('distinct_id', as_index=False).last()
    df_assess_last = df_assess_last.add_suffix('_last')
    df_assess_fl = df_assess_first.join(df_assess_last)

    # add a week index
    # df_assess_first['week_index'] = ((df_assess_first.timestamp - dt.datetime(2021, 1, 1)).dt.days / 7).astype(int)
    # df_week = df_assess_first.groupby(df_assess_first.week_index).mean()
    
    df_assess_fl['total_time'] = (df_assess_fl.timestamp_last - df_assess_fl.timestamp).dt.days
    df_assess_fl['dep_diff'] = (df_assess_fl.phq_dep_score_last - df_assess_fl.phq_dep_score)
    df_assess_fl['anx_diff'] = (df_assess_fl.phq_gad_score_last - df_assess_fl.phq_gad_score)
    df_assess_fl['mood_diff'] = (df_assess_fl.phq_mood_score_last - df_assess_fl.phq_mood_score)
    df_assess_fl['func_diff'] = (df_assess_fl.wsas_score_last - df_assess_fl.wsas_score)
    df_assess_fl['wb_diff'] = (df_assess_fl.wemwbs_score_last - df_assess_fl.wemwbs_score)

    dep_sd = df_assess_fl['dep_diff'].std()
    anx_sd = df_assess_fl['anx_diff'].std()
    mood_sd = df_assess_fl['mood_diff'].std()
    func_sd = df_assess_fl['func_diff'].std()
    wb_sd = df_assess_fl['wb_diff'].std()

    df_assess_fl['dep_d'] = (df_assess_fl['dep_diff'] / dep_sd)
    df_assess_fl['anx_d'] = (df_assess_fl['anx_diff'] / anx_sd)
    df_assess_fl['mood_d'] = (df_assess_fl['mood_diff'] / mood_sd)
    df_assess_fl['func_d'] = (df_assess_fl['func_diff'] / func_sd)
    df_assess_fl['wb_d'] = (df_assess_fl['wb_diff'] / wb_sd)
    
    return df_assess_fl


def get_df_use_resource(df):
    
    filter1 = (df['event'] == 'Access Resource')
    filter2 = ((df['event'] == 'Page Load') & (df['url_name'] == 'resource-wrapper'))
    
    return df[filter1 | filter2]


def get_df_accesses(df_use_resource):
    
    #Groupby user and get the timestamps for each user
    timestamps_by_user = df_use_resource.groupby(['distinct_id']).timestamp

    # print(timestamps_by_user.get_group('179fbb0b04c71f-0bd657411479c4-48183301-13c680-179fbb0b04d231e'))

    # Dictionary of dictionaries
    # Top-level is users and then we get a count of each timestamp for each user
    # This will allow us to look up a given timestamp to see if there are duplicates

    timestamp_dict = {} # dict of dicts
    for group in timestamps_by_user.groups: # iterate through user timestamp lists
        timestamp_dict[group] = {} # create the users dict
        group_list = timestamps_by_user.get_group(group) # get the group list
    
        for timestamp in group_list: # iterate through the timestamps
            if timestamp in timestamp_dict[group]:
                timestamp_dict[group][timestamp] += 1
            else:
                timestamp_dict[group][timestamp] = 1
                
    def unique_row(row):
        if row['event'] == 'Access Resource':
            return True
        if timestamp_dict[row['distinct_id']][row['timestamp']] > 1:
            return False
        return True
                
    df_accesses = df_use_resource[df_use_resource.apply(unique_row, axis=1)]
    
    return df_accesses


def get_df_first_access(df_accesses):
    '''
    Compute the first non-null entry of each column.
    
    :df_accesses:
    '''

    df_first_access = df_accesses.groupby('distinct_id').first()
    df_first_access.rename(columns = {'timestamp': 'timestamp_access'}, inplace=True)
    
    #df_first_access.reset_index(drop=True, inplace=True)
    
    return df_first_access


def get_grouped_event(df, event_type, id_str, function=None, rename=None):
    
    df = df[df['event'] == event_type].groupby(id_str, as_index=False) # select event type and group
    
    if (not function is None):
        df = getattr(df, function)()
    
    if (not rename is None):
        df.rename(columns=rename, inplace=True)

    return df


def get_df_first_access_merged(df, df_accesses, df_first_access):
    '''
    Users who took assessments and accessed resources.
    
    :df:
    :df_first_access:
    '''
    
    df0 = get_grouped_event(df, 
                            event_type = 'Signup', 
                            id_str = 'distinct_id', 
                            function = 'first', 
                            rename = {'timestamp': 'timestamp_signup'})
    df0 = df0[['distinct_id', 'timestamp_signup']]
    df_first_access_merged = df_first_access.merge(df0, on='distinct_id', how="outer")
    
    
    df1 = get_grouped_event(df, 
                            event_type = 'Self Assessment Completed', 
                            id_str = 'distinct_id', 
                            function = 'first', 
                            rename = {'timestamp': 'timestamp_assess'})
    df1 = df1[['distinct_id', 'timestamp_assess']]
    df_first_access_merged = df_first_access_merged.merge(df1, on='distinct_id', how="left")
    
    
    df2 = get_grouped_event(df, 
                            event_type = 'Complete Assessment', 
                            id_str = 'distinct_id', 
                            function = 'count', 
                            rename = {'timestamp': 'assess_count'})
    df2 = df2[['distinct_id', 'assess_count']]
    df_first_access_merged = df_first_access_merged.merge(df2, on='distinct_id', how="left")
    
    
    df3 = get_grouped_event(df, 
                            event_type = 'Page Load', 
                            id_str = 'distinct_id', 
                            function='last', 
                            rename={'language': 'language_last'})
    df3 = df3[['distinct_id', 'language_last']]
    df_first_access_merged = df_first_access_merged.merge(df3, on='distinct_id', how="left")
    

    df_first_access_merged[['timestamp_signup', 'timestamp_access', 'timestamp_assess']] = \
    df_first_access_merged[['timestamp_signup', 'timestamp_access', 'timestamp_assess']].apply(pd.to_datetime, errors='coerce')

    df_first_access_merged['time_to_access'] = \
    df_first_access_merged['timestamp_access'] - df_first_access_merged['timestamp_signup'] 
    
    df_first_access_merged['time_to_access_30'] = df_first_access_merged['time_to_access'] < dt.timedelta(days=30)

    df_access_count = get_df_access_count(df_accesses)

    df_first_access_merged = df_first_access_merged.merge(df_access_count, on='distinct_id', how="outer")
    
    return df_first_access_merged


def get_df_access_count(df_accesses):
    
    df_access_count = df_accesses.groupby('distinct_id', as_index=False).count()[['distinct_id', 'event']]
    df_access_count.rename(columns = {'event': 'access_count'}, inplace=True)
    
    return df_access_count


def get_df_assess_fl_access(df_assess_fl, df_access_count):

    df_assess_fl_access = df_assess_fl.merge(df_access_count, on='distinct_id', how='left')
    df_assess_fl_access['access_count'] = df_assess_fl_access['access_count'].fillna(0)
    
    return df_assess_fl_access
       
    
def get_results_table (df, headers, score_vars, score_labels, std=True, d=False):
    
    # check for correct number of labels problems
    if len(score_vars) != len(score_labels): 
        raise Exception("score_vars and score_labels must have equal length")
    
    # set up our list of tuples from score_vars and score_labels
    tup_list = []
    for i in range(0, len(score_vars)):
        tup_list.append((score_vars[i], score_labels[i]))

    # create our tables based on what vars indicated
    table = []
    for tup in tup_list:
        label = tup[1]
        mean = df[[tup[0]]].mean()
        stdev = df[[tup[0]]].std()
        
        add_list = [label, mean]
        if std:
            add_list.append(stdev)
        if d:
            add_list.append(mean/stdev)
            
        table.append(add_list)
        
    # make sure headers is the right length
    if len(headers) != len(add_list): 
        raise Exception("incorrect number of headers")
    
        
    text_output = tabulate.tabulate(table, headers=headers, tablefmt='html', floatfmt=["",".2f",".2f",".2f",".0%",".2f",".3f"])

    display(text_output)
    
    
    
    
    
    














# expects a list of lists, each list containing two dates to pass to API

def download_raw_mixpanel(files):
    
    url = "https://data.mixpanel.com/api/2.0/export" # Raw export API url
    
    headers = {
        "Accept": "application/x-ndjson",
        "Authorization": "Basic MDg5NTY1MzJlNmQyNGM0MmE1YTk2YTc2NGI2ZDZjZmU6Vk5iWjRrNzYlIyZl"
    }
    
    for file in files:

        start = dt.datetime.now() # Timing it to keep track of speed and number of files completed
        
        # Get the response from the mixpanel API
        querystring = {"from_date": file[0],"to_date": file[1]}
        response = requests.request("GET", url, headers=headers, params=querystring)

        # Process the ndjson
        data = ndjson.loads(response.text)
        df = pd.json_normalize(data)

        # fix the column names: strip 'properties' and '$'
        
        columns = []
        for column in df.columns:
            column = column.replace("properties.", "").replace("$","")
            columns.append(column)

        df.columns = columns
        df.time = pd.to_datetime(df.time, unit='s') # datetimes!

        # output to csv
        df.to_csv("csvs/mixpanel_events_from_" + file[0] + "_to_" + file[1] + ".csv", index = False)

        finish = dt.datetime.now() # print progress
        print("finished file: ", file[0] + " to " + file[1] + " in ", (finish - start).seconds)
        