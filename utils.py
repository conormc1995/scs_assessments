import pandas as pd
from numpy import nan
import datetime as dt
from dicts import *


def csvs_to_df(csvs=csvs, desired_col=assess_desired_col):
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
        
        # Filter only Completed Assessment
        df0 = pd.concat([chunk[chunk['event'] == 'Complete Assessment'] for chunk in iter_csv])
        
        if len(df) == 0:
            df = df0.copy()        
        else:
            df = df.append(df0)
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


def process_df(df):
    '''
    :df: pandas dataframe
    '''
    
    df = df.drop('event', axis=1)
    
    df['timestamp'] = pd.to_datetime(df['time'])
    df = df.drop('time', axis=1)
    
    df['question_scores'] = df['question_scores'].apply(lambda x: string_array_to_array(x))
    df['question_labels'] = df['question_labels'].apply(lambda x: string_array_to_array(x)) 
                                                      
    df['category_scores'] = df['category_scores'].apply(lambda x: string_array_to_array(x, m=3))
    df['categories'] = df['categories'].apply(lambda x: string_array_to_array(x, m=3))
    

    df[var_names] = pd.DataFrame(df['question_scores'].tolist(), index=df.index)
    
    
    col_names = df.columns
    
    df = df.assign(phq_gad_score = df[['phq_gad_1', 'phq_gad_2']].mean(axis=1)*2)
    df = df.assign(phq_dep_score = df[['phq_dep_1', 'phq_dep_2']].mean(axis=1)*2)
    df = df.assign(phq_mood_score = df.phq_gad_score + df.phq_dep_score)
    df = df.assign(wemwbs_score = df[col_names[14:28]].mean(axis=1)*14)
    df = df.assign(wsas_score = df[col_names[29:34]].mean(axis=1)*5)
    
 
    df[scores] = pd.DataFrame(df['category_scores'].tolist(), index=df.index)
    
    df['gs_mood_score'] = reverse(df.gs_mood_score, 12)
    df['gs_wsas_score'] = reverse(df.gs_wsas_score, 40)
    df['phq_mood_score'] = reverse(df.phq_mood_score, 12)
    df['phq_dep_score'] = reverse(df.phq_dep_score, 12)
    df['phq_gad_score'] = reverse(df.phq_gad_score, 12)
    df['wsas_score'] = reverse(df.wsas_score, 40)
    
    df.sort_values(by=['distinct_id','timestamp'], inplace=True, ascending=True) # sort by ID and date
    
    df.reset_index(drop=True, inplace=True)
    
    return df


def reverse (series, const):
    '''
    :series: pandas dataframe column
    :const: int
    '''
    return const - series














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
        

        
def csv_aggregator(folder_path):
    x = 0
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            new_df = pd.read_csv(folder_path + '/' + file, keep_default_na=False)
            if (x == 0):
                df = new_df
                x = 1
            else:
                df = df.append(new_df)
    
    return df

                
def get_grouped_event(df, event_type, id_str, function=None, rename=None):
    
    df = df[df['event'] == event_type].groupby(id_str, as_index=False) # select event type and group
    
    if (not function is None):
        df = getattr(df, function)()
    
    if (not rename is None):
        df.rename(columns=rename, inplace=True)

    return df
