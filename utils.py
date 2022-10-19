# Configuration
import pandas as pd
from numpy import nan
import os
import datetime as dt
import ndjson
import requests
import math

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


def string_array_to_array (response_array):
    a = response_array.str.replace('"','').str.replace("'","") #remove quotes
    a = a.str.replace(' ','')

    b = a.str.slice(start=1,stop=-1) #remove the front and back brackets

    c = b.str.split(',') #split by the commas

    return c


def vars_from_array_response (response_arrays, var_dict): # assigns each variable from the array of responses on each line
    
    for response_array in response_arrays: 
        if type(response_array) == float: # they are all undefined or none of them are
            for key in var_dict:
                var_dict[key].append(nan)
        else:
            i = 0
            for key in var_dict: # iterate through the item and assign; dict is in order
                var_dict[key].append(response_array[i])
                i +=1
    return var_dict


def dict_to_vars (df, var_dict, undefined=False):
    
    for key in var_dict: 
#         print(len(var_dict[key]), len(df))
        if key not in df.columns:
            df[key] = var_dict[key]          
    return df

def reverse (series, const):
    return const - series

def assemble_df(csvs, desired_col):
    df = pd.DataFrame()
    for csv in csvs:
        header = pd.read_csv(csv, nrows=0)
        cols_intersect = list(set(header) & set(desired_col))
        
        if len(df) == 0:
            df = pd.read_csv(csv, usecols=cols_intersect)
            
        else:
            df = df.append(pd.read_csv(csv, usecols=cols_intersect))
    return df
   
                
