import pandas as pd
from scipy import stats
from numpy import nan
from datetime import datetime
import os
import tabulate
import re

def file_aggregator (data_path, file_list):

    i = 0 
    for f in file_list:
        new_df = pd.read_csv(os.path.join(data_path, f))
        if i == 0:
            df = new_df
        else:
            df = df.append(new_df)
        i += 1

    return df


def get_files (folder_path, months):
# For event-based files where only one thing happened for each line df's can be appended rather than merged
    files = []
    year_month_regex = re.compile('\d{6}')
    
    for file in os.listdir(folder_path):
        if year_month_regex.search(file).group() in months:
            files.append(file)
    return files


def get_user_files(folder_path, file_list, user_var="Distinct ID"):
# For user-based files with a tally of the number of times a thing happened for each uuid
    file_path = os.path.join(folder_path, file_list[0])
    df = pd.read_csv (file_path, keep_default_na=False).drop('Event', axis=1)
    
    for file in file_list[1:]:
        file_path = os.path.join(folder_path, file)
        new_df = pd.read_csv (file_path, keep_default_na=False)
        
        df = pd.merge (df, new_df.drop('Event', axis=1), on=user_var, how="outer")
        
    return df


def string_array_to_array (response_array):
    a = response_array.str.replace('"','') #remove quotes

    b = a.str.slice(start=1,stop=-1) #remove the front and back brackets

    c = b.str.split(',') #split by the commas

    return c


def vars_from_array_response (response_arrays, var_dict): # assigns each variable from the array of responses on each line
    
    for response_array in response_arrays: #
        if response_array[0] == 'undefined': # they are all undefined or none of them are
            for key in var_dict:
                var_dict[key].append('undefined')
        else:
            i = 0
            for key in var_dict: # iterate through the item and assign; dict is in order
                var_dict[key].append(response_array[i])
                i +=1
    return var_dict

def ndefine (items):
    for item in items:
        if item[0] == 'ndefine':
            item[0] = 'undefined'
    return items


def dict_to_vars (df, var_dict, undefined=False):
    
    for key in var_dict: 
#         print(len(var_dict[key]), len(df))
        if key not in df.columns:
            df[key] = var_dict[key]
            if undefined == True:
                df[[key]] = df[[key]].replace('undefined', nan).astype(float)
                          
    return df

def reverse (df, const):
    return const - df

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
    

def corr(df, var_list):
    display(df[var_list].corr())

