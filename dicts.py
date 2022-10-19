import pandas as pd
from scipy import stats
from numpy import nan
from datetime import datetime
import os

cat_to_num_dict = { # for recoding to numbers
    
    # phq9 french
    'Plus de sept jours':2, 
    'Presque tous les jours':3, 
    'Plus de la moitié des jours':2, 
    'Plusieurs jours':1, 
    'Jamais':0,
    
    # phq9 english
    'Nearly every day':3,
    'More than half the days':2, 
    'Several days':1, 
    'Not at all':0, 
    'undefined':nan,
    
    # retired question
    'false':0, 
    'true':1, 
    'undefined':nan
}

var_dict = {
    # Item  1    > suicidality
    'phq_suicide': [],   
    
    # Items 2:3  > gad2 anxiety
    'phq_gad_1' : [], 'phq_gad_2' : [], 
    
    # Items 4:5  > phq2 depression
    'phq_dep_1' : [], 'phq_dep_2' : [],  
    
    # Items 6:19 > wemwbs
    'wemwbs_1' : [],                     
    'wemwbs_2' : [],   
    'wemwbs_3' : [],
    'wemwbs_4' : [],
    'wemwbs_5' : [],
    'wemwbs_6' : [],
    'wemwbs_7' : [],
    'wemwbs_8' : [],
    'wemwbs_9' : [],
    'wemwbs_10' : [],
    'wemwbs_11' : [],
    'wemwbs_12' : [],
    'wemwbs_13' : [],
    'wemwbs_14' : [],
    
    # Item  20   > "Are you retired or choose not to have a job for reasons unrelated to your mental health?"
    'retired' : [],     
    
    # WSAS, Items 21:25 0 not at all to 8 very severely (impaired)
    'wsas_1' : [],    # Item  21   > Because of my mental health, my ability to work is impaired.  
    'wsas_2' : [],    # Item  22   > Because of my mental health, my home management (cleaning, tidying, cooking, looking after home or children, paying bills) is impaired.
    'wsas_3' : [],    # Item  23   > Because of my mental health, my social leisure activities (with other people) are impaired.
    'wsas_4' : [],    # Item  24   > Because of my mental health, my private leisure activities (done alone, such as reading, gardening, collecting, sewing, walking alone) are impaired.
    'wsas_5' : [],    # Item  25   > Because of my mental health, my ability to form and maintain close relationships with others, including those I live with, is impaired.
}

score_dict = {
    'gs_mood_score' : [],
    'gs_wemwbs_score' : [],
    'gs_wsas_score' : []
}
