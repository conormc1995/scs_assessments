from numpy import nan


csvs = ['csvs/aggregated/mixpanel_events_from_2020-04-05_to_2020-12-31.csv',
        'csvs/aggregated/mixpanel_events_from_2021-01-01_to_2021-06-30.csv',
        'csvs/aggregated/mixpanel_events_from_2021-07-01_to_2021-12-31.csv',
        'csvs/aggregated/mixpanel_events_from_2022-01-01_to_2022-06-30.csv']


desired_col = ['event', 'time', 'timestamp', 'distinct_id', 'browser', 'browser_version', 'device', 'initial_referrer',
               'initial_referring_domain', 'os', 'referrer', 'referring_domain', 'screen_height', 'screen_width',
               'language', 'url', 'url_name', 'source', 'slug', 'categories', 'category_scores', 'question_labels',
               'question_scores', 'response_id', 'total_score', 'total_score_normalized', 'mp_country_code', 'city', 
               'radio', 'region', 'organization', 'organization_name']  


assess_desired_col = ['event', 'timestamp', 'distinct_id', 'response_id', 'url_name', 'categories', 'category_scores',
                      'question_labels', 'question_scores', 'total_score']
    


var_names = ['phq_suicide', # Item  1    > suicidality
             'phq_gad_1', 'phq_gad_2', # Items 2:3  > gad2 anxiety
             'phq_dep_1', 'phq_dep_2', # Items 4:5  > phq2 depression
             'wemwbs_1', 'wemwbs_2', 'wemwbs_3', 'wemwbs_4','wemwbs_5', 'wemwbs_6', 'wemwbs_7', # Items 6:19 > wemwbs
             'wemwbs_8', 'wemwbs_9', 'wemwbs_10', 'wemwbs_11', 'wemwbs_12', 'wemwbs_13','wemwbs_14', 
              # Item  20   > "Are you retired or choose not to have a job for reasons unrelated to your mental health?"
             'retired',
             # WSAS, Items 21:25 0 not at all to 8 very severely (impaired)
             'wsas_1', # Item  21   > Because of my mental health, my ability to work is impaired.
             'wsas_2', # Item  22   > Because of my mental health, my home management (cleaning, tidying, cooking, looking after home or children, paying bills) is impaired.
             'wsas_3', # Item  23   > Because of my mental health, my social leisure activities (with other people) are impaired.
             'wsas_4', # Item  24   > Because of my mental health, my private leisure activities (done alone, such as reading, gardening, collecting, sewing, walking alone) are impaired.
             'wsas_5'] # Item  25   > Because of my mental health, my ability to form and maintain close relationships with others, including those I live with, is impaired.


scores = ['gs_mood_score', 'gs_wemwbs_score', 'gs_wsas_score']


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
