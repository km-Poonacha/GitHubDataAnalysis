# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 10:05:29 2019

@author: kmpoo
"""

import csv


import pandas as pd
import numpy as np

def dfInit(csv):
    pd.options.display.max_rows = 4
    pd.options.display.float_format = '{:.3f}'.format
    dataframe = pd.read_csv(csv, sep=",",error_bad_lines=False,header= 0, low_memory=False, encoding = "Latin1")
    return dataframe



  
def main():
    csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190604.csv"
    Topicdata = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190604T.csv"
    df = dfInit(csv)
    df_t = pd.DataFrame()
    df = df.assign(DESC_WORDS = lambda x: df['DESCRIPTION'].str.lower().str.split())
    df = df.assign(TD_WORDS = lambda x: df['DESC_WORDS'].append(df['TOPICS']))
    print(df['TD_WORDS'])
#    for index, row in df['OWNER_COUNTRY'].iteritems():        
#        if df_country['COUNTRY'].isin([row]).any():
#            df_c = df_c.append((df_country.loc[df_country['COUNTRY'] == row]), ignore_index = True)
#        else:
#            df_e = pd.DataFrame(["","","","","","","","",""]).T
#            df_e.columns = ['COUNTRY','GH_IND','GH_MAS','EUROPE','SP_2014','SP_2015','SP_MISC','SP','GH_AVG']
#            df_c = df_c.append(df_e, ignore_index = True)
#    n_df = pd.concat([df.reset_index(drop=True),df_c.reset_index(drop=True)], axis=1)
#    
#    n_df.to_csv(IVdata)
    
    
main()   
    