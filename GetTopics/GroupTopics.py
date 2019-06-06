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
    full_csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190604IV.csv"
    lang_csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\P_Topic.csv"
    Topicdata = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190604IVT.csv"
    df = dfInit(full_csv)
    df_t = pd.DataFrame()
    df_lang = dfInit(lang_csv)
    
    for index, row in df['MAIN_LANGUAGE'].iteritems():        
        if df_lang['PROGRAMMING_LANGUAGE'].isin([row]).any():
            df_t = df_t.append((df_lang.loc[df_lang['PROGRAMMING_LANGUAGE'] == row]), ignore_index = True)
        else:
            df_e = pd.DataFrame(["",""]).T
            df_e.columns = ['PROGRAMMING_LANGUAGE','P_TOPIC']
            df_t = df_t.append(df_e, ignore_index = True)
    n_df = pd.concat([df.reset_index(drop=True),df_t.reset_index(drop=True)], axis=1)
    
    n_df.to_csv(Topicdata)
    
    
main()   
    