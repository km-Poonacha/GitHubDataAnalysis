# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 16:44:44 2019

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

def dfcleancountry(df):
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['WOrld','Worldwide','Worldwise','Worlwide','Workdwide','Wordwide',], 'World')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['The Netherlands'], 'Netherlands')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['USA','us'], 'US')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['Demark'], 'Denmark')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['Austraila','Austrialia'], 'Australia')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['South Korea'], 'Korea')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['Hawaii'], 'US')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['-'], '')
    df['OWNER_COUNTRY'] = df['OWNER_COUNTRY'].replace(['??'], '')
    return df

  
def main():
    csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190603.csv"
    country_csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\Country_Final.csv"
    IVdata = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190603IV.csv"
    df = dfInit(csv)
    df_country = dfInit(country_csv)
    df = dfcleancountry(df)
    df.to_csv("C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190603_clean.csv")
    df_c = pd.DataFrame()

    for index, row in df['OWNER_COUNTRY'].iteritems():        
        if df_country['COUNTRY'].isin([row]).any():
            df_c = df_c.append((df_country.loc[df_country['COUNTRY'] == row]), ignore_index = True)
        else:
            df_e = pd.DataFrame(["","","","","","","","",""]).T
            df_e.columns = ['COUNTRY','GH_IND','GH_MAS','EUROPE','SP_2014','SP_2015','SP_MISC','SP','GH_AVG']
            df_c = df_c.append(df_e, ignore_index = True)
    n_df = pd.concat([df.reset_index(drop=True),df_c.reset_index(drop=True)], axis=1)
    
    n_df.to_csv(IVdata)
    
    
main()   
    
    
    
    
    