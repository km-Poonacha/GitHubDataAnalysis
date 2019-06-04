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
    df['OWNER_COUNTRY'].replace(['WOrld','Worldwide','Worldwise','Worlwide'], 'World')
    df['OWNER_COUNTRY'].replace(['The Netherlands'], 'Netherlands')
    df['OWNER_COUNTRY'].replace(['USA','us'], 'US')
    df['OWNER_COUNTRY'].replace(['Demark'], 'Denmark')
    df['OWNER_COUNTRY'].replace(['Austraila','Austrialia'], 'Australia')
    df['OWNER_COUNTRY'].replace(['South Korea'], 'Korea')
    df['OWNER_COUNTRY'].replace(['Hawaii'], 'US')
    df['OWNER_COUNTRY'].replace(['-'], '')
    df['OWNER_COUNTRY'].replace(['??'], '')


  
def main():
    csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190601.csv"
    country_csv = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\Country.csv"
    IVdata = "C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\IVdata.csv"
    with open(csv, 'rt', encoding="utf-8") as fileobj:
        csvhandle = csv.reader(fileobj)
        for row in csvhandle:
            owner_country = row[103]
            with open(country_ccsv, 'rt', encoding="utf-8") as fileobj2:
                countryIV = csv.reader(fileobj2)
                for country in countryIV:
                    if country[0] == owner_country:
                        
    
main()   
    
    
    
    
    