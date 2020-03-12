#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 16:54:21 2017

Get all users whos location is US and Canada
"""


import csv
import sys
if "C:\\Users\\pmedappa\\\Dropbox\\HEC\\Python\\CustomLib\\PooLIB" not in sys.path:
    sys.path.append('C:\\Users\\pmedappa\\\Dropbox\\HEC\\Python\\CustomLib\\PooLIB')
    print(sys.path)
from poo_ghmodules import getGitHubapi
from poo_ghmodules import ghpaginate
from poo_ghmodules import ghparse_row
from datetime import datetime

import pandas as pd
import numpy as np

MAX_ROWS_PERWRITE = 1000

DF_REPO = pd.DataFrame()
DF_COUNT = 0
PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW\PW_GitHub.csv'
LOG_CSV = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\MS Acquire Github\Data\Sponsor\UserSpon_log.csv'


user_xl = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\MS Acquire Github\Data\Sponsor\Users_USCAN.xlsx'

                 
def appendrowindf(NEWREPO_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(pd.Series(row), ignore_index = True)
    DF_COUNT = DF_COUNT + 1
    if DF_COUNT == MAX_ROWS_PERWRITE :
        df = pd.read_excel(NEWREPO_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(NEWREPO_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()
        
def main():
    global DF_REPO 
    global DF_COUNT
    search_key = ['us']#,'usa','states','america','canada','california','ca']
    period = ['2018-05-31..2019-01-01','2018-01-01..2018-06-01',
              '2017-05-31..2018-01-01','2017-01-01..2017-06-01',
              '2016-05-31..2017-01-01','2016-01-01..2016-06-01',
              '2015-05-31..2016-01-01','2015-01-01..2015-06-01',
              '2014-05-31..2015-01-01','2014-01-01..2014-06-01',
              '2013-05-31..2014-01-01','2013-01-01..2013-06-01',
              '2012-05-31..2013-01-01','2012-01-01..2012-06-01',
              '2011-05-31..2012-01-01','2011-01-01..2011-06-01',
              '2010-05-31..2011-01-01','2010-01-01..2010-06-01',
              '2009-05-31..2010-01-01','2009-01-01..2009-06-01',
              '2008-05-31..2009-01-01','2008-01-01..2008-06-01',]

    for loc in search_key:
        for p in period:        
            search_url = "https://api.github.com/search/users?q=repos:%3E5+location%3A"+loc+"+created:"+p+"+type:user&per_page=100"
            
            while search_url:
                user_res = getGitHubapi(search_url,PW_CSV,LOG_CSV)
                user_json = user_res.json()

                if int(user_json['total_count']) > 1000:
                    with open(LOG_CSV, 'at') as logobj:
                        l_data = list()
                        log = csv.writer(logobj)
                        l_data.append("Search Results Exceeds 1000")
                        l_data.append(search_url)
                        l_data.append(user_json['total_count'])
                        log.writerow(l_data)
                    print(search_url,"  ",user_json['total_count'])
                for user in user_json["items"]:
                    user_row = ghparse_row(user,"login", "id")
                    appendrowindf(user_xl, user_row)
                search_url = ghpaginate(user_res)
                
    if DF_COUNT < MAX_ROWS_PERWRITE:
        df = pd.read_excel(user_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
 
if __name__ == '__main__':
  main()
  