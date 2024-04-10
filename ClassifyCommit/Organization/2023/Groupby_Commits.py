# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 22:01:51 2021

@author: pmedappa
"""


import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib')
    print(sys.path)
from poo_ghmodules import getGitHubapi
from poo_ghmodules import ghpaginate
from poo_ghmodules import ghparse_row
from poo_ghmodules import gettoken

import math
import pandas as pd
import numpy as np
import requests




MAX_ROWS_PERWRITE = 10000

GLOBAL_DF = dict()


def inintiateglobaldf(filenames):
    global GLOBAL_DF
    
    for f in filenames:
        GLOBAL_DF["DF_REPO_"+f] = pd.DataFrame()
        GLOBAL_DF["DF_COUNT_"+f] = 0
    

def appendrowindf(user_xl, row, df_flag = 0, filename ="" ):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global GLOBAL_DF
    DF_REPO = GLOBAL_DF["DF_REPO_"+filename]
    DF_COUNT = GLOBAL_DF["DF_COUNT_"+filename]
        
    # note there is an issue when shape is used for series and df. 
    if df_flag == 0:
        DF_REPO= DF_REPO.append(pd.DataFrame(row).T, ignore_index = True)
        DF_COUNT = DF_COUNT + 1 # use row.shape[0] for dataframe
    else:

        DF_REPO= DF_REPO.append(row, ignore_index = True)
        DF_COUNT = DF_COUNT + row.shape[0]
        
    if DF_COUNT >= MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,header= 0)
        df= df.append(DF_REPO, ignore_index = True)
        writer = pd.ExcelWriter(user_xl,options={'strings_to_urls': False})
        df.to_excel(writer , index = False) 
        writer.close()
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()

    GLOBAL_DF["DF_REPO_"+filename] = DF_REPO
    GLOBAL_DF["DF_COUNT_"+filename] = DF_COUNT       
    

def monthwise(df_commit,w_commit_xl):
    #Aggregate over the entrie author month

    df2 = pd.DataFrame()
    df2 = df_commit.groupby('commit_authoredDate_yearmonth')['Noveltys2p1','Noveltys2p2','Noveltys2p3','Usefulnesss2p1',
                                                      'Usefulnesss2p2','Usefulnesss2p3','CommitType_features2p1',
                                                      'CommitType_features2p2','CommitType_bugs2p1','CommitType_bugs2p2',
                                                      'CommitType_docs2p1','CommitType_docs2p2','CommitType_peers2p1',	
                                                      'CommitType_peers2p2','CommitType_processs2p1','CommitType_processs2p2',	
                                                      'CommitType_tests2p1','CommitType_tests2p2',
                                                      'commit_authors_totalCount','commit_authoredByCommitter','commit_additions',
                                                      'commit_deletions','commit_changedFiles','commit_parents_totalCount'].mean()
    
    df2 = pd.concat([df2,df_commit.groupby('commit_authoredDate_yearmonth')['Noveltys2p1'].count().to_frame('total_month_contributions')], axis=1)
    df2= df2.reset_index()


    appendrowindf(w_commit_xl, df2, df_flag = 1, filename = 'w_commit_xl')
    


def parsedate(df_commit):
    """Split date into year month and day columns, aggregate month strating from 2008-01-01"""
    # Author date
    df_commit['commit_authoredDate_year'] = pd.to_numeric(df_commit['commit_authoredDate'].str.split('-').str[0])
    df_commit['commit_authoredDate_month'] = pd.to_numeric(df_commit['commit_authoredDate'].str.split('-').str[1])
    
    df_commit['commit_authoredDate_2008yearmonth'] = (df_commit['commit_authoredDate_year'] - 2008) *12 + df_commit['commit_authoredDate_month'] 

    #Commit date

    df_commit['commit_committedDate_year'] = pd.to_numeric(df_commit['commit_committedDate'].str.split('-').str[0])
    df_commit['commit_committedDate_month'] = pd.to_numeric(df_commit['commit_committedDate'].str.split('-').str[1])
    
    df_commit['commit_committedDate_2008yearmonth'] = (df_commit['commit_committedDate_year'] - 2008) *12 + df_commit['commit_committedDate_month']         
    df_commit = df_commit.assign(commit_committedDate_yearmonth = df_commit['commit_committedDate_2008yearmonth'] - df_commit['commit_authoredDate_2008yearmonth'].min())    
    df_commit = df_commit.assign(commit_authoredDate_yearmonth = df_commit['commit_authoredDate_2008yearmonth'] - df_commit['commit_authoredDate_2008yearmonth'].min())    

    return df_commit
    
def  getcollab(df_commit, w_commit_xl):
    """Identify colllaborators and contributors using commit information"""
    
  
    # if login  not found populate with user name 

    df_commit['commit_author_user_login'] = df_commit.apply( lambda x: x['commit_author_name'] if pd.isnull(x['commit_author_user_login']) else x['commit_author_user_login'], axis = 1)
    df_commit['commit_committer_user_login'] = df_commit.apply( lambda x: x['commit_committer_name'] if pd.isnull(x['commit_committer_user_login']) else x['commit_committer_user_login'], axis = 1)

  
    
    monthwise(df_commit,w_commit_xl)
    
    return df_commit

     
def main():
    global GLOBAL_DF

    r_commit_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\2023\microsoft\classified_microsoft_commit_5.xlsx"
    w_commit_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\2023\microsoft\month_classified_microsoft_commit_5.xlsx"
     
                   
    write_files =   ['w_commit_xl']
    inintiateglobaldf(['w_commit_xl'])
    
    commit_df = pd.read_excel(r_commit_xl,header= 0)
    df_commit = pd.DataFrame()
    df_commit.to_excel(w_commit_xl, index = False) 
    

    commit_df['repo_name'] = commit_df['repo_name'].fillna(method='ffill')
    for i,row in commit_df.iterrows():
        if  pd.notnull(row['org_login']):
            print("Repo ",row['repo_name'])
            org_name= str(row['org_login'])
            
            if len(df_commit) > 0 :
                df_commit = parsedate(df_commit)
                df_commit = getcollab(df_commit,w_commit_xl)
 
            df_commit = pd.DataFrame()
            appendrowindf(w_commit_xl, row,filename = 'w_commit_xl') # write repo info
        else:
            df_commit = df_commit.append(row, ignore_index = True)
            
    if len(df_commit) > 0 :
        df_commit = parsedate(df_commit)
        df_commit  = getcollab(df_commit, w_commit_xl)
     
    for f in write_files:  
        
        if  GLOBAL_DF["DF_COUNT_"+f]  > 0:   
            if f == "w_commit_xl":
                df = pd.read_excel( w_commit_xl,header= 0)
                df= df.append(GLOBAL_DF["DF_REPO_"+f], ignore_index = True)
                df.to_excel(w_commit_xl , index = False) 

            GLOBAL_DF["DF_COUNT_"+f] = 0
            GLOBAL_DF["DF_REPO_"+f] = pd.DataFrame()    
    
    
main()