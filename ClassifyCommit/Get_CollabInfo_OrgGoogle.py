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




MAX_ROWS_PERWRITE = 0 # keep it zero as a you are writting multiple files

DF_REPO = pd.DataFrame()
DF_COUNT = 0

def appendrowindf(user_xl, row, df_flag = 0):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
        
    # note there is an issue when shape is used for series and df. 
    if df_flag == 0:
        DF_REPO= DF_REPO.append(pd.DataFrame(row).T, ignore_index = True)
        DF_COUNT = DF_COUNT + 1 # use row.shape[0] for dataframe
    else:
        # row = row.reset_index(drop=True)
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

def monthwise(contri):
    #get month information for each commit
    contri.columns = ['NAN', 'C_TYPE','C_NAME','CONTRIBUTIONS','PULLS','S_DATE','E_DATE']

    contri= contri.assign( s_month =   pd.to_numeric(contri['S_DATE'].str.split('-').str[1]))
    # contri= contri.assign( s_month =   pd.DatetimeIndex(pd.to_datetime(contri['S_DATE'], infer_datetime_format=True,errors='coerce')).month)
    # contri = contri.assign( s_day =   pd.DatetimeIndex(pd.to_datetime(contri['S_DATE'], infer_datetime_format=True,errors='coerce')).day)
    
    contri= contri.assign( e_month =   pd.to_numeric(contri['E_DATE'].str.split('-').str[1]))
    # contri= contri.assign( e_month =   pd.DatetimeIndex(pd.to_datetime(contri['E_DATE'], infer_datetime_format=True,errors='coerce')).month)
    # contri = contri.assign( e_day =   pd.DatetimeIndex(pd.to_datetime(contri['E_DATE'], infer_datetime_format=True,errors='coerce')).day)

    contri['Collaborator']= contri['C_TYPE'].apply( lambda x: 1 if x == 'Collaborator' else 0)
    contri['Contributor']= contri['C_TYPE'].apply( lambda x: 1 if x == 'Contributor' else 0)
    df2 = pd.DataFrame({'month' : [1,2,3,4,5,6,7,8,9,10,11,12]})
    df2 = df2.set_index('month')
    df2 = pd.concat([df2,contri.groupby('s_month')['Collaborator'].sum().to_frame('start_colab')], axis=1)
    df2 = pd.concat([df2, contri.groupby('e_month')['Collaborator'].sum().to_frame('end_colab')], axis=1)
    df2 = pd.concat([df2,contri.groupby('s_month')['Contributor'].sum().to_frame('start_cont')], axis=1)
    df2 = pd.concat([df2, contri.groupby('e_month')['Contributor'].sum().to_frame('end_cont')], axis=1)
    df2 = df2.fillna(0)

    # df2.rename(columns={"index": "month"}, inplace = True)

    return df2

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
    

    contributors = pd.DataFrame()
    collaborators = pd.DataFrame()
    
    # if login  not found populate with user name 

    df_commit['commit_author_user_login'] = df_commit.apply( lambda x: x['commit_author_name'] if pd.isnull(x['commit_author_user_login']) else x['commit_author_user_login'], axis = 1)
    df_commit['commit_committer_user_login'] = df_commit.apply( lambda x: x['commit_committer_name'] if pd.isnull(x['commit_committer_user_login']) else x['commit_committer_user_login'], axis = 1)

    collaborators = pd.DataFrame(df_commit['commit_committer_user_login'].unique(), columns = ['contributor_login'])
    contributors = pd.DataFrame(df_commit['commit_author_user_login'].unique(), columns = ['contributor_login'])

    #remove collbaorators from contributors
    contributors = contributors.merge(collaborators, left_on=['contributor_login'], right_on=['contributor_login'],  how='left', indicator=True)
    contributors = contributors[contributors['_merge']=='left_only']
    contributors = contributors.drop(columns=[ '_merge'])
    contributors = contributors.assign(contributor_type = 'Contributor')
    collaborators = collaborators.assign(contributor_type = 'Collaborator')
    contributors = contributors.append(collaborators, ignore_index = True )
    appendrowindf(w_commit_xl, df_commit, df_flag = 1)
    
    return df_commit, contributors

def  find_startenddays(df_commit, contributors):
    """ Find the start and end dates for the contributors and collaborators"""
    n_contributors = pd.DataFrame()
    for i,c in contributors.iterrows():
        contri_dates = df_commit[df_commit['commit_author_user_login'] == c['contributor_login']][['commit_authoredDate','commit_author_name','commit_author_user_login','commit_authoredDate_2008yearmonth','commit_authoredDate_yearmonth']]
        contri_dates.columns = ['commit_Date','commit_user_name','commit_user_login','commit_Date_2008yearmonth','commit_Date_yearmonth']


        colab_dates = df_commit[df_commit['commit_committer_user_login'] == c['contributor_login']][['commit_committedDate','commit_committer_name','commit_committer_user_login','commit_committedDate_2008yearmonth','commit_committedDate_yearmonth']]
        colab_dates.columns = ['commit_Date','commit_user_name','commit_user_login','commit_Date_2008yearmonth','commit_Date_yearmonth']

        full_contri_dates = pd.concat([contri_dates,colab_dates], axis=0)
        full_contri_dates = full_contri_dates.reset_index(drop=True) 

        n_row = pd.concat([pd.DataFrame(c).T.reset_index(drop=True), pd.DataFrame([full_contri_dates['commit_Date_2008yearmonth'].min()], columns = ['contributor_start_2008yearmonth']), pd.DataFrame([full_contri_dates['commit_Date_2008yearmonth'].max()], columns = ['contributor_end_2008yearmonth'])], axis = 1)
        
        # Add date info 
        n_row = pd.concat([n_row, pd.DataFrame([full_contri_dates['commit_Date_yearmonth'].min()], columns = ['contributor_start_yearmonth']) ,  pd.DataFrame([full_contri_dates['commit_Date_yearmonth'].max()], columns = ['contributor_end_yearmonth'])], axis = 1)

        n_row = pd.concat([n_row,pd.DataFrame([full_contri_dates['commit_Date'].min()], columns = ['contributor_start_date']),pd.DataFrame([full_contri_dates['commit_Date'].max()], columns = ['contributor_end_date']) ], axis = 1)
        
        n_row = pd.concat([n_row,pd.DataFrame([len(contri_dates)], columns = ['total_author_contributions']),pd.DataFrame([len(colab_dates)], columns = ['total_committing_contributions']) ], axis = 1)
       
        n_contributors = n_contributors.append(n_row)

                
    return n_contributors.reset_index(drop=True)
     
def main():
    global DF_REPO 
    global DF_COUNT
    r_commit_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\classified_google_commit_2.xlsx"
    w_commit_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\new_classified_google_commit_2.xlsx"
    w_colab_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\col_classified_google_commit_2.xlsx"

    commit_df = pd.read_excel(r_commit_xl,header= 0)
    df_commit = pd.DataFrame()
    df_commit.to_excel(w_commit_xl, index = False) 
    
    contributors = pd.DataFrame()
    contributors.to_excel(w_colab_xl , index = False) 
    commit_df['repo_name'] = commit_df['repo_name'].fillna(method='ffill')
    for i,row in commit_df.iterrows():
        if  pd.notnull(row['org_login']):
            print("Repo ",row['repo_name'])
            org_name= str(row['org_login'])
            
            if len(df_commit) > 0 :
                df_commit = parsedate(df_commit)
                df_commit, contributors = getcollab(df_commit,w_commit_xl)
                contributors= find_startenddays(df_commit, contributors)

                appendrowindf(w_colab_xl, contributors, df_flag = 1)


            df_commit = pd.DataFrame()
            appendrowindf(w_commit_xl, row) # write repo info
            appendrowindf(w_colab_xl, row) # write repo info
        else:
            df_commit = df_commit.append(row, ignore_index = True)
            
    if len(df_commit) > 0 :
        df_commit = parsedate(df_commit)
        df_commit, contributors  = getcollab(df_commit, w_commit_xl)
        contributors= find_startenddays(df_commit, contributors)

        appendrowindf(w_colab_xl, contributors, df_flag = 1)

        
    if  DF_COUNT  > 0:           
        df = pd.read_excel(w_commit_xl ,header= 0)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(w_commit_xl , index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()    
    
    
main()