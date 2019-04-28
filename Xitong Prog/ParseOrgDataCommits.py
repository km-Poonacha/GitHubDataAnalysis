# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 18:42:31 2019

@author: kmpoo
"""

import csv
import sys
from datetime import datetime
import matplotlib.pyplot as plt

import pandas as pd
import numpy as np

def dfInit(csv):
    pd.options.display.max_rows = 4
    pd.options.display.float_format = '{:.3f}'.format
    dataframe = pd.read_csv(csv, sep=",",error_bad_lines=False,header= 0, low_memory=False, encoding = "Latin1")
    return dataframe

def dfproc(dataframe,drop_col, ass_col):
    repo_df = dataframe[dataframe['REPO_ID'].notnull()]
    commit_df = dataframe[dataframe['REPO_ID'].isnull()]    
    repo_df = repo_df.assign(parsedate =  (pd.to_datetime(repo_df['CREATED_AT']).dt.year - 2012)*12 + pd.to_datetime(repo_df['CREATED_AT']).dt.month)
    repo_df = repo_df.assign(year =  (pd.to_datetime(repo_df['CREATED_AT']).dt.year ))

    commit_df = commit_df.drop(axis=1,columns=drop_col)
    print(commit_df)
    commit_df.columns = ass_col
    commit_df = commit_df.assign(parsedate =  (pd.to_datetime(commit_df['AUTHOR_DATE']).dt.year - 2012)*12 + pd.to_datetime(commit_df['AUTHOR_DATE']).dt.month)
    commit_df = commit_df.assign(year =  (pd.to_datetime(commit_df['AUTHOR_DATE']).dt.year))
    
    # calculate contributions per project
    repo_freq = repo_df['parsedate'].value_counts(normalize=False, sort=False, ascending=False, bins=None, dropna=True)
    commit_freq = commit_df['parsedate'].value_counts(normalize=False, sort=False, ascending=False, bins=None, dropna=True)
    df_rfreq = repo_freq.rename_axis('pdate').reset_index(name='repo_counts')
    df_rfreq = df_rfreq.sort_values(by=['pdate'])
    df_rfreq = df_rfreq.assign(cum_repocount = df_rfreq['repo_counts'].cumsum())
    df_cfreq = commit_freq.rename_axis('pdate').reset_index(name='commit_counts')
    df_cfreq=df_cfreq.sort_values(by=['pdate'])
    df_freq = df_cfreq.merge(df_rfreq, how='inner', on='pdate', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
    df_freq = df_freq.assign(commit_repocum = (df_freq['commit_counts']/df_freq['cum_repocount']))
    df_freq = df_freq.assign(commit_repo = (df_freq['commit_counts']/df_freq['repo_counts']))

    
    return repo_df,commit_df,df_freq

    
#    print(repo_freq)
#    print(commit_freq)

def plt_histtime(ser, f_save, xl, yl, title):
    plt.xticks(np.arange(30, 90, step=1))
    n, bins, patches = plt.hist(ser.dropna(), bins=np.arange(30, 90, 1),range = (30, 100), histtype='bar', align='mid', orientation='vertical', rwidth=None, label= ['Repos Created'])
    patches[48].set_fc('r')
    patches[52].set_fc('r')
    
    plt.xlabel(xl, fontsize=10)
    plt.ylabel(yl, fontsize=10)
    plt.suptitle(title)
    
    plt.savefig(f_save)
    

def plt_linetime(x_ser, y_ser, f_save, xl, yl, title):
    plt.xticks(np.arange(30, 90, step=1))
    lines = plt.plot(x_ser,y_ser) 
    plt.vlines(78,0,lines[0].get_data()[1][np.where(lines[0].get_data()[0] == 78)])
    plt.vlines(82,0,lines[0].get_data()[1][np.where(lines[0].get_data()[0] == 82)])
    plt.savefig(f_save)
    plt.xlabel(xl, fontsize=10)
    plt.ylabel(yl, fontsize=10)
    plt.suptitle(title)


def main():
     
    # For MS
    MS_rCSV = 'D:\\Xitong proj\Xitong proj MS\\MSRepoCommit_Repofreq.csv'
    MS_cCSV = 'D:\\Xitong proj\Xitong proj MS\\MSRepoCommit_Commitfreq.csv'
        
    MS_fCSV= 'D:\\Xitong proj\Xitong proj MS\\MSRepoCommit_Freq.csv'
    MS_Proj = 'D:\\Xitong proj\Xitong proj MS\\MS_Proj.png'
    MS_Com = 'D:\\Xitong proj\Xitong proj MS\\MS_Com.png'
    MS_Com_Proj = 'D:\\Xitong proj\Xitong proj MS\\MS_Com_Proj.png'
    MSCom_Projcum = 'D:\\Xitong proj\Xitong proj MS\\MS_Com_Projcum.png'
    
    df_full = pd.DataFrame()
    for i in range(1,13):
        REPO_CSV_MS = 'D:\\Xitong proj\Xitong proj MS\MSRepoCommit_'+str(i)+'.csv'
        print(REPO_CSV_MS)
        dataframe = dfInit(REPO_CSV_MS)
        df_full = df_full.append(dataframe)

    drop_col = ['REPO_NAME', 'WATCHERS', 'HAS_ISSUES','LANGUAGE','HAS_PROJECTS',	'HAS_DOWNLOADS',	'HAS_WIKI','HAS_PAGES','FORKS',	'MIRROR_URL',	'ARCHIVED','DISABLED','OPEN_ISSUES',	'LICENSE',	'FORKS.1',	'OPEN_ISSUES.1',	'WATCHERS.1','DEFAULT_BRANCH',	'PERMISSIONS']
    ass_col = ['REPO_ID','SHA','AUTHOR_NAME','AUTHOR_EMAIL','AUTHOR_DATE','COMMITTER_NAME','COMITTER_EMAIL','COMMIT_DATE','COMMENTS','URL']
    print(df_full)
    repo_df,commit_df,df_freq = dfproc(df_full,drop_col,ass_col )   
    del df_full
    del dataframe
    repo_df.to_csv(MS_rCSV)
    commit_df.to_csv(MS_cCSV)
    df_freq.to_csv(MS_fCSV)
    # Plot hist times
#    plt_histtime(repo_df['parsedate'],MS_Proj, 'Month', 'No of Repos', 'REPO CREATED PER MONTH')
#    plt_histtime(commit_df['parsedate'],MS_Com, 'Month', 'No of Commits', 'COMMITS PER MONTH')    
#    plt_linetime(df_freq['pdate'],df_freq['commit_repo'],MS_Com_Proj, 'Month', 'Commits per Repo', 'COMMITS PER REPO CREATED')
    plt_linetime(df_freq['pdate'],df_freq['commit_repocum'],MSCom_Projcum , 'Month', 'Commits per Repo', 'COMMITS PER REPO (CUMMULATIVE)')
     

    # For Google
    Google_rCSV = 'D:\\Xitong proj\Xitong proj Google\\GoogleRepoCommit_Repofreq.csv'
    Google_cCSV = 'D:\\Xitong proj\Xitong proj Google\\GoogleRepoCommit_Commitfreq.csv'

    Google_fCSV = 'D:\\Xitong proj\Xitong proj Google\\GoogleRepoCommit_Freq.csv'
    Google_Proj = 'D:\\Xitong proj\Xitong proj Google\\Google_Proj.png'
    Google_Com = 'D:\\Xitong proj\Xitong proj Google\\Google_Com.png'
    Google_Com_Proj = 'D:\\Xitong proj\Xitong proj Google\\Google_Com_Proj.png'
    GoogleCom_Projcum = 'D:\\Xitong proj\Xitong proj Google\\Google_Com_Projcum.png'
    
    df_full = pd.DataFrame()
    for i in range(1,8):
        REPO_CSV_MS = 'D:\\Xitong proj\Xitong proj Google\GoogleRepoCommit_'+str(i)+'.csv'
        print(REPO_CSV_MS)
        dataframe = dfInit(REPO_CSV_MS)
        df_full = df_full.append(dataframe)

    drop_col = ['STARS', 'WATCHERS', 'HAS_ISSUES','LANGUAGE','HAS_PROJECTS',	'HAS_DOWNLOADS',	'HAS_WIKI','HAS_PAGES','FORKS',	'MIRROR_URL',	'ARCHIVED','DISABLED','OPEN_ISSUES',	'LICENSE',	'FORKS.1',	'OPEN_ISSUES.1',	'WATCHERS.1','DEFAULT_BRANCH',	'PERMISSIONS']
    ass_col = ['REPO_ID','SHA','AUTHOR_NAME','AUTHOR_EMAIL','AUTHOR_DATE','COMMITTER_NAME','COMITTER_EMAIL','COMMIT_DATE','COMMENTS','URL']
    print(df_full)
    repo_df,commit_df,df_freq = dfproc(df_full,drop_col,ass_col )   
    del df_full
    del dataframe
    repo_df.to_csv(Google_rCSV)
    commit_df.to_csv(Google_cCSV)
    df_freq.to_csv(Google_fCSV)
    # Plot hist times
#    plt_histtime(repo_df['parsedate'],Google_Proj, 'Month', 'No of Repos', 'REPO CREATED PER MONTH')
#    plt_histtime(commit_df['parsedate'],Google_Com, 'Month', 'No of Commits', 'COMMITS PER MONTH')    
#    plt_linetime(df_freq['pdate'],df_freq['commit_repo'],Google_Com_Proj, 'Month', 'Commits per Repo', 'COMMITS PER REPO CREATED')
    plt_linetime(df_freq['pdate'],df_freq['commit_repocum'],GoogleCom_Projcum, 'Month', 'Commits per Repo', 'COMMITS PER REPO (CUMMULATIVE)')

   
    # For IBM 

    REPO_CSV = 'D:\\Xitong proj\Xitong proj IBM\IBMRepoCommit_work.csv'    
    IBM_rCSV = 'D:\\Xitong proj\Xitong proj IBM\\IBMRepoCommit_Repofreq.csv'
    IBM_cCSV = 'D:\\Xitong proj\Xitong proj IBM\\IBMRepoCommit_Commitfreq.csv'
        
    IBM_fCSV= 'D:\\Xitong proj\Xitong proj IBM\\IBMRepoCommit_Freq.csv'
    IBM_Proj = 'D:\\Xitong proj\Xitong proj IBM\\IBM_Proj.png'
    IBM_Com = 'D:\\Xitong proj\Xitong proj IBM\\IBM_Com.png'
    IBM_Com_Proj = 'D:\\Xitong proj\Xitong proj IBM\\IBM_Com_Proj.png'
    IBMCom_Projcum = 'D:\\Xitong proj\Xitong proj IBM\\IBM_Com_Projcum.png'

    dataframe = dfInit(REPO_CSV)
    drop_col = ['HAS_ISSUES','HAS_PROJECTS',	'HAS_DOWNLOADS',	'HAS_WIKI','HAS_PAGES','FORKS',	'MIRROR_URL',	'ARCHIVED','DISABLED','OPEN_ISSUES',	'LICENSE',	'FORKS.1',	'OPEN_ISSUES.1',	'WATCHERS.1','DEFAULT_BRANCH',	'PERMISSIONS']
    ass_col = ['REPO_ID','SHA','AUTHOR_NAME','AUTHOR_EMAIL','AUTHOR_DATE','COMMITTER_NAME','COMITTER_EMAIL','COMMIT_DATE','MESSAGE','COMMENTS','VERIFICATION','URL','PARENTS']
    repo_df,commit_df,df_freq = dfproc(dataframe,drop_col,ass_col )
    repo_df.to_csv(IBM_rCSV)
    commit_df.to_csv(IBM_cCSV)
    df_freq.to_csv(IBM_fCSV)

#    plt_histtime(repo_df['parsedate'],IBM_Proj, 'Month', 'No of Repos', 'REPO CREATED PER MONTH')
#    plt_histtime(commit_df['parsedate'],IBM_Com, 'Month', 'No of Commits', 'COMMITS PER MONTH' )    
#    plt_linetime(df_freq['pdate'],df_freq['commit_repo'],IBM_Com_Proj, 'Month', 'Commits per Repo', 'COMMITS PER REPO CREATED')
    plt_linetime(df_freq['pdate'],df_freq['commit_repocum'],IBMCom_Projcum, 'Month', 'Commits per Repo', 'COMMITS PER REPO (CUMMULATIVE)')    
    plt.legend(['MS', 'Google', 'IBM'])
    
    
    plt.show()
if __name__ == '__main__':
  main()