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
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    dataframe = pd.read_csv(csv, sep=",",error_bad_lines=False,header= 0, low_memory=False, encoding = "Latin1")
    return dataframe

def plt_histtime(ser):
    plt.xticks(np.arange(30, 90, step=1))
    n, bins, patches = plt.hist(ser.dropna(), bins=np.arange(30, 90, 1),range = (30, 100), histtype='bar', align='mid', orientation='vertical', rwidth=None, label='Repos Created')
    patches[48].set_fc('r')
    patches[52].set_fc('r')
    plt.show()

def plt_linetime(x_ser, y_ser):
    plt.xticks(np.arange(30, 90, step=1))
    lines = plt.plot(x_ser,y_ser) 
    plt.vlines(78,0,lines[0].get_data()[1][np.where(lines[0].get_data()[0] == 78)])
    plt.vlines(82,0,lines[0].get_data()[1][np.where(lines[0].get_data()[0] == 82)])
    plt.show()

def main():
     
    # For WINDOWS 

    REPO_CSV = 'D:\\Xitong proj\Xitong proj IBM\IBMRepoCommit_work.csv'
    NEWREPO_CSV = 'D:\\Xitong proj\Xitong proj IBM\\IBMRepoCommit_parse.csv'
    dataframe = dfInit(REPO_CSV)
    repo_df = dataframe[dataframe['REPO_ID'].notnull()]
    commit_df = dataframe[dataframe['REPO_ID'].isnull()]    
    repo_df = repo_df.assign(parsedate =  (pd.to_datetime(repo_df['CREATED_AT']).dt.year - 2012)*12 + pd.to_datetime(repo_df['CREATED_AT']).dt.month)
    repo_df = repo_df.assign(year =  (pd.to_datetime(repo_df['CREATED_AT']).dt.year ))

    commit_df = commit_df.drop(axis=1,columns=['HAS_ISSUES','HAS_PROJECTS',	'HAS_DOWNLOADS',	'HAS_WIKI','HAS_PAGES','FORKS',	'MIRROR_URL',	'ARCHIVED','DISABLED','OPEN_ISSUES',	'LICENSE',	'FORKS.1',	'OPEN_ISSUES.1',	'WATCHERS.1','DEFAULT_BRANCH',	'PERMISSIONS'])
    commit_df.columns = ['REPO_ID','SHA','AUTHOR_NAME','AUTHOR_EMAIL','AUTHOR_DATE','COMMITTER_NAME','COMITTER_EMAIL','COMMIT_DATE','MESSAGE','COMMENTS','VERIFICATION','URL','PARENTS']
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
#    print(df_rfreq)
#    print(df_cfreq)
#    print(df_freq)
#    df_freq.to_csv(NEWREPO_CSV)
    plt_linetime(df_freq['pdate'],df_freq['commit_repo'])

    # Plot hist times
#    repo_df.hist(column = 'parsedate', bins = 70, by = repo_df['year'] )
#    commit_df.hist(column = 'parsedate', bins = 100, by = commit_df['year']  )
#    plt_histtime(repo_df['parsedate'])
#    plt_histtime(commit_df['parsedate'])    

    
#    print(repo_freq)
#    print(commit_freq)
if __name__ == '__main__':
  main()