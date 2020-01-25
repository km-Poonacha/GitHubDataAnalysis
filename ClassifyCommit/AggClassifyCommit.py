# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

Aggregate commit creativity at the level of months. Remove projects for which no commits were found. 
"""

import pandas as pd
import numpy as np
import ast


COMMIT2_XLSX ="C:/Data/092019 CommitInfo/uptestRepoCommit1_287_1.xlsx"
CLEAN_XLSX = "C:/Data/092019 CommitInfo/CleanRepoCommit1_287_1.xlsx"
TEST_XLSX = "C:/Data/092019 CommitInfo/Test.xlsx"
TEST2_XLSX = "C:/Data/092019 CommitInfo/Test2.xlsx"

def consolidate_prob(x, a1, a2,a3):
    "Aggregae the probabilities calculated into a single construct"
    text_file = ['txt','md']
    if pd.isna(x['PINDEX']) and pd.notna(x['OPEN_ISSUES']):
        files = ast.literal_eval(x['OPEN_ISSUES'])
        for i in files:
            l = i.split('.')
            if len(l) > 1:
                if l[1].lower() not in text_file:
                    return (x[a1]+x[a2]*2+x[a3]*3)/3

    return np.NaN

def consolidate_commits(df):
    """ COnsolidate commits monthly"""
    df.dropna(subset=['con_novelty'],inplace = True)
    df2 = df.groupby('c_month')['con_novelty', 'con_usefulness'].sum()
    df2= pd.concat([df2, df.groupby('c_month')['con_novelty'].count(),df.groupby(['c_month'])['SIZE'].nunique()], axis = 1)
    df2.columns = ['m_novelty','m_usefulness','c_count','n_contributors']
    df2= df2.reset_index()
    return df2
    
def main():
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    df_commit= pd.read_excel(COMMIT2_XLSX, sep=",",error_bad_lines=False,header=0,  encoding = "Latin1")
    df_commit.drop(df_commit[(df_commit.PINDEX.notna()) & (df_commit.PINDEX.shift(-1).notna())].index, inplace= True)
    #clean_df(df_commit)
    df_commit.drop(columns=["Unnamed: 0"], inplace = True)
    
    #get month information for each commit
    df_commit = df_commit.assign( c_month =   pd.DatetimeIndex(pd.to_datetime(df_commit['OWNER'], infer_datetime_format=True,errors='coerce')).month)
    df_commit = df_commit.assign( c_day =   pd.DatetimeIndex(pd.to_datetime(df_commit['OWNER'], infer_datetime_format=True,errors='coerce')).day)
    #consolidate novelty and usefulness
    df_commit['con_novelty'] = df_commit.apply(consolidate_prob, args =('Noveltys2p1','Noveltys2p2','Noveltys2p3'), axis = 1)
    df_commit['con_usefulness'] = df_commit.apply(consolidate_prob, args =('Usefulnesss2p1','Usefulnesss2p2','Usefulnesss2p3'), axis = 1)
    
    #aggregate to monthly
    
    #get commits of a repo
    repo_commits = pd.DataFrame()
    repo_details = pd.Series()
    write_commits = pd.DataFrame()
    
    for i,row in df_commit.iterrows():
        if pd.notna(row['PINDEX']):
            write_commits = write_commits.append(row, sort = False, ignore_index = True)
            write_commits = write_commits.reindex(df_commit.columns, axis=1)
            if i > 1: 
                
                repo_commits.to_excel(TEST_XLSX)
                df_mcommit = consolidate_commits(repo_commits)
                write_commits = write_commits.append(df_mcommit, sort = False, ignore_index = True)                
                write_commits.to_excel(TEST2_XLSX)
                return
            repo_commits = pd.DataFrame()
            continue

        repo_commits = repo_commits.append(row, sort = False)
    
    
    print(df_commit)
    df_commit.to_excel(CLEAN_XLSX)
    # Save the full labeled data sample post processing in CSV
   
if __name__ == '__main__':
  main()
  