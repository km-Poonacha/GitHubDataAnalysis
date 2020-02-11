# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

Aggregate commit creativity at the level of months. Remove projects for which no commits were found. 
"""

import pandas as pd
import numpy as np
import ast


COMMIT2_XLSX ="C:/Data/092019 CommitInfo/ClassifiedRepoCommit/ClassifiedRepoCommit288_500_1.xlsx"

MC_XLSX = "C:/Data/092019 CommitInfo/ClassifiedRepoCommit/MC_RepoCommit288_500_1.xlsx"
# NEW_XLSX = "C:/Data/092019 CommitInfo/Contributors_monthwise/Final_COL_MC_RepoCommit.xlsx"

def consolidate_prob(x, a1, a2,a3):
    "Aggregae the probabilities calculated into a single construct"
    text_file = ['txt','md','doc','docx']
    if pd.isna(x['PINDEX']) and pd.notna(x['OPEN_ISSUES']):
        files = ast.literal_eval(x['OPEN_ISSUES'])
        for i in files:
            l = i.split('.')
            if len(l) > 1:
                if l[1].lower() not in text_file:
                    return (x[a1]+x[a2]*2+x[a3]*3)/3

    return np.NaN

def consolidate_commits(df):
    """ Consolidate commits monthly"""
    if int(df.shape[0]) <1: return pd.DataFrame()
    df.dropna(subset=['con_novelty'],inplace = True)
    df2 = df.groupby('c_month')['con_novelty', 'con_usefulness'].mean()
    df2= pd.concat([df2, df.groupby('c_month')['con_novelty'].count(),df.groupby(['c_month'])['SIZE'].nunique()], axis = 1)
    
    # Change index from c_months 
    df2= df2.reset_index()
    df2.columns = ['nc_month','m_novelty','m_usefulness','c_count','c_contributors']
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
    write_commits = pd.DataFrame()
    indx = df_commit.columns
    indx = indx.append(pd.Index(['nc_month','m_novelty','m_usefulness','c_count','c_contributors']))

    for i,row in df_commit.iterrows():
        if pd.notna(row['PINDEX']):
            #get the monthly aggregate commit info
            df_mcommit = consolidate_commits(repo_commits)
            write_commits = write_commits.append(df_mcommit, sort = False, ignore_index = True)                           
            #append repo row info
            write_commits = write_commits.append(row, sort = False, ignore_index = True)
            repo_commits = pd.DataFrame()
            continue

        

        repo_commits = repo_commits.append(row, sort = False)
        
    write_commits = write_commits.append(df_mcommit, sort = False, ignore_index = True)  
    write_commits = write_commits.reindex(indx, axis=1)
    write_commits = write_commits.drop(axis=1,columns=['REPO_ID.1', 'yhat','opt_deg_sup_ind','opt_deg_sup_org','Unnamed: 104','UNKNOWN','c_month','c_day','con_novelty','con_usefulness'])
    write_commits.to_excel(MC_XLSX)
    # Save the full labeled data sample post processing in CSV
   
if __name__ == '__main__':
  main()
  