# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

Get all commits corresponding to java projects 
"""

import pandas as pd
import numpy as np
import ast


REPOCOMMIT_LIST =[
                   "C:/Data/092019 CommitInfo/RepoCommit1_287_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit288_500_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit501_1000_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit1001_1500_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit1501_2000_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit2002_2500_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit2501_3250_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit3251_4000_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit4001_5000_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit5001_5202_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit5203_6000_1.xlsx",
                   "C:/Data/092019 CommitInfo/RepoCommit6001_6570_1.xlsx"
                  ]

MC_XLSX = "C:/Data/092019 CommitInfo/Java_RepoCommit1_287_1.xlsx"



def consolidate_commits(df):
    """ Consolidate commits monthly"""
    if int(df.shape[0]) <1: return pd.DataFrame()
    df.dropna(subset=['con_novelty'],inplace = True)
    df2 = df.groupby('c_month')['con_novelty', 'con_usefulness'].sum()
    df2= pd.concat([df2, df.groupby('c_month')['con_novelty'].count(),df.groupby(['c_month'])['SIZE'].nunique()], axis = 1)
    
    # Change index from c_months 
    df2= df2.reset_index()
    df2.columns = ['nc_month','m_novelty','m_usefulness','c_count','c_contributors']
    return df2
    
def main():
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    write_commits = pd.DataFrame()
    for xl_sheet in REPOCOMMIT_LIST:
        df_commit= pd.read_excel(xl_sheet, sep=",",error_bad_lines=False,header=0,  encoding = "Latin1")
        #clean_df(df_commit)
        df_commit.drop(df_commit[(df_commit.PINDEX.notna()) & (df_commit.PINDEX.shift(-1).notna())].index, inplace= True)
        
            
        #get commits of a repo
        writer = pd.ExcelWriter(MC_XLSX , engine='xlsxwriter')
        
        # print(df_commit.loc[df_commit[9].str.lower() == 'java'])
        i_list = df_commit.index[df_commit['MAIN_LANGUAGE'].str.lower() == 'java'].tolist()
        
        for start in i_list:
            repo_commits = pd.DataFrame()
            for i, row in df_commit.loc[start:].iterrows():
    
                if pd.notna(row[0]) and int(repo_commits.shape[0]) >1:
                    break
                repo_commits = repo_commits.append(row, sort = False, ignore_index = True)
            write_commits = write_commits.append(repo_commits, sort = False, ignore_index = True)
        write_commits = write_commits.reindex(df_commit.columns, axis=1)
        write_commits.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()
        
    
    
    # Save the full labeled data sample post processing in CSV
  
if __name__ == '__main__':
  main()
  