# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

Merge collab info with the commit info. 
"""

import pandas as pd
import numpy as np
import ast



COL_MC_XLSX = "C:/Data/092019 CommitInfo/COL_MC_RepoCommit1_287_1.xlsx"
NEW_XLSX = "C:/Data/092019 CommitInfo/Final_COL_MC_RepoCommit1_287_1.xlsx"


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
    # df_commit= pd.read_excel(MC_XLSX, sep=",",error_bad_lines=False,header=0,  encoding = "Latin1")
    df_contri = pd.read_excel(COL_MC_XLSX, sep=",",error_bad_lines=False,header=0,  encoding = "Latin1")
    
    repo_df = df_contri[df_contri[0].notna()]
    df_contri[0] = df_contri[0].fillna(method='ffill')
    write_xl = pd.DataFrame()
    
    for i, repo in repo_df.iterrows():
            
        write_xl = write_xl.append(repo, sort = False, ignore_index = True)
        contri = df_contri[df_contri[0]==repo[0]]
        contri= contri[contri[7].isna()]
        contri= contri.drop(list(range(7,105)),axis = 1)
        contri.columns = ['REPO_ID', 'C_TYPE','C_NAME','CONTRIBUTIONS','PULLS','S_DATE','E_DATE']
    #get month information for each commit
        contri= contri.assign( s_month =   pd.DatetimeIndex(pd.to_datetime(contri['S_DATE'], infer_datetime_format=True,errors='coerce')).month)
        # contri = contri.assign( s_day =   pd.DatetimeIndex(pd.to_datetime(contri['S_DATE'], infer_datetime_format=True,errors='coerce')).day)
        contri= contri.assign( e_month =   pd.DatetimeIndex(pd.to_datetime(contri['E_DATE'], infer_datetime_format=True,errors='coerce')).month)
        # contri = contri.assign( e_day =   pd.DatetimeIndex(pd.to_datetime(contri['E_DATE'], infer_datetime_format=True,errors='coerce')).day)
    
        contri['Collaborator']= contri['C_TYPE'].apply( lambda x: 1 if x == 'Collaborator' else 0)
        contri['Contributor']= contri['C_TYPE'].apply( lambda x: 1 if x == 'Contributor' else 0)
        df2 = pd.DataFrame({'month' : [1,2,3,4,5,6,7,8,9,10,11,12]})
        df2 = df2.set_index('month')
        df2 = pd.concat([df2,contri.groupby('s_month')['Collaborator'].sum().to_frame('start_colab')], axis=1)
        df2 = pd.concat([df2, contri.groupby('e_month')['Collaborator'].sum().to_frame('end_colab')], axis=1)
        df2 = df2.fillna(0)
        df2 = df2.reset_index()
        df2.rename(columns={"index": "month"}, inplace = True)
        print(df2)
        
        write_xl = write_xl.append(df2, sort = False, ignore_index = True)

    write_xl.to_excel(NEW_XLSX)
    #aggregate to monthly
    
    #get commits of a repo
    # repo_commits = pd.DataFrame()
    # write_commits = pd.DataFrame()
    # indx = df_commit.columns
    # indx = indx.append(pd.Index(['ncon_month','start_date','n_collaborators','n_contributors']))

    # for i,row in df_contri.iterrows():
    #     if pd.notna(row['PINDEX']):
    #         #get the monthly aggregate commit info
    #         df_mcommit = consolidate_commits(repo_commits)
    #         write_commits = write_commits.append(df_mcommit, sort = False, ignore_index = True)                           
    #         #append repo row info
    #         write_commits = write_commits.append(row, sort = False, ignore_index = True)
    #         repo_commits = pd.DataFrame()
    #         continue

        

    #     repo_commits = repo_commits.append(row, sort = False)
    
    # write_commits = write_commits.reindex(indx, axis=1)
    # write_commits = write_commits.drop(axis=1,columns=['REPO_ID.1', 'yhat','opt_deg_sup_ind','opt_deg_sup_org','Unnamed: 104','UNKNOWN','c_month','c_day','con_novelty','con_usefulness'])

    # # Save the full labeled data sample post processing in CSV
   
if __name__ == '__main__':
  main()
  