# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 15:11:32 2020

@author: pmedappa

New code to find the collaborators for each project and date of their first contribution.
Using the merge events sheet C:/Users\pmedappa\Dropbox\HEC\Project 5 - Roles and Coordination\Data\MergeEvents 
"""


import pandas as pd
import numpy as np
import ast


EVENT_CSV ="C:/Users/pmedappa/Dropbox/HEC/Project 5 - Roles and Coordination/Data/MergeEvents/NewEvent2014_15_"

TEST_CSV = "C:/Users/pmedappa/Dropbox/HEC/Project 5 - Roles and Coordination/Data/ContributorInfo/12_CollabExportedISRDataCollab_22012018_2_Complete.csv"
MC_XLSX = "C:/Data/092019 CommitInfo/MC_RepoCommit1_287_1.xlsx"
COL_MC_XLSX = "C:/Data/092019 CommitInfo/COL_MC_RepoCommit.xlsx"

def find_events(df_row):
    return df_row

def main():
    """Get data from the training sample CSV and perform various cleaning and data preprocessing"""

    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    writer = pd.ExcelWriter(COL_MC_XLSX , engine='xlsxwriter')
    for iterate in range(1,8):
        print("******** File ", iterate)

        e_df = pd.read_csv(EVENT_CSV+str(iterate)+".csv", sep=",",error_bad_lines=False,header= None, low_memory=False, encoding = "Latin1")
        repo_df = e_df[e_df[0].notna()]
        e_df[0] = e_df[0].fillna(method='ffill')
        write_xl = pd.DataFrame()
        
        for i, repo in repo_df.iterrows():
            
            write_xl = write_xl.append(repo, sort = False, ignore_index = True)
            events = e_df[e_df[0]==repo[0]]
            events= events.drop(events[(events[1] != '2014') & (events[1] != '2015')].index)
            try:
                events[4] = pd.to_datetime(events[4], format='%Y-%m-%d %H:%M:%S')
            except: pass

                
            # events[5] = pd.to_datetime(events[5], format='%Y-%m-%d %H:%M:%S')
            contributors = events[2].unique()
    
            write_events = events.drop(events[events[3] == 'PullRequestEvent'].index)
    
            collaborators = write_events[2].unique()
            print("Repo : ",repo[0]," contributors ",len(contributors)," collaborators ",len(collaborators))
            
            # find first occurance of collborators
            for c in collaborators:
                c_events = write_events.drop(write_events[write_events[2] != c].index)
                fl_events = c_events[4].iloc[[0, -1]]
                write_xl = write_xl.append(pd.Series(["","Collaborator", c ,events.drop(events[events[2] != c].index).shape[0],c_events.shape[0], c_events[4].iloc[[0]].values[0], c_events[4].iloc[[-1]].values[0] ]), sort = False, ignore_index = True)
                contributors = contributors[contributors != c]
    
            for co in contributors:
                c_events = events.drop(events[events[2] != co].index)
                fl_events = c_events[4].iloc[[0, -1]]
                write_xl = write_xl.append(pd.Series(["","Contributor",  co , c_events.shape[0],0, c_events[4].iloc[[0]].values[0], c_events[4].iloc[[-1]].values[0] ]), sort = False, ignore_index = True)
            
        write_xl.to_excel(writer , sheet_name='Sheet1', index=False)
        writer.save()
if __name__ == '__main__':
  main()
  