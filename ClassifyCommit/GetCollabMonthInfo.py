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


EVENT_CSV ="C:/Users/pmedappa/Dropbox/HEC/Project 5 - Roles and Coordination/Data/MergeEvents/NewEvent2014_15_test.csv"

TEST_CSV = "C:/Users/pmedappa/Dropbox/HEC/Project 5 - Roles and Coordination/Data/ContributorInfo/12_CollabExportedISRDataCollab_22012018_2_Complete.csv"
MC_XLSX = "C:/Data/092019 CommitInfo/MC_RepoCommit1_287_1.xlsx"
COL_MC_XLSX = "C:/Data/092019 CommitInfo/COL_MC_RepoCommit1_287_1.xlsx"

def find_events(df_row):
    return df_row

def main():
    """Get data from the training sample CSV and perform various cleaning and data preprocessing"""
    year_list = ['2014','2015']
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    repo_id = 15814446

    e_df = pd.read_csv(EVENT_CSV, sep=",",error_bad_lines=False,header= None, low_memory=False, encoding = "Latin1")
    e_df[0] = e_df[0].fillna(method='ffill')
    events = e_df[e_df[0]==repo_id]
    events= events.drop(events[events[1] != '2014'].index)
    contributors = events[2].unique()
    write_events = events.drop(events[events[3] == 'PullRequestEvent'].index)
    collaborators = write_events[2].unique()
    print(contributors)
    print(collaborators)
    
    # find first occurance of collborators
    for c in collaborators:
        c_events = write_events.drop(write_events[write_events[2] != c].index)
        fl_events = c_events[4].iloc[[0, -1]]
        print(fl_events)
        
        
    
    # events[6] = pd.to_datetime(events[4])
    # repo_event = pd.DataFrame()

    # repo_event = e_df.apply(find_events, axis = 1)
    events.to_excel(COL_MC_XLSX, sheet_name='Sheet1', index=False)
    
if __name__ == '__main__':
  main()
  