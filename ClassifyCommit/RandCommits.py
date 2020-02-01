# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 14:47:45 2018

@author: kmpoo
"""

import pandas as pd

import numpy as np

from datetime import datetime

COMMIT_CSV = 'C:/Data/14_ContriCollab_2_off2.csv'
COMMIT_SAMPLE_CSV = 'D:/Contributorinfo/14_Contricollab_Sample.csv'

pd.options.display.max_rows = 10
pd.options.display.float_format = '{:.1f}'.format

commit_dataframe = pd.read_excel(COMMIT_CSV, sep=",",error_bad_lines=False,header = None,low_memory=False)

nrepo =  commit_dataframe.iloc[:,0].count()
ncontri =  commit_dataframe.iloc[:,1].count() - nrepo
ncommit = commit_dataframe.shape[0] - (ncontri + nrepo)
print(" No of repos = ", nrepo ,"\n No of contri = ", ncontri, "\n No of commit = ", ncommit )

commit_info_df = pd.DataFrame(commit_dataframe.loc[commit_dataframe[0].isnull() & commit_dataframe[1].isnull() ].iloc[:,2:14].copy()) 
commit_info_df = commit_info_df.set_axis(['Sha', 'Date', 'Dsc', 'nComments', 'Committer', 'CommitterID', 'CommitDate','URL','Parents','Verified','Changes','Files'], axis='columns', inplace=False)

commit_info_df = commit_info_df.assign(year = lambda x: x['Date'].str.slice(start=0, stop=4))

print("Total commits extracted = ", commit_info_df.shape[0] )
commit_info_df = commit_info_df.drop(commit_info_df[commit_info_df.year.isnull()].index)
commit_info_df = commit_info_df.drop(commit_info_df[commit_info_df.year.astype(int) > 2015].index)
print("Total commits before 2016 = ", commit_info_df.shape[0] )
commit_info_df.sample(100).to_csv(COMMIT_SAMPLE_CSV)