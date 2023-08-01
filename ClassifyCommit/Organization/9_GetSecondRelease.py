# -*- coding: utf-8 -*-
"""
Created on Thu Jul 22 15:10:25 2021

@author: pmedappa

Code to gt second release yearmonth for the falsification test
"""
import pandas as pd

import ast 

merged_file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM3_Center2.xlsx'

updated_file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM3_Center_SR.xlsx'

pd.options.display.max_rows = 10
pd.options.display.float_format = '{:.3f}'.format

merg_df = pd.read_excel(merged_file ,header= 0)

write_df = pd.DataFrame()
'releases_nodes'


for i,row in merg_df.iterrows():    
   
    if  pd.notnull(row['releases_nodes']):
        releases = row['releases_nodes'].split("createdAt")

        if len(releases) > 2:
            second_release = releases[2].split("\'")[2]
        else:
            second_release = "2050-01-01T00:00:00Z"
    else:
        second_release = "2050-01-01T00:00:00Z"
    row = row.append(pd.Series(second_release, index=['second_release']))

    write_df= write_df.append(row,ignore_index=True)

write_df['second_release_yearmonth'] = (pd.to_numeric(write_df['second_release'].str.split('-').str[0]) - 2008) * 12 + pd.to_numeric(write_df['second_release'].str.split('-').str[1])

write_df.to_excel(updated_file, index = False)
# write_df['second_release'] = write_df.apply(lambda x :"2050-01-01T00:00:00Z" if pd.isna(x['releases_nodes_0_publishedAt']) and pd.notna(x['org_login']) else x['releases_nodes_0_publishedAt'],axis = 1)
# write_df['second_release'] = (pd.to_numeric(write_df['first_release'].str.split('-').str[0]) - 2008) * 12 + pd.to_numeric(write_df['first_release'].str.split('-').str[1])
