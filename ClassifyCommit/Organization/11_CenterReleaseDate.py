# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 13:23:46 2022

@author: pmedappa
"""


import pandas as pd

MAX_ROWS_PERWRITE = 20000

DF_REPO = pd.DataFrame()
DF_COUNT = 0
ORG_NAME = list()

file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM4_Center_SR.xlsx'
r_file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM4_Center_SR_2.xlsx'

r_df = pd.read_excel(file ,header= 0)


write_df = pd.DataFrame()

repo_names = pd.DataFrame()
repo_ids = r_df['repo_id'].unique()
print(len(repo_ids))

for row in repo_ids:
    print("Repo: ",row)
    temp_df = r_df[r_df.repo_id == row]
    temp_df['month_active'] = len(temp_df)
    release_date =temp_df['first_release'].unique()[0]
    if release_date == 505:
        center_date = round((len(temp_df)/2) - 0.5)
        
    else:
        print(release_date)
        ym_center = temp_df[temp_df['yearmonth2008'] == release_date] 
        if (len(ym_center) == 0 ):
            while (len(ym_center) == 0 ):
                release_date = release_date + 1
                ym_center = temp_df[temp_df['yearmonth2008'] == release_date]   
                
                if release_date == 505:
                    ym_center = temp_df.tail(1)
                    break
        
        center_date = ym_center['continuos_yearmonth'].reset_index(drop=True).item()
    print(center_date)
    temp_df['center_yearmonth_center'] = temp_df['continuos_yearmonth']- (center_date + 1)
    write_df = write_df.append(temp_df, ignore_index = True)

write_df.to_excel(r_file, index = False) 