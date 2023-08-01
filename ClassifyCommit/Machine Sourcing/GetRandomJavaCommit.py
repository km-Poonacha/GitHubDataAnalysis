# -*- coding: utf-8 -*-
"""
Created on Fri Nov  4 17:45:30 2022

@author: pmedappa
"""
import pandas as pd
from ast import literal_eval


W_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\ibm\ibm_commit_rs9.xlsx"

f_df = pd.DataFrame()
for i in [1,2,3,4,"EMPTY"]:
    R_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\ibm\ibm_commit_"+str(i)+".xlsx"
    r_df = pd.read_excel(R_XL,header= 0)
    
    r_df = r_df[['repo_id','repo_owner_login','repo_name','repo_languages_nodes','commit_oid']]
    r_df['repo_languages_nodes'] = r_df['repo_languages_nodes'].ffill()
    r_df['repo_name'] = r_df['repo_name'].ffill()
    r_df['repo_owner_login'] = r_df['repo_owner_login'].ffill()
    r_df['language'] = r_df['repo_languages_nodes'].apply(literal_eval).str[0]
    r_df['language'] = r_df['language'].str['name']
    r_df =r_df[r_df['language'] == 'Java']
    r_df= r_df.assign(url = lambda x: "http://github.com/"+x['repo_owner_login']+"/"+x['repo_name']+"/commit/"+x['commit_oid'])
    print(r_df.shape)
    r_df = r_df.dropna(subset=['commit_oid'])
    f_df = f_df.append(r_df,ignore_index = True, sort=False)

f_df = f_df.sample(int(r_df.shape[0]/10))
f_df = f_df.drop_duplicates(subset=['repo_name'])
f_df.to_excel(W_XL, index = False) 