# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 09:59:48 2021

@author: pmedappa

Merge 28072020_FinalCleanData_Full_Clean2 with Final_COL_MC_RepoCommit_UserInfo_Ext
"""
import pandas as pd

LEFT_XLSX = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\ClassifiedRepoCommit\UPnew28072020_FinalCleanData_Full.xlsx"
RIGHT_XLSX = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Contributors_monthwise\Final_COL_MC_RepoCommit_UserInfo_Ext_2.xlsx"
MERGE_XLSX = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\ClassifiedRepoCommit\08032021_MergeExt_FinalCleanData_Full_Clean2.xlsx"


df_left = pd.read_excel(LEFT_XLSX ,header= 0)
df_right = pd.read_excel(RIGHT_XLSX ,header= 0)

df_right = df_right.rename(columns={0: 'REPO_ID'})
df_right['REPO_ID'] = df_right['REPO_ID'].fillna(method='ffill')
df_right = df_right[['REPO_ID','month','net_int_all',	'net_int_acc',	'net_intall_colab',	'net_intall_cont',	'net_intacc_colab',	'net_intacc_cont',	'net_colab_chk']]
print(df_right)

print(df_left['REPO_ID'])
result = pd.merge(df_left, df_right,how="inner", on=["REPO_ID", "month"])
result.to_excel(MERGE_XLSX , index = False) 