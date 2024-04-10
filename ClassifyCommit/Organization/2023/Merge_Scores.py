# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 22:01:51 2021

@author: pmedappa
"""


import sys

import pandas as pd
import numpy as np


     
def main():
    left_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GMA_2023_2.xlsx"

    w_commit_xl = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GMA_2023_3.xlsx"
     
                   
    left_df = pd.read_excel(left_xl,header= 0)
    print(left_df.shape)
    right_df= pd.DataFrame()
    
    for i in ["1","EMPTY"]:
        temp_df = pd.read_excel(r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\2023\apple\month_classified_apple_commit_"+i+".xlsx",header= 0)
        
        temp_df = temp_df[["repo_id",'commit_authoredDate_yearmonth','Noveltys2p1','Noveltys2p2','Noveltys2p3','Usefulnesss2p1','Usefulnesss2p2','Usefulnesss2p3']]
        
        right_df = pd.concat([right_df, temp_df], axis = 0, ignore_index = True)
    
    
    right_df = right_df.rename(columns={"repo_id": "repo_id",'commit_authoredDate_yearmonth': 'month','Noveltys2p1':'Noveltys2p1_2023','Noveltys2p2':'Noveltys2p2_2023','Noveltys2p3':'Noveltys2p3_2023'
                                        ,'Usefulnesss2p1' : 'Usefulnesss2p1_2023','Usefulnesss2p2': 'Usefulnesss2p2_2023','Usefulnesss2p3': 'Usefulnesss2p3_2023'})
    
    print(right_df.shape)
    right_df["repo_id"] = right_df["repo_id"].ffill()
    right_df['dups'] = right_df['repo_id'] + right_df['month'].astype(str) 
    
    right_df = right_df.drop_duplicates(['dups'],keep = 'last').reset_index()
    print(right_df.shape)

    
    
    merge_df = left_df.merge(right_df, left_on=['repo_id','month'], right_on=['repo_id','month'], how = 'left')
    
    print(merge_df.shape)


    merge_df.to_excel(w_commit_xl, index = False) 
    
    
    
main()