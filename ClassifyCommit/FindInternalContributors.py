# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 13:35:31 2021

@author: pmedappa

Find if contributors is internal or external 

rule 1: If org matches the org listed
rule 2: First day committers
rule 3: Org affiliation of first day committers ??
rule 4: Sounds like
"""
import pandas as pd
import numpy as np
import json
import ast 

MAX_ROWS_PERWRITE = 1000

DF_REPO = pd.DataFrame()
DF_COUNT = 0
ORG_NAME = list()

LOG_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Contributors_monthwise\UserSpon_log.csv'

def appendrowindf(user_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(row, ignore_index = True)
    DF_COUNT = DF_COUNT + row.shape[0]
    if DF_COUNT >= MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,header= 0)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()
    

def get_orgname(row):
    """Get the org name associated with an internal user """
    org = list()
    if pd.notnull(row[7]):
        company = row[7].replace("@", "").lower()
        org.append(company)
    if pd.notnull(row[10]):
        for o in ast.literal_eval(row[10]):
            org.append(o['login'])    
    return org


def rules(row,w_user_xl,date,o_name):
    """ rule 1: If org matches the org owner listed """
    """ rule 2: First day committers """
    """ rule 3: Org affiliation of first day committers """
    global ORG_NAME
    org_name = list()
    org_name = get_orgname(row)
    if  pd.notnull(row[8]): # rule 1
        row[14] = 1
        ORG_NAME.append(row[8].lower())        
        # add company
    elif row[5][:10] == date[:10]: #rule 2
        row[14] = 2        
    else:
        org_a = set(org_name)
        org_b = set(ORG_NAME)
        org_n = set(o_name)
        if (org_a & org_n): #rule 1 
            row[14] = 1
        elif (org_a & org_b): #rule 3
            row[14] = 3
        else:
            row[14] = 0

    
    row[15] = org_name
    for o in org_name:
        ORG_NAME.append(o)
        
    appendrowindf(w_user_xl, row)     

 

def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    global ORG_NAME
    r_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Contributors_monthwise\COL_MC_RepoCommit_UserInfo_10000.xlsx'
    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Contributors_monthwise\COL_MC_RepoCommit_UserInfo_10000_Ext2.xlsx'
    user_df = pd.read_excel(r_user_xl,header= 0)
    df_test = pd.DataFrame()
    df_test.to_excel(w_user_xl, index = False) 
    u_flag = 0
    for i,row in user_df.iterrows():
        if  ~np.isnan(row[0]):
            print("Repo: ",row[0]," Owner: ",row[2]," Type: ",row[3])
            date = row[5]
            o_name= str(row[2])
            ORG_NAME = list()
            if row[3] == "User":
                u_flag = 1
            else: 
                u_flag = 0                
                appendrowindf(w_user_xl, row)            
        else:
            if u_flag == 1:
                continue
            rules(row,w_user_xl,date,o_name.lower())
            
    df = pd.read_excel(w_user_xl,header= 0)
    df= df.append(DF_REPO, ignore_index = True)
    df.to_excel(w_user_xl, index = False) 
    DF_COUNT = 0
    DF_REPO = pd.DataFrame()
  
main()