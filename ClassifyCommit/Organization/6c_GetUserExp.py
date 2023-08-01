# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 15:11:32 2020

@author: pmedappa
Find the experience of the contributor. (Requested by AE)

Try to integrate this into #6

****************** NOTE THE RELEASE DATES ARE NO TWORKING currectly here 
"""


import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib')
    print(sys.path)

import pandas as pd
import numpy as np


LOG_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\facebook\Classified\UserSpon_log.csv'

MAX_ROWS_PERWRITE = 20000

DF_REPO = pd.DataFrame()
DF_COUNT = 0
      
# R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\facebook\Classified\int2_org_col_classified_facebook_commit_1.xlsx'
#              ]   

#google
# R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\int2_org_col_classified_google_commit_1.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\int2_org_col_classified_google_commit_2.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\int2_org_col_classified_google_commit_3.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\int2_org_col_classified_google_commit_4.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\int2_org_col_classified_google_commit_EMPTY.xlsx'
#              ]   

R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_1.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_2.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_3.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_4.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_5.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_6_2.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_7.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_8.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_EMPTY.xlsx'
              ]   

#IBM
# R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\apple\Classified\int2_org_col_classified_apple_commit.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\apple\Classified\int2_org_col_classified_apple_commit_EMPTY.xlsx',
#              ]    

def appendrowindf(user_xl, row, df_flag = 0):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    
    
    # note there is an issue when shape is used for series and df. 
    if df_flag == 0:
        DF_REPO= DF_REPO.append(row, ignore_index = True)
        DF_COUNT = DF_COUNT + 1 # use row.shape[0] for dataframe
    else:
        # row = row.reset_index(drop=True)
        DF_REPO= DF_REPO.append(row, ignore_index = True)
        DF_COUNT = DF_COUNT + row.shape[0]
        
    if DF_COUNT >= MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,header= 0)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()
 
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    r_file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\Experience\exp_int2_org_col_classified_microsoft_commit_full.xlsx'
    cooccurance_file = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\Experience\cooccurance.xlsx'

    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\Experience\coexp_exp_int2_org_col_classified_microsoft_commit_full.xlsx'
    user_df = pd.DataFrame()
    df_test = pd.DataFrame()
    df_test.to_excel(w_user_xl, index = False) 

    user_df = pd.read_excel(r_file,header= 0)
    # ffill repo_id
    user_df['repo_name'] = user_df['repo_name'].fillna(method='ffill')

    user_df_temp = user_df.dropna(subset=['contributor_login'])
    user_agg = user_df_temp.groupby(['repo_name'], as_index = False).agg({'contributor_login': list})

    
    u = (pd.get_dummies(pd.DataFrame(user_agg['contributor_login'].tolist()), prefix='', prefix_sep='')
           .groupby(level=0, axis=1)
           .sum())
    
    v = u.T.dot(u)
    v.values[(np.r_[:len(v)], ) * 2] = 0
    user_agg.set_index('repo_name', inplace=True)

    # v.to_excel(cooccurance_file) 
    temp_df = pd.DataFrame()

    user_df['cowork_exp'] = ''
    for i, row in user_df.iterrows():
        if  pd.notnull(row[0]):
            print("Repo: ",row['repo_name'])
            
            if  len(temp_df) != 0:

                
                # contri = [x for x in contri if x != 0]

                for i, c_row in temp_df.iterrows():
                    login = c_row['contributor_login']

                    contri = user_agg.loc[c_row['repo_name']]['contributor_login']
 
                    try: 
                        # print(login , " = ", v.loc[[login]][contri].sum(axis = 1).values[0] - len(contri) + 1)
                        c_row['cowork_exp'] = v.loc[[login]][contri].sum(axis = 1).values[0] - len(contri) + 1
                        appendrowindf(w_user_xl, c_row, df_flag = 0)
                    except:
                        row['cowork_exp'] = 0
                        appendrowindf(w_user_xl, c_row, df_flag = 0)
                
                # m_temp_df = getcolab(temp_df)
                # m_temp_df= getnetcolab(m_temp_df)
                # appendrowindf(w_user_xl, m_temp_df, df_flag = 1)
            

            appendrowindf(w_user_xl, row, df_flag = 0)
            temp_df = pd.DataFrame()
            # prev_repo = row['repo_name']
        else:           
            temp_df = temp_df.append(row, ignore_index = True)
    
    if  len(temp_df) != 0:
        for i, c_row in temp_df.iterrows():
            login = c_row['contributor_login']

            contri = user_agg.loc[c_row['repo_name']]['contributor_login']
 
            try: 
                # print(login , " = ", v.loc[[login]][contri].sum(axis = 1).values[0] - len(contri) + 1)
                c_row['cowork_exp'] = v.loc[[login]][contri].sum(axis = 1).values[0] - len(contri) + 1
                appendrowindf(w_user_xl, c_row, df_flag = 0)
            except:
                row['cowork_exp'] = 0
                appendrowindf(w_user_xl, c_row, df_flag = 0)
    df = pd.read_excel(w_user_xl,header= 0)
    df= df.append(DF_REPO, ignore_index = True)
    df.to_excel(w_user_xl, index = False) 
    DF_COUNT = 0
    DF_REPO = pd.DataFrame()        
 
main()