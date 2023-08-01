# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 15:11:32 2020

@author: pmedappa
Find the experience of the contributor. (Requested by AE)

Try to integrate this into #6
"""


import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib')
    print(sys.path)

import pandas as pd



LOG_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\facebook\Classified\UserSpon_log.csv'


      
# R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\facebook\Classified\int2_org_col_classified_facebook_commit_1.xlsx'
#              ]   

#microsoft
R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_1.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_1_2.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_2.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_3.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_4.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_5.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_6_2.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_7.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_8.xlsx',
              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\int2_org_col_classified_microsoft_commit_EMPTY.xlsx'
              ]   

#Apple
# R_USER_XL = [r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\apple\Classified\int2_org_col_classified_apple_commit.xlsx',
#              r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\apple\Classified\int2_org_col_classified_apple_commit_EMPTY.xlsx',

#              ]   
 
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    
    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\microsoft\Classified\Experience\exp_int2_org_col_classified_microsoft_commit_full.xlsx'
    user_df = pd.DataFrame()
    df_test = pd.DataFrame()
    df_test.to_excel(w_user_xl, index = False) 
    
    for r_file in R_USER_XL:
        temp_df = pd.read_excel(r_file,header= 0)
        user_df = user_df.append(temp_df, ignore_index=True)
    contri_df = user_df.dropna(subset=['contributor_login'])
    
    df2 = contri_df.groupby(['contributor_login']).size().to_frame('exp_counts')
    df2 = df2.reset_index()
    contri_df2 = contri_df[contri_df.contributor_type == 'Contributor']
    df2 = pd.merge(df2, contri_df2.groupby(['contributor_login']).size().to_frame('exp_counts_cont'), how = 'left', on=['contributor_login'] )
    df2['exp_counts_cont'] = df2['exp_counts_cont'].fillna(0)            
    contri_df3 = contri_df[contri_df.contributor_type == 'Collaborator']
    df2 = pd.merge(df2, contri_df3.groupby(['contributor_login']).size().to_frame('exp_counts_collab'), how = 'left', on=['contributor_login'])
    df2['exp_counts_collab'] = df2['exp_counts_collab'].fillna(0) 
    n_user_df = pd.merge(user_df, df2, how="left", on=['contributor_login'])

#
    df = pd.read_excel(w_user_xl,header= 0)
    df= df.append( n_user_df , ignore_index = True)
    df.to_excel(w_user_xl, index = False) 
    DF_COUNT = 0
    DF_REPO = pd.DataFrame()
  
main()