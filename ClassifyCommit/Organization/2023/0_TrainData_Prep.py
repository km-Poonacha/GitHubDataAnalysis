# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 11:44:25 2023

@author: pmedappa
"""

import pandas as pd
import numpy as np
def find_files():
    """fond the number of files from commit information"""   
    r_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\LabelData\Training Data 2023\Java Commit Creativity - Full 2023 Clean.xlsx'
    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\LabelData\Training Data 2023\Java Commit Creativity - Full 2023 Clean 2.xlsx'
    train_df = pd.read_excel(r_user_xl,header= 0)
    train_df['files'] = train_df['commit_code'].str.count("filename")
    train_df.to_excel(w_user_xl, index = False) 


def main():
    """Main function"""
    r_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Train_Clean.xlsx'
    w_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Train_Clean no Dup.xlsx'
    d_user_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Dups.xlsx'
    train_df = pd.read_excel(r_user_xl,header= 0)
    dups = train_df[train_df.Description.duplicated()]
    train_df = train_df.drop_duplicates(subset=['Description'])    
    
    train_df.to_excel(w_user_xl, index = False) 
    dups.to_excel(d_user_xl, index = False) 


main()
