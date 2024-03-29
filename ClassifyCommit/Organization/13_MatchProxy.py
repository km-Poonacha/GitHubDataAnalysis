# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

This code built on earlier code uses an existing trainset to classify commits (no randomization).
"""

import pandas as pd
import numpy as np
import ast
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.utils import shuffle #To shuffle the dataframe
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
from scipy import sparse
import matplotlib.pyplot as plt
from sklearn.model_selection import learning_curve
import gensim
from scipy.spatial import distance
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize


DATA_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM5_Center_SR_var2.xlsx"
MERGE = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Merged\GIMFA_Panel_Exp_CEM5_Center_SR_var2.xlsx"


def main():

    df_vv = pd.DataFrame()
    for i in [1,'1_2',2,3,4,5,6,7,8,'Empty']:
        VAR_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Microsoft\Classified\VecVar_microsoft_commit_"+str(i)+".xlsx"
        df_vv = df_vv.append(pd.read_excel(VAR_XLSX ,header=0),ignore_index=True)

    df_vv = df_vv.drop_duplicates(subset=['repo_id','yearmonth2008'], keep='last')

    df_l = pd.read_excel(DATA_XLSX,header= 0)
    print(df_l.shape)

    df_r = df_vv 
    print(df_r.shape)


    df_m = df_l.merge(df_r, left_on=['repo_id','yearmonth2008'],how='left', right_on=['repo_id','yearmonth2008'])
    
    # df_m['location'] = 'china'
    
    # df_m =df_m.drop(['github username','Unnamed: 0_x','Unnamed: 0_y', 'match confirm'], axis = 1)
    # df_m =df_m.drop(['stack ID_y','stack url_y'], axis = 1)
    
    
    # df_m =df_m.drop(['stack url','bio','sponsor_created_2018yearmonth','email','sponsorsListing_tiers_edges','sponsorshipsAsMaintainer_nodes'], axis = 1)
    print(df_m.shape)
    df_m.to_excel(MERGE, index = False)  
 
if __name__ == '__main__':
  main()
  