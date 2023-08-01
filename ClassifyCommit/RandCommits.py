# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 14:47:45 2018

@author: kmpoo

Generate a random sample of java commits. Remove all commits that do not have java files.
"""

import pandas as pd

import numpy as np
import ast

COMMIT_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\Full Java Commit\Java_RepoCommit.xlsx'

COMMIT_SAMPLE_1 = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\Full Java Commit\Random Sample\RANDJava_Commit_Sample_1.xlsx'

def clean_files(x):
    "Aggregae the probabilities calculated into a single construct"
    text_file = ['java']    
    if pd.notna(x['FILES']):
        try:
            files = ast.literal_eval(x['FILES'])
        except: 
            print("Remove ", x['SHA'])
            return pd.Series([np.NaN,np.NaN,np.NaN,np.NaN,np.NaN], index=['REPO_ID','OWNER','NAME','SHA'])
        if isinstance(files, (int)): return pd.Series([np.NaN,np.NaN,np.NaN,np.NaN,np.NaN], index=['REPO_ID','OWNER','NAME','SHA'])
        for i in files:
            l = i.split('.')
            if len(l) > 1:
                if l[1].lower() in text_file:
                    return pd.Series(x,index=['REPO_ID','OWNER','NAME','SHA'])

    return pd.Series([np.NaN,np.NaN,np.NaN,np.NaN,np.NaN], index=['REPO_ID','OWNER','NAME','SHA'])



pd.options.display.max_rows = 10
pd.options.display.float_format = '{:.1f}'.format

commit_dataframe = pd.read_excel(COMMIT_XL, header = 0)

print(" No of commit = ", commit_dataframe.shape[0] )

commit_dataframe = commit_dataframe.rename(columns ={0: 'REPO_ID', 1: 'OWNER', 2: 'NAME', 3: 'SHA'})


commit_dataframe2= commit_dataframe.apply(clean_files,  axis = 1)

commit_dataframe2 = commit_dataframe2.dropna()

print(" No of commit removed = ", commit_dataframe.shape[0] - commit_dataframe2.shape[0])

print("Total commits = ", commit_dataframe2.shape[0] )
commit_dataframe2 = commit_dataframe2.assign(url = lambda x: "http://github.com/"+x['NAME']+"/"+x['OWNER']+"/commit/"+x['SHA'])

# writer = pd.ExcelWriter(COMMIT_UPDATE_XL,options={'strings_to_urls': False})

# commit_dataframe2.to_excel(writer)

commit_dataframe2.sample(300).to_excel(COMMIT_SAMPLE_1)
