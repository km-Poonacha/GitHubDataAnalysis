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

TRAIN_CSV = r'C:/Users/pmedappa/Dropbox/HEC/Project 5 - Roles and Coordination/Data/ML/Commit Creativity - Train3_Details.xlsx'
TRAIN_SET = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\Classifier 66 62\Trainset.xlsx'
TEST_SET = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\Classifier 66 62\Testset.xlsx'
LABELFULL_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\ML/Trainout.csv'

COMMIT_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\google_commit_Empty.xlsx"
COMMIT2_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\Vec_google_commit_Empty.xlsx"
VAR_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Organization_Specific\Classified\VecVar_google_commit_Empty.xlsx"

def getnoelements(x):
    """ Parse str represetation of python object into a list and return lenght"""
    no_parents = len(ast.literal_eval(x))
    return no_parents

def parsedate(df_commit):
    """Split date into year month and day columns, aggregate month strating from 2008-01-01"""

    #Commit date

    df_commit['commit_year'] = pd.to_numeric(df_commit['commit_committedDate'].str.split('-').str[0])
    df_commit['commit_month'] = pd.to_numeric(df_commit['commit_committedDate'].str.split('-').str[1])
    
    df_commit['commit_2008yearmonth'] = (df_commit['commit_year'] - 2008) *12 + df_commit['commit_month']         

    return df_commit    


def vectordsc(corpus):
    """Convert the description text into ngram vector of features. Sparse matrix format"""
    corpus = corpus.dropna()
    word_vectorizer = TfidfVectorizer(
                                        sublinear_tf=True,
                                        strip_accents='unicode',
                                        analyzer='word',
                                        token_pattern=r'\w{1,}',
                                        stop_words='english',
                                        ngram_range=(1, 2),
                                        max_features=1000)

    word_vectorizer.fit(corpus)
    return word_vectorizer

def initialize_D2V(tagged_data,vec_size,alpha):
    model = Doc2Vec(vector_size=vec_size,
                    alpha=alpha, 
                    min_alpha=0.00025,
                    min_count=1,
                    dm =1)
      
    model.build_vocab(tagged_data)  
    return model

def trainD2V(tagged_data,model,max_epochs):  
    for epoch in range(max_epochs):
        print('iteration {0}'.format(epoch))
        model.train(tagged_data,
                    total_examples=model.corpus_count,
                    epochs=model.epochs
                    )
        # decrease the learning rate
        model.alpha -= 0.0002
        # fix the learning rate, no decay
        model.min_alpha = model.alpha
    return model

def main():
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format

    df_c = pd.read_excel(COMMIT_XLSX,header=0)
    repo_ids = df_c['repo_id'].unique()
    df_c['repo_id'] = df_c['repo_id'].fillna(method='ffill')
    write_df = pd.DataFrame()
    write_var = pd.DataFrame()
    for row in repo_ids:
        print("Repo: ",row)
        repo_id = row
        temp_df = df_c[df_c.repo_id == row]
        if len(temp_df) < 2:
            continue
        # model = gensim.models.Word2Vec(temp_df['commit_message'], size=100)
        # word_vectorizer = vectordsc(temp_df['commit_message'] )
        temp_df = parsedate(temp_df)
        
        tagged_data = [TaggedDocument(words=word_tokenize(_d.lower()), tags=[str(i)]) for i, _d in enumerate(temp_df['commit_message'].astype(str))]
        model = initialize_D2V(tagged_data,50,0.025)
        max_epochs = 20
        model = trainD2V(tagged_data,model,max_epochs)
       
        model.save("d2v.model")
        print("Model Saved")

        print(" Comits ",temp_df.shape[0])
       
        ym_l = temp_df['commit_2008yearmonth'].unique()
        ym_l = ym_l[~np.isnan(ym_l)]

        for ym in ym_l:
            print("ym: ",ym)
            ym_df = temp_df[temp_df['commit_2008yearmonth'] == ym].reset_index()           
            vec = np.empty((0, 50))                        
            for index, row2 in ym_df.iterrows():
                v1 = model.infer_vector(word_tokenize(str(row2['commit_message']).lower()))
                vec = np.vstack((vec, v1))
          
            ym_df['docvec'] =  pd.Series(vec.tolist())

            # ym_df = ym_df.assign(docvec = pd.Series(vec.tolist()))
            # print(ym_df['docvec'])
            var = vec.var(axis = 0)

            print(row,ym,var.mean())
            write_var = pd.concat([write_var, pd.DataFrame([[repo_id,ym,var.mean()]], columns=["repo_id","yearmonth2008","vec_variance"])], 
                                          ignore_index=True)
            # temp_df ['word_vec'] =  temp_df ['word_vec'].todense()
            # df2 = temp_df.groupby('commit_2008yearmonth')['docvec'].var()
   
            write_df = write_df.append(ym_df, ignore_index = True)
    write_df  =  write_df.assign(nWords = lambda x : x['commit_message'].astype(str).str.split().str.len() )
    write_var.to_excel(VAR_XLSX)
    
    print( write_var.shape[0])
    write_df.to_excel(COMMIT2_XLSX)   


if __name__ == '__main__':
  main()
  