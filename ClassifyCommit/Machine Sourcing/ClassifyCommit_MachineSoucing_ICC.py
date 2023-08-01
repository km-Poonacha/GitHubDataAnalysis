# -*- coding: utf-8 -*-
"""
Created on Wed Jan  2 10:30:42 2019

@author: kmpoo

This is the code that tries to build a clasifier model for creativity and uses it to label any commit.
"""

import pandas as pd
import numpy as np
import ast
from time import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.utils import shuffle #To shuffle the dataframe
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
from scipy.sparse import hstack
import matplotlib.pyplot as plt
from sklearn.model_selection import learning_curve
from numpy import array
from numpy import float32
from sklearn.metrics import f1_score
from sklearn.metrics import mean_squared_error

TRAIN_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\LabelDatset_Organised_2023_2.xlsx'
TRAIN_XL_TEXT = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\LabelDatset_Text_2023.xlsx'
LABELFULL_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\PreProcessed.xlsx'
TRAINSET_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\Trainset.xlsx'
TESTSET_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\Testset.xlsx'

CHECK_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\CHECK.xlsx'
CHECK1_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\CHECK1.xlsx'
CHECK2_XL = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\CHECK2.xlsx'


def getnoelements(x):
    """ Parse str represetation of python object into a list and return lenght"""
    no_parents = len(ast.literal_eval(x))
    return no_parents

def geticommit(x):
    text_file = ['txt','md']
    if pd.isna(x['PINDEX']) and pd.notna(x['OPEN_ISSUES']):
        files = ast.literal_eval(x['OPEN_ISSUES'])
        for i in files:
            l = i.split('.')
            if len(l) > 1:
                if l[1].lower() not in text_file:
                    return pd.Series([x['PUSHED_DATE'],x['MAIN_LANGUAGE'],x['NO_LANGUAGES'],x['SCRIPT_SIZE'],x['STARS'],x['SUBSCRIPTIONS']], index=['Comments','Message','Added','Deleted','Parents','Files'])

    return pd.Series([np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN], index=['Comments','Message','Added','Deleted','Parents','Files'])
    
def getVDF(TRAIN_XL):
    """Get data from the training sample CSV and perform various cleaning and data preprocessing"""
    dataframe = pd.read_excel(TRAIN_XL, header= 0)

    # Shuffle the dataframe
    # dataframe = shuffle(dataframe)
    # Encode type of commit
    if 'VECTORS' in dataframe.columns:
        dataframe = dataframe.dropna(subset=["VECTORS"])

        dataframe = dataframe.drop_duplicates(subset=['VECTORS'],keep='last')
        dataframe = dataframe[dataframe['COUNT2']<3]
        # dataframe.to_excel(CHECK_XL)
        print(dataframe.shape)
    else:
        dataframe = dataframe.dropna(subset=["C_Description"])
        print(dataframe.shape) 
    dataframe = dataframe.assign(CommitType = lambda x: x['Type of Commit (Primary)'].str.split().str.get(0).str.strip(','))
    dataframe.CommitType = dataframe.CommitType.replace({'Feature': 1,
                                                                       'Bug/Issue': 2,
                                                                       'Documentation': 3,
                                                                       'Peer': 4,
                                                                       'Process': 5,
                                                                       'Testing': 6})
    dataframe = dataframe.drop(axis=1,columns=['Type of Commit (Primary)','Optional Type of Commit (Secondary)'])
    
 
    # Create three class labeld for novelty and usefulness
    conditions = [
        (dataframe['Novelty'] > 3),
        (dataframe['Novelty'] < 3),]
    choices = [2, 0]
    dataframe['Novelty3'] = np.select(conditions, choices, default=1)
    
    conditions = [
        (dataframe['Usefulness'] > 3),
        (dataframe['Usefulness'] < 3),]
    choices = [2, 0]
    dataframe['Usefulness3'] = np.select(conditions, choices, default=1)
    #Create count of words feature
    dataframe = dataframe.assign(nWords = lambda x : x['C_Description'].str.split().str.len() )
#    dataframe_sd = dataframe.drop(dataframe[dataframe.CommitType.astype(int) == 3].index)

    return dataframe

def vectordsc(corpus, train_text, test_text):
    """Convert the description text into ngram vector of features. Sparse matrix format"""
    word_vectorizer = TfidfVectorizer(
                                        sublinear_tf=True,
                                        strip_accents='unicode',
                                        analyzer='word',
                                        token_pattern=r'\w{1,}',
                                        stop_words='english',
                                        ngram_range=(1, 2),
                                        max_features=5000)

    word_vectorizer.fit(corpus)
    train_word_features = word_vectorizer.transform(train_text)
    test_word_features = word_vectorizer.transform(test_text)
    return train_word_features, test_word_features, word_vectorizer

def MLPmodel(train_x, train_y, test_x, test_y, LCurve = False):
    """MLP classifier model"""
    nn = MLPClassifier(
                        hidden_layer_sizes=(100),  activation='relu', solver='adam', alpha=0.001, batch_size='auto',
                        learning_rate='constant', learning_rate_init=0.001, power_t=0.5, max_iter=1000, shuffle=False,
                        random_state=None, tol=0.0001, verbose=False, warm_start=False, momentum=0.9, nesterovs_momentum=True,
                        early_stopping=False, validation_fraction=0.1, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
    n = nn.fit(train_x, train_y)
    p_train = n.predict_proba(train_x)
    p_test = n.predict_proba(test_x)
    pred_test = n.predict(test_x)
    acc = n.score(test_x,test_y)
    pred_test = n.predict(test_x)   
    print("accuracy is = ",  acc)
    print('f1 ', f1_score(test_y, pred_test, average='macro'))
    print("mse is = ",   mean_squared_error(test_y,pred_test))
    # print("recall is = ",  recall_score(test_y, pred_test, average='macro'))
    pd.DataFrame([[test_y, pred_test]])
    return p_train, p_test, acc, n

def RFCmodel(train_x, train_y, test_x, test_y, LCurve = False):
    """Random forest Classifier model"""
    rfc = RandomForestClassifier(n_estimators=10)
    r = rfc.fit(train_x, train_y)
    acc = r.score(test_x,test_y)
    print("accuracy of rfc is = ", acc)
    pred_test = r.predict(test_x)
    print('f1 ', f1_score(test_y, pred_test, average='macro'))
    p_train = r.predict_proba(train_x)
    p_test = r.predict_proba(test_x)
    
    pred_test = r.predict(test_x)  
    print("mse is = ",   mean_squared_error(test_y,pred_test))
    # print("recall is = ",  recall_score(test_y, pred_test, average='macro'))
    return p_train, p_test, acc, r

def organizevectors(df):
    df['lVec'] = np.array(df['VECTORS'].apply(lambda x : "" if x in ("",np.nan) else dict(eval(x))['f']))
    # df['lVec'] = df['VECTORS'].apply(lambda x : "" if x in ("",np.nan) else dict(eval(x))).apply(pd.Series)
    
    df= pd.concat([df,(df['lVec'].apply(pd.Series))], axis = 1)

    return df
def prepfulldata(XL):
    dataframe = pd.read_excel(XL, header= 0)
    
    dataframe.rename(columns={'PUSHED_DATE':'C_Comments', 'MAIN_LANGUAGE':'C_Description', 'NO_LANGUAGES':'C_Additions','SCRIPT_SIZE':'C_Deletions','STARS':'C_nParents','SUBSCRIPTIONS':'C_nFiles'}, inplace=True)
    dataframe = dataframe.dropna(subset=['VECTORS', 'DEG_SUPER', 'LICENCE_NAME'], how='all')
    dataframe=  dataframe.reset_index()
    dataframe = dataframe.assign(nWords = lambda x : x['C_Description'].str.split().str.len() )    
    dataframe['C_Description'].fillna('', inplace=True)
    
    return dataframe

def main():
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    
    vector_dataframe = getVDF(TRAIN_XL)
    text_dataframe = getVDF(TRAIN_XL_TEXT)
    vector_dataframe  = organizevectors(vector_dataframe)
    
 
    df2 = pd.DataFrame()
    list_c = [i for i in range(0,384)]
    # list_c = ['lVec2']

    list_o = ['C_Description','C_Author_Email','C_Additions','C_Author_Date','C_Commiter_Name','C_Comments','REPO_ID','C_Deletions',
              'C_Commiter_Email','C_nParents','C_nFiles','Commit URL','Novelty','Usefulness','CommitType','Novelty3','Usefulness3',
              'nWords']
    list_oc = list_o
    list_oc.extend(list_c)
    df2 = pd.DataFrame()
    df2 = vector_dataframe
    
    # df2 = pd.concat([df2 , vector_dataframe.groupby('SHA')[list_c].min()], axis=1)
    
   # Save the full labeled data sample post processing in CSV
#    vector_dataframe = vector_dataframe.drop_duplicates(subset = ['C_Description'])
    
    vector_dataframe.to_excel(LABELFULL_XL)
    
    #Split vector data frame into training and test samples
    df_train, df_test = train_test_split(df2, test_size=0.15)
    # df_train = df2[:1400]
    # df_test = df2[1400:]
    t_df_train, t_df_test = train_test_split(text_dataframe , test_size=0.15)
    #Reset the indices for merging other features later on
    df_train=df_train.reset_index()
    df_test = df_test.reset_index()
    
    t_df_train=t_df_train.reset_index()
    t_df_test = t_df_test.reset_index()
    #Create new CSV represnting the training and testing samples
    df_train.to_excel(TRAINSET_XL)
    df_test.to_excel(TESTSET_XL)
    #Convert description text into a vetor of features. Train_x,test_x are in sparse matrix format
    #Use different file since number of textual descriptions available is greater 
    t_train_x, t_test_x, word_vectorizer = vectordsc(text_dataframe['C_Description'], t_df_train['C_Description'], t_df_test['C_Description'] )



    for i in ["Novelty","Usefulness"]:
        '''MLPClassifier'''
        print("************ TEXT ONLY *************")
        print("* RFC Classifier - One Stage - "+i+"5 *")
        train_text_vec= word_vectorizer.transform(t_df_train['C_Description'])
        test_text_vec = word_vectorizer.transform(t_df_test['C_Description'])
        
        tp_train5,tp_test5, acc, classifier_textonly =  RFCmodel(train_text_vec, t_df_train[i], test_text_vec , t_df_test[i])
        temp_prob = pd.DataFrame(classifier_textonly.predict_proba(test_text_vec), columns = [i+'_tp1',i+'_tp2',i+'_tp3',i+'_tp4',i+'_tp5'])
        temp_pred = pd.DataFrame(classifier_textonly.predict(test_text_vec), columns = [i+'_LabelPrediction'])
        full_vec_df = pd.concat([t_df_test[i],temp_pred], axis=1)
        OUT_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\\"+i+"_RFC_TEXT_OUT.xlsx"
        full_vec_df.to_excel(OUT_XL)   
        
        print("* MLP Classifier - One stage - "+i+"5 *")
        tp_train5,tp_test5, acc, classifier_textonly = MLPmodel(train_text_vec, t_df_train[i], test_text_vec , t_df_test[i])
        
        #Run the model

        # temp_vec = np.array(full_vec_df['C_Description'].apply(lambda x : "" if x in ("",np.nan) else word_vectorizer.transform(x)))
        temp_prob = pd.DataFrame(classifier_textonly.predict_proba(test_text_vec), columns = [i+'_tp1',i+'_tp2',i+'_tp3',i+'_tp4',i+'_tp5'])
        temp_pred = pd.DataFrame(classifier_textonly.predict(test_text_vec), columns = [i+'_LabelPrediction'])
        
        full_vec_df = pd.concat([t_df_test[i],temp_pred], axis=1)
        OUT_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\\"+i+"_MLP_TEXT_OUT.xlsx"
        full_vec_df.to_excel(OUT_XL) 
        
        print("************ CODE ONLY *************") 
        print("* RFC Classifier - One stage - "+i+"5 *")
        
        c_vec_train = df_train[list_c]
        c_vec_test = df_test[list_c]
        vp_train5,vp_test5, acc, classifier_codeonly = RFCmodel(c_vec_train, df_train[i], c_vec_test, df_test[i]) 
        temp_prob = pd.DataFrame(classifier_codeonly.predict_proba(c_vec_test), columns = [i+'_cp1',i+'_cp2',i+'_cp3',i+'_cp4',i+'_cp5'])
        temp_pred = pd.DataFrame(classifier_codeonly.predict(c_vec_test), columns = [i+'_LabelPrediction'])
        full_vec_df = pd.concat([df_test[i],temp_pred], axis=1)
        
        OUT_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\\"+i+"_RFC_CODE_OUT.xlsx"
        full_vec_df.to_excel(OUT_XL) 
        
        print("* MLP Classifier - One stage - "+i+"5 *")
        vp_train5,vp_test5, acc, classifier_codeonly = MLPmodel(c_vec_train, df_train[i], c_vec_test, df_test[i])                
        code_train_prob = pd.DataFrame(vp_train5, columns = [i+'_cp1',i+'_cp2',i+'_cp3',i+'_cp4',i+'_cp5'])
        code_test_prob = pd.DataFrame(vp_test5, columns = [i+'_cp1',i+'_cp2',i+'_cp3',i+'_cp4',i+'_cp5'])  
        
        temp_prob = pd.DataFrame(classifier_codeonly.predict_proba(c_vec_test), columns = [i+'_cp1',i+'_cp2',i+'_cp3',i+'_cp4',i+'_cp5'])
        temp_pred = pd.DataFrame(classifier_codeonly.predict(c_vec_test), columns = [i+'_LabelPrediction'])
        full_vec_df = pd.concat([df_test[i],temp_pred], axis=1)
        
        OUT_XL = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\ML\\"+i+"_MLP_CODE_OUT.xlsx"
        full_vec_df.to_excel(OUT_XL) 
                
        # print("*** MLP Classifier - One stage - "+i+"3 ***")
        # print("*** CODE ***")
        # tp2_train3,tp2_test3, acc, classifier_mlp1s5 = MLPmodel(c_vec_train,  df_train[i+'3'], c_vec_test, df_test[i+'3'])        
        

if __name__ == '__main__':
  main()
  