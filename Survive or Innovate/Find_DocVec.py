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

from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier

from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from nltk.tokenize import word_tokenize

TRAIN_Full= r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023/Label_Full.xlsx'
TRAIN_SET = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Train_Clean Final.xlsx'
TEST_SET = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Testset.xlsx'
LABELFULL_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\Classifiers\2023\Trainout.csv'

# COMMIT_XLSX =r"C:\Users\pmedappa\Dropbox\Research\3_Project 3 - Roles and Coordination\Editables\RR1\Test_sample.xlsx"
CLASS_COMMIT_XLSX =r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\SI_2023\Class_Commit_6001_6570.xlsx"

VAR_XLSX = r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\SI_2023\var_Commit_6001_6570.xlsx"

COMMIT_XLSX_LIST =[
                   r"C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo/RepoCommit6001_6570_1.xlsx"]
                                        
 
def getnoelements(x):
    """ Parse str represetation of python object into a list and return lenght"""
    no_parents = len(ast.literal_eval(x))
    return no_parents

def geticommit(x):
    """Find commit with code and file changes"""
    if pd.isna(x['PINDEX']) and x['MAIN_LANGUAGE']:
        return pd.Series([x['REPO_ID'],x['PUSHED_DATE'],x['MAIN_LANGUAGE'],x['NO_LANGUAGES'],x['SCRIPT_SIZE'],x['STARS'],x['SUBSCRIPTIONS']], index=['commit_oid', 'commit_authors_totalCount','commit_message','commit_additions','commit_deletions','commit_parents_totalCount','commit_changedFiles'])
        
    return pd.Series([np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN], index=['commit_oid','commit_authors_totalCount','commit_message','commit_additions','commit_deletions','commit_parents_totalCount','commit_changedFiles'])
    
def getVDF(TRAIN_CSV):
    """Get data from the training sample CSV and perform various cleaning and data preprocessing"""
    dataframe = pd.read_excel(TRAIN_CSV,header= 0)

    dataframe = dataframe.drop(axis=1,columns=['Commit URL', 'Sha','URL','Author Name','Author Email','Commit Date','Verification','Author Date'])
    # Shuffle the dataframe
    dataframe = shuffle(dataframe)
    # Encode type of commit
    dataframe = dataframe.assign(CommitType = lambda x: x['Type of Commit (Primary)'].str.split().str.get(0).str.strip(','))
    dataframe['CommitType_feature'] = dataframe.CommitType.replace({'Feature': 1,
                                                                       'Bug/Issue': 0,
                                                                       'Documentation': 0,
                                                                       'Peer': 0,
                                                                       'Process': 0,
                                                                       'Testing': 0})

    dataframe['CommitType_bug'] = dataframe.CommitType.replace({'Feature': 0,
                                                                       'Bug/Issue': 1,
                                                                       'Documentation': 0,
                                                                       'Peer': 0,
                                                                       'Process': 0,
                                                                       'Testing': 0})

    dataframe['CommitType_doc'] = dataframe.CommitType.replace({'Feature': 0,
                                                                       'Bug/Issue': 0,
                                                                       'Documentation': 1,
                                                                       'Peer': 0,
                                                                       'Process': 0,
                                                                       'Testing': 0})

    dataframe['CommitType_peer'] = dataframe.CommitType.replace({'Feature': 0,
                                                                       'Bug/Issue': 0,
                                                                       'Documentation': 0,
                                                                       'Peer': 1,
                                                                       'Process': 0,
                                                                       'Testing': 0})

    dataframe['CommitType_process'] = dataframe.CommitType.replace({'Feature': 0,
                                                                       'Bug/Issue': 0,
                                                                       'Documentation': 0,
                                                                       'Peer': 0,
                                                                       'Process': 1,
                                                                       'Testing': 0})

    dataframe['CommitType_test'] = dataframe.CommitType.replace({'Feature': 0,
                                                                       'Bug/Issue': 0,
                                                                       'Documentation': 0,
                                                                       'Peer': 0,
                                                                       'Process': 0,
                                                                       'Testing': 1})

    dataframe['CommitType'] = dataframe.CommitType.replace({'Feature': 1,
                                                                       'Bug/Issue': 2,
                                                                       'Documentation': 3,
                                                                       'Peer': 4,
                                                                       'Process': 5,
                                                                       'Testing': 6})

    dataframe = dataframe.drop(axis=1,columns=['Type of Commit (Primary)','Optional Type of Commit (Secondary)'])
    # Find number of parents
    dataframe['commit_parents_totalCount'] = dataframe['commit_parents_totalCount'].map(getnoelements)
    # Convert the number of lines of code into nChanges, commit_additions, commit_deletions
    # Get total number of changes
    dataframe = dataframe.assign(nChanges = lambda x : x['Lines of Code Changed'].str.split(':').str.get(1).str.split(',').str.get(0) )
    # Get total number of additions
    dataframe = dataframe.assign(commit_additions = lambda x : x['Lines of Code Changed'].str.split(':').str.get(2).str.split(',').str.get(0) )
    # Get total number of deletions
    dataframe = dataframe.assign(commit_deletions = lambda x : x['Lines of Code Changed'].str.split(':').str.get(3).str.split('}').str.get(0) )
    # Create three class labeld for novelty and usefulness
    conditions = [
        (dataframe['Novelty'] > 3),
        (dataframe['Novelty'] < 3),]
    choices = ['High', 'Low']
    dataframe['Novelty3'] = np.select(conditions, choices, default='Medium')
    
    conditions = [
        (dataframe['Usefulness'] > 3),
        (dataframe['Usefulness'] < 3),]
    choices = ['High', 'Low']
    dataframe['Usefulness3'] = np.select(conditions, choices, default='Medium')
    #Create count of words feature
    dataframe = dataframe.assign(nWords = lambda x : x['Description'].str.split().str.len() )
    dataframe_sd = dataframe.drop(dataframe[dataframe.CommitType.astype(int) == 3].index) #drop document type

    return dataframe, dataframe_sd

def vectordsc(corpus, train_text, test_text):
    """Convert the description text into ngram vector of features. Sparse matrix format"""
    word_vectorizer = TfidfVectorizer(
                                        sublinear_tf=True,
                                        strip_accents='unicode',
                                        analyzer='word',
                                        token_pattern=r'\w{1,}',
                                        stop_words='english',
                                        ngram_range=(1, 2),
                                        max_features=10000)

    word_vectorizer.fit(corpus)
    train_word_features = word_vectorizer.transform(train_text)
    test_word_features = word_vectorizer.transform(test_text)
    return train_word_features, test_word_features, word_vectorizer

def MLPmodel(train_x, train_y, test_x, test_y, LCurve = False):
    """MLP classifier model"""
    nn = MLPClassifier(
                        hidden_layer_sizes=(100),  activation='relu', solver='adam', alpha=0.001, batch_size='auto',
                        learning_rate='constant', learning_rate_init=0.001, power_t=0.5, max_iter=1000, shuffle=True,
                        random_state=None, tol=0.0001, verbose=False, warm_start=False, momentum=0.9, nesterovs_momentum=True,
                        early_stopping=False, validation_fraction=0.1, beta_1=0.9, beta_2=0.999, epsilon=1e-08)
    n = nn.fit(train_x, train_y)
    p_train = n.predict_proba(train_x)
    p_test = n.predict_proba(test_x)
    acc = n.score(test_x,test_y)
    print("accuracy is = ",  acc)

    return p_train, p_test, acc, n

def RFCmodel(train_x, train_y, test_x, test_y, LCurve = False):
    """Random forest Classifier model"""
    rfc = RandomForestClassifier(n_estimators=10)
    r = rfc.fit(train_x, train_y)
    acc = r.score(test_x,test_y)
    print("accuracy of rfc is = ", acc)
    p_train = r.predict_proba(train_x)
    p_test = r.predict_proba(test_x)

    return p_train, p_test, acc, r

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

def parsedate(df_commit):
    """Split date into year month and day columns, aggregate month strating from 2008-01-01"""

    #Commit date

    df_commit['commit_year'] = pd.to_numeric(df_commit['OWNER'].str.split('-').str[0])
    df_commit['commit_month'] = pd.to_numeric(df_commit['OWNER'].str.split('-').str[1])
    
    df_commit['commit_2008yearmonth'] = (df_commit['commit_year'] - 2008) *12 + df_commit['commit_month']         

    return df_commit    


def doc2vec(df_write):
    df_c = df_write
    repo_ids = df_c['repo_id'].unique()
    df_c['repo_id'] = df_c['repo_id'].fillna(method='ffill')
    df_c = df_c[df_c['PINDEX'].isnull()]
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
        
        tagged_data = [TaggedDocument(words=word_tokenize(_d.lower()), tags=[str(i)]) for i, _d in enumerate(temp_df['MAIN_LANGUAGE'].astype(str))]
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
                v1 = model.infer_vector(word_tokenize(str(row2['MAIN_LANGUAGE']).lower()))
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
    write_df  =  write_df.assign(nWords = lambda x : x['MAIN_LANGUAGE'].astype(str).str.split().str.len() )
    write_var.to_excel(VAR_XLSX)
    df2 = pd.DataFrame()
    df2 = write_df.groupby(['repo_id','commit_2008yearmonth'])['Noveltys2p1','Noveltys2p2','Noveltys2p3','Usefulnesss2p1',
                                                      'Usefulnesss2p2','Usefulnesss2p3','CommitType_features2p1',
                                                      'CommitType_features2p2','CommitType_bugs2p1','CommitType_bugs2p2',
                                                      'CommitType_docs2p1','CommitType_docs2p2','CommitType_peers2p1',	
                                                      'CommitType_peers2p2','CommitType_processs2p1','CommitType_processs2p2',	
                                                      'CommitType_tests2p1','CommitType_tests2p2',
                                                     ].mean()    
    print( write_var.shape[0])
    df2.to_excel(CLASS_COMMIT_XLSX)   


def main():
    pd.options.display.max_rows = 10
    pd.options.display.float_format = '{:.3f}'.format
    # vector_dataframe, vector_dataframe_sd = getVDF(TRAIN_CSV)
    # Save the full labeled data sample post processing in CSV
    # vector_dataframe.to_csv(LABELFULL_CSV)
    #Split vector data frame into training and test samples
    # df_train, df_test = train_test_split(vector_dataframe, test_size=0.2)
    df_train = pd.read_excel(TRAIN_SET,header= 0)
    df_test = pd.read_excel(TEST_SET,header= 0)
    vector_dataframe = pd.read_excel(TRAIN_Full,header= 0)
    #Reset the indices for merging other features later on

    df_train = df_train.dropna(subset=['Description'])
    df_test = df_test.dropna(subset=['Description'])
    vector_dataframe = vector_dataframe.dropna(subset=['Description'])
    df_train=df_train.reset_index()
    df_test = df_test.reset_index()
    #Convert description text into a vetor of features. Train_x,test_x are in sparse matrix format
    train_x, test_x, word_vectorizer = vectordsc(vector_dataframe['Description'], df_train['Description'], df_test['Description'] )
    acuracy = list()
    macc = list()
    macc_l = list()
    df_classify = pd.DataFrame()
    df_write = pd.DataFrame()

    for COMMIT_XLSX in COMMIT_XLSX_LIST:
            print(COMMIT_XLSX)
            df = pd.read_excel(COMMIT_XLSX,header=0)
            df = df.drop(['LICENCE_NAME','REPO_ID.1'], axis=1)
            df_write = pd.concat([df_write, df])
            # df_write['PINDEX'] = df_write['PINDEX'].fillna(df_write['REPO_ID'])
            df_write['repo_id'] = np.where(df_write.PINDEX.notnull(), df_write['REPO_ID'] , np.nan)
            print(df_write.shape)
    

    print(" rows ",df_write.shape[0])
    df_write = df_write[~df_write['NO_LANGUAGES'].isin(['Not Found'])]
    df_write = df_write[~df_write['SCRIPT_SIZE'].isin(['Not Found'])]
    dataframe_classify = df_write.apply(geticommit, axis =1 )
    print(dataframe_classify.shape)
    dataframe_classify = dataframe_classify.assign(nWords = lambda x : x['commit_message'].astype(str).str.split().str.len() )
    print(dataframe_classify.shape)
    
    word_features = word_vectorizer.transform(dataframe_classify['commit_message'].astype(str))

    for i in ["Novelty", "Usefulness"]:
        '''MLPClassifier'''
        print("************ MLP Classifier *************")
        del acuracy[:] 
        del macc[:] 
        #Stage 1  
        print("*** MLP Classifier - First stage - "+i+"5 ***")
        p_train5,p_test5, acc, classifier_mlp1s5 = RFCmodel(train_x, df_train[i], test_x, df_test[i]) #classify on description
        #Stage 2
        df_train_prob = pd.DataFrame(p_train5, columns = [i+'p1',i+'p2',i+'p3',i+'p4',i+'p5'])
        train_x_s2 = pd.concat([df_train_prob,df_train['commit_changedFiles'],df_train['commit_additions'],df_train['commit_deletions'],df_train['commit_parents_totalCount'],df_train['nWords']], axis=1)
        df_test_prob = pd.DataFrame(p_test5, columns = [i+'p1',i+'p2',i+'p3',i+'p4',i+'p5'])
        test_x_s2 = pd.concat([df_test_prob,df_test['commit_changedFiles'],df_test['commit_additions'],df_test['commit_deletions'],df_test['commit_parents_totalCount'],df_test['nWords']], axis=1)
        print("*** MLP Classifier - Two stage - "+i+"5 ***")
        p_train_s2,p_test_s2, acc, classifier_mlp2s5 = MLPmodel(train_x_s2, df_train[i+'3'], test_x_s2, df_test[i+'3'], LCurve = False)
        acuracy.append(["MLP Classifier - Two stage - "+i+"5", float(acc),classifier_mlp1s5, classifier_mlp2s5])
        print("MLP Classifier - Two stage - "+i+"5 ", acc)

        print("*** MLP Classifier - One stage - "+i+"3 ***")
        p_train3,p_test3, acc, classifier_mlp1s3 = MLPmodel(train_x, df_train[i+'3'], test_x, df_test[i+'3'])
        acuracy.append(["MLP Classifier - One stage - "+i+"3", float(acc), classifier_mlp1s3])
        
        #Stage 2
        df_train_prob = pd.DataFrame(p_train3, columns = [i+'p1',i+'p2',i+'p3'])
        train_x_s2 = pd.concat([df_train_prob,df_train['commit_changedFiles'],df_train['commit_additions'],df_train['commit_deletions'],df_train['commit_parents_totalCount'],df_train['nWords']], axis=1)
        df_test_prob = pd.DataFrame(p_test3, columns = [i+'p1',i+'p2',i+'p3'])
        test_x_s2 = pd.concat([df_test_prob,df_test['commit_changedFiles'],df_test['commit_additions'],df_test['commit_deletions'],df_test['commit_parents_totalCount'],df_test['nWords']], axis=1)
        print("*** MLP Classifier - Two stage - "+i+"3 ***")
        p_train_s2,p_test_s2, acc, classifier_mlp2s3 = MLPmodel(train_x_s2, df_train[i+'3'], test_x_s2, df_test[i+'3'], LCurve = False)
        acuracy.append(["MLP Classifier - Two stage - "+i+"3", float(acc),classifier_mlp1s3, classifier_mlp2s3])
        print("MLP Classifier - Two stage - "+i+"3 ", acc)
    
        # USE 1S-5p, 2S - 3p MLP classifier for classifying the commits
    
        p_classify5 = classifier_mlp1s5.predict_proba(word_features)
        df_classify_prob = pd.DataFrame(p_classify5, columns = [i+'p1',i+'p2',i+'p3',i+'p4',i+'p5'])
        classify_x_s1 = pd.DataFrame(pd.concat([df_classify_prob,dataframe_classify['commit_changedFiles'],dataframe_classify['commit_additions'],dataframe_classify['commit_deletions'],dataframe_classify['commit_parents_totalCount'],dataframe_classify['nWords']], axis=1) ).fillna(0)
        p_classify_s2  = classifier_mlp2s5.predict_proba(classify_x_s1)
        classify_x_s2  = pd.DataFrame(p_classify_s2, columns = [i+'s2p1',i+'s2p2',i+'s2p3'])
        
        df_classify = pd.concat([df_classify, classify_x_s2], axis=1)
        
        macc = max(acuracy, key=lambda x: x[1])
        print("MAX ACCURACY - = ",macc)
        macc_l.append([macc[0],macc[1],macc[2]]) 


    
    for i in ["CommitType_feature","CommitType_bug","CommitType_doc","CommitType_peer","CommitType_process","CommitType_test"]:
        '''MLPClassifier'''
        print("************ MLP Classifier *************")
        del acuracy[:] 
        del macc[:] 
        #Stage 1  
        print("*** MLP Classifier - One stage - "+i+" ***")
        p_train5,p_test5, acc, classifier_mlp1s5 = MLPmodel(train_x, df_train[i], test_x, df_test[i]) #classify on description
        #Stage 2
        df_train_prob = pd.DataFrame(p_train5, columns = [i+'p1',i+'p2'])
        train_x_s2 = pd.concat([df_train_prob,df_train['commit_changedFiles'],df_train['commit_additions'],df_train['commit_deletions'],df_train['commit_parents_totalCount'],df_train['nWords']], axis=1)
        df_test_prob = pd.DataFrame(p_test5, columns = [i+'p1',i+'p2'])
        test_x_s2 = pd.concat([df_test_prob,df_test['commit_changedFiles'],df_test['commit_additions'],df_test['commit_deletions'],df_test['commit_parents_totalCount'],df_test['nWords']], axis=1)
        print("*** MLP Classifier - Two stage - "+i+" ***")
        p_train_s2,p_test_s2, acc, classifier_mlp2s5 = MLPmodel(train_x_s2, df_train[i], test_x_s2, df_test[i], LCurve = False)
        acuracy.append(["MLP Classifier - Two stage - "+i, float(acc),classifier_mlp1s5, classifier_mlp2s5])
        print("MLP Classifier - Two stage - "+i, acc)

    
        # USE 1S-5p, 2S - 3p MLP classifier for classifying the commits
    
        p_classify5 = classifier_mlp1s5.predict_proba(word_features)
        df_classify_prob = pd.DataFrame(p_classify5, columns = [i+'p1',i+'p2'])
        classify_x_s1 = pd.DataFrame(pd.concat([df_classify_prob,dataframe_classify['commit_changedFiles'],dataframe_classify['commit_additions'],dataframe_classify['commit_deletions'],dataframe_classify['commit_parents_totalCount'],dataframe_classify['nWords']], axis=1) ).fillna(0)
        p_classify_s2  = classifier_mlp2s5.predict_proba(classify_x_s1)
        classify_x_s2  = pd.DataFrame(p_classify_s2, columns = [i+'s2p1',i+'s2p2'])
        
        df_classify = pd.concat([df_classify, classify_x_s2], axis=1)
        
        macc = max(acuracy, key=lambda x: x[1])
        print("MAX ACCURACY - = ",macc)
        macc_l.append([macc[0],macc[1],macc[2]])
    
    print(df_write.shape[0],df_classify.shape[0])
    df_write = pd.concat([df_write,df_classify], axis=1)
    print(df_write.shape[0])
    print("Finding vectors")
    doc2vec(df_write)



if __name__ == '__main__':
  main()
  