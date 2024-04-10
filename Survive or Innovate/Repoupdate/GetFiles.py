# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 16:50:25 2024

@author: pmedappa
"""
import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLib')
    print(sys.path)

from poo_ghmodules import gettoken
import pandas as pd
import numpy as np
import requests
from time import sleep
import ast
MAX_ROWS_PERWRITE = 100

DF_REPO = pd.DataFrame()
DF_COUNT = 0

def appendrowindf(user_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(row, ignore_index = True, sort=False)
    DF_COUNT = DF_COUNT + 1
    if DF_COUNT >= MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,header= 0)
        df= df.append(DF_REPO, ignore_index = True, sort=False)
        df.to_excel(user_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()


def run_query(row,user_xl): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    print(row['NAME'])
    TOKEN = gettoken(r"C:\Users\pmedappa\Dropbox\Code\PW\GHtoken.txt")
    headers = {"Authorization": "Bearer "+ TOKEN }

    query = """ 
 query {
        rateLimit {
          cost
          remaining
          resetAt
        }    
  repository(owner:\""""+str(row['OWNER'])+"""\", name: \""""+str(row['NAME'])+"""\") {
    object(expression: "HEAD:") {
      ... on Tree {
        entries {
          name
          type
          mode
          language {
            name
          }
          lineCount

        }
      }
    }
  }
}
    """
   
    try:
        request = requests.post('https://api.github.com/graphql', json={'query':query}, headers=headers)
        req_json = request.json()
        print(req_json['data']['rateLimit']['remaining'])
        if int(req_json['data']['rateLimit']['remaining']) <100:
            print("sleeping ........")
            sleep(60)
    except:
        print("Error getting starting cursor")
        print(req_json)
        temp_row = list()
        temp_row.append("")


        
        user_row_temp= row.append(pd.Series(temp_row), ignore_index = True)
        appendrowindf(user_xl, user_row_temp) 
        return 404
    parse_query_response(req_json,row,user_xl)
 
    return 0

def parse_query_response(req_json,user_row,user_xl):
    temp_row = list()
    combine_temp = list()
        
    try:
        files = req_json['data']['repository']['object']['entries']
        
    except: 
        temp_row.append("")
        user_row_temp= user_row.append(pd.Series(temp_row), ignore_index = True)
        appendrowindf(user_xl, user_row_temp)  
        return
    
    for f in files:

        try:         
            name = f['name']
            
        except:
            name = ''
        try:
            language = f['language']['name']
        except:
            language = ""
            
        try:
            lines = f['lineCount']
        except:
            lines = ""
            
        temp_tuple = tuple([name, language,lines]) 
                       

        combine_temp.append(temp_tuple)
    temp_row.append(combine_temp)
    user_row_temp= user_row.append(pd.Series(temp_row), ignore_index = True)
    appendrowindf(user_xl, user_row_temp)  
    
    print(temp_row)

    
    
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    r_user_xl = r"C:\Users\pmedappa\Dropbox\Research\4_Survie or Innovate\Data\Creativity_Survival_CEM_IV_4.xlsx"
    w_user_xl = r"C:\Users\pmedappa\Dropbox\Research\4_Survie or Innovate\Data\Creativity_Survival_CEM_IV_4_Doc.xlsx"
    user_df = pd.read_excel(r_user_xl,header= 0)
    df_test = pd.DataFrame()
    df_test.to_excel(w_user_xl, index = False) 
    col = user_df.columns.tolist()
    col.append('TotalFiles')
    

    for i,row in user_df.iterrows():
        run_query(row,w_user_xl)
    

    df = pd.read_excel(w_user_xl,header= 0)
    df= df.append(DF_REPO, ignore_index = True, sort=False)

    df.columns = col
    

    df.to_excel(w_user_xl, index = False) 
    DF_COUNT = 0
    DF_REPO = pd.DataFrame()

main()