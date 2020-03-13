# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 17:04:10 2019

@author: pmedappa
"""
import csv
import sys
if "C:\\Users\\pmedappa\\\Dropbox\\HEC\\Python\\CustomLib\\PooLIB" not in sys.path:
    sys.path.append('C:\\Users\\pmedappa\\\Dropbox\\HEC\\Python\\CustomLib\\PooLIB')
    print(sys.path)
from poo_ghmodules import getGitHubapi
from poo_ghmodules import ghpaginate
from poo_ghmodules import ghparse_row

import pandas as pd
import numpy as np
import requests

MAX_ROWS_PERWRITE = 1000

DF_REPO = pd.DataFrame()
DF_COUNT = 0

PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW\PW_GitHub.csv'
LOG_CSV = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\MS Acquire Github\Data\Sponsor\UserSpon_log.csv'
headers = {"Authorization": "Bearer "+"0ffc36287e794f02657627c790aae04572035a65"} 

def appendrowindf(user_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(pd.Series(row), ignore_index = True)
    DF_COUNT = DF_COUNT + 1
    if DF_COUNT == MAX_ROWS_PERWRITE :
        df = pd.read_excel(user_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()

def run_query(loc, period,user_xl): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    q = "location:"+loc+" repos:>5 created:"+period
    
    query = """
    query{
      search(query: \""""+q+"""\", type: USER, first: 1) {
        userCount
        pageInfo {
          startCursor
          hasNextPage
        }}}"""    
    try:
        request = requests.post('https://api.github.com/graphql', json={'query':query}, headers=headers)
        req_json = request.json()
        endc = req_json['data']['search']['pageInfo']['startCursor']
    except:
        print("Error getting starting cursor")
        print(req_json)
        return 404
    
    end = False
    while not end:
        query = """
query($cursor:String! ) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  search(query: \""""+q+"""\", type: USER, first: 100, after:$cursor) {
    userCount
    pageInfo {
      endCursor
      hasNextPage
    }
    edges {
      node {
        ... on User {
          login
          name
          id
          email
          company
          bio
          location
          createdAt
          isHireable
          followers {
            totalCount
          }
          following {
            totalCount
          }
          repositories {
            totalCount
          }
          sponsorsListing {
            createdAt
            shortDescription
            name
            tiers(first: 100) {
              totalCount
              edges {
                node {
                  name
                  description
                  monthlyPriceInDollars
                  updatedAt
                }
              }
            }
          }
          sponsorshipsAsMaintainer(first: 100) {
            totalCount
            nodes {
              createdAt
              sponsor {
                login
              }
            }
          }
        }
      }
    }
  }
}
"""
        variables = {
                 "cursor" : endc
                 }
            
        body = {
                "query": query,
                "variables": variables
                    }
        try:
            request = requests.post('https://api.github.com/graphql', json=body, headers=headers)
            req_json = request.json()
            print(loc," ",period," ",req_json['data']['search']['userCount'] )
            if  int(req_json['data']['search']['userCount']) > 1000:
                # log if the total user count is greater than 1000
                with open(LOG_CSV, 'at') as logobj:                    
                    log = csv.writer(logobj)
                    l_data = list()
                    l_data.append("Search Results Exceeds 1000")
                    l_data.append(loc)
                    l_data.append(period)
                    l_data.append(req_json['data']['search']['userCount'])
                    log.writerow(l_data)
            print(req_json['data']['rateLimit']['remaining'])
        except:
            print("Error running graphql")
            end = True
            print(req_json)
            return 404
        
        if req_json['data']['search']['pageInfo']['hasNextPage']:     
            endc= req_json['data']['search']['pageInfo']['endCursor']
        else:
            end = True   
            
        users = req_json['data']['search']['edges']

        for user in users:
            user_row = list()
            if(user['node']):
                user_row = ghparse_row(user,"node*login", "node*name", "node*email", "node*company", "node*bio", "node*location",
                                       "node*createdAt", "node*isHireable", "node*followers*totalCount", "node*following*totalCount",
                                       "node*repositories*totalCount")
                if user['node']['sponsorsListing']:               
                    user_row.append(user['node']['sponsorsListing']['createdAt'])
                    user_row.append(user['node']['sponsorsListing']['shortDescription'])
                    user_row.append(user['node']['sponsorsListing']['name'])
                    user_row.append(user['node']['sponsorsListing']['tiers']['totalCount'])
                    user_row.append(user['node']['sponsorsListing']['tiers']['edges'])
                    user_row.append(user['node']['sponsorshipsAsMaintainer']['totalCount'])
                    user_row.append(user['node']['sponsorshipsAsMaintainer']['nodes'])
                else: user_row.append("")
            appendrowindf(user_xl, user_row)
        

    return 0
       

def find_i_split(loc,y):    
    q = "location:"+loc+" repos:>5 created:"+str(y)+"-01-01.."+str(y)+"-12-31"
    print(q)
    query = """
    query{
      search(query: \""""+q+"""\", type: USER, first: 1) {
        userCount
        pageInfo {
          startCursor
          hasNextPage
        }}}"""
    
    
    try:
        request = requests.post('https://api.github.com/graphql', json={'query':query}, headers=headers)
        req_json = request.json()
        cursor = req_json['data']['search']['pageInfo']['startCursor']
        u_count = int(req_json['data']['search']['userCount'])
        print("Year ",y," Total count = ", u_count)
        if u_count  < 1000:
            i_m = 0
        elif u_count < 1500:
            i_m = 1
        elif u_count > 1500 and u_count <= 3000:
            i_m = 2
        elif u_count > 3000 and u_count <= 5000:
            i_m = 3
        else:
            i_m = 4
        
    except:
        print("Error getting starting cursor")
        print(req_json)
        return 404,404
    return cursor, i_m

    
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    search_key = ['usa']#us,'usa','states','america','canada','california','ca']
    period = ['2018-05-31..2019-01-01','2018-01-01..2018-06-01',
              '2017-05-31..2018-01-01','2017-01-01..2017-06-01',
              '2016-05-31..2017-01-01','2016-01-01..2016-06-01',
              '2015-05-31..2016-01-01','2015-01-01..2015-06-01',
              '2014-05-31..2015-01-01','2014-01-01..2014-06-01',
              '2013-05-31..2014-01-01','2013-01-01..2013-06-01',
              '2012-05-31..2013-01-01','2012-01-01..2012-06-01',
              '2011-05-31..2012-01-01','2011-01-01..2011-06-01',
              '2010-05-31..2011-01-01','2010-01-01..2010-06-01',
              '2009-05-31..2010-01-01','2009-01-01..2009-06-01',
              '2008-05-31..2009-01-01','2008-01-01..2008-06-01']
    year=['2008','2009','2010','2011','2012','2013','2014','2015','2016','2017','2018']
    month = [['01'],['01','06'],['01','04','07','10'],['01','03','05','07','09','11'],['01','02','03','04','05','06','07','08','09','10','11','12']]
    for loc in search_key:
        user_xl = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\MS Acquire Github\Data\Sponsor\UserSponsor_'+loc+'.xlsx'
        df_test = pd.DataFrame()
        df_test.to_excel(user_xl, index = False) 
        # for p in period:
        #     ret_val = run_query(loc, p,user_xl)         
        for y in range(len(year)):  
            cursor, i_m = find_i_split(loc,year[y])
            print("Split found ", month[i_m])
            if int(i_m) == 404: 
                print("Ending .........")
                return
            for m in range(len(month[i_m])):        
                if m != len(month[i_m])-1: 
                    p = year[y]+'-'+month[i_m][m]+'-01..'+year[y]+'-'+month[i_m][m+1]+'-01'
                else: 
                    p = year[y]+'-'+month[i_m][m]+'-01..'+year[y]+'-'+'12'+'-31'
                ret_val = run_query(loc, p,user_xl)

                        
    if DF_COUNT < MAX_ROWS_PERWRITE:
        df = pd.read_excel(user_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
             
main()