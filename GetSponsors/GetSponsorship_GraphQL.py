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
user_xl = r'C:\\Users\pmedappa\Dropbox\Course and Research Sharing\Research\MS Acquire Github\Data\Sponsor\Users_USCAN.xlsx'

def appendrowindf(NEWREPO_xl, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    global DF_REPO 
    global DF_COUNT
    DF_REPO= DF_REPO.append(pd.Series(row), ignore_index = True)
    DF_COUNT = DF_COUNT + 1
    if DF_COUNT == MAX_ROWS_PERWRITE :
        df = pd.read_excel(NEWREPO_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(NEWREPO_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()

def run_query(loc, period): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    headers = {"Authorization": "Bearer "+"fa0fcc3a388a5801ec11dacb55bed2509febbd9d"} 
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
            print(req_json['data']['rateLimit']['remaining'])
        except:
            print("Error running graphql")
            end = True
        
        if req_json['data']['search']['pageInfo']['hasNextPage']:     
            endc= req_json['data']['search']['pageInfo']['endCursor']
            print("Endcursor : ", endc)
        else:
            end = True   
            
        users = req_json['data']['search']['edges']

        for user in users:
            if(user['node']):
                print(user['node']['name'])
                if user['node']['sponsorsListing']:
                    print(user['node']['name']," ",user['node']['sponsorsListing']['createdAt'])
            
            # appendrowindf(NEWREPO_xl, commit_row)
        return req_json

    return "404"
       



    
def main():
    """Main function"""   
    global DF_REPO 
    global DF_COUNT
    search_key = ['us']#,'usa','states','america','canada','california','ca']
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
              '2008-05-31..2009-01-01','2008-01-01..2008-06-01',]

    for loc in search_key:
        for p in period:        
            run_query(loc, p) 


"""
    df_full = pd.DataFrame()
    df_full.to_excel(NEWREPO_xl, index = False) 
    with open(repo_csv, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        rcount = 1
        for repo_row in repo_handle:
            appendrowindf(NEWREPO_xl, repo_row)
            repo_id = repo_row[1]            
            name, owner = get_name(repo_id)  
            result = "0"
            if name:  
                result = run_query(name, owner, NEWREPO_xl )            
#                print(result)
                if result == '404':
                    print("Error runnig graphql")
                  
    if DF_COUNT < MAX_ROWS_PERWRITE:
        df = pd.read_excel(user_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(user_xl, index = False) 
"""              
main()