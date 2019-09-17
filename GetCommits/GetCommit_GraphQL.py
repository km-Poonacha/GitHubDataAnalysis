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
PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW\PW_GitHub3.csv'
LOG_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\\2014GithubRepoData_Latest\RepoCommit_log.csv'

def run_query(name, owner): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    headers = {"Authorization": "Bearer "+"dfb9844388015057b2bb8331c562068b04d9807f"}  
    endc = ""
    commit_df = pd.DataFrame()
    while True:
        query = """
                    query{
                      rateLimit {
                        cost
                        remaining
                        resetAt
                      }
                      repository(name: """+name+""", owner: """+owner+""") {
                        ref(qualifiedName: "master") {
                          target {
                            ... on Commit {
                              history(first:2 until: "2016-01-01T00:00:00Z" """+endc+"""){
                                totalCount
                                pageInfo {
                                  endCursor
                                  hasNextPage
                                }
                                    edges {
                                      node {
                                        comments{
                                          totalCount
                                        }
                                        parents{
                                          totalCount
                                        }
                                        changedFiles
                                        additions
                                        deletions
                                        messageHeadline
                                        oid
                                        message
                                        author {
                                          name
                                          email
                                          date                        
                                          }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """
        try:
            request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
            req_json = request.json()

            endcursor = req_json['data']['repository']['ref']['target']['history']['pageInfo']['endCursor']            
            print(endcursor)
            commits = req_json['data']['repository']['ref']['target']['history']['edges']
            for commit in commits:
                print(commit['node']['changedFiles'])
            return req_json
        except:
            break
        
    return "404"
       


def get_name(repo_id):
    repo_url = "https://api.github.com/repositories/"+str(repo_id)
    repo_req = getGitHubapi(repo_url,PW_CSV,LOG_CSV)
    if repo_req:
        repo_json = repo_req.json()
        return repo_json['name'], repo_json['owner']['login']
    else:
        return None, None

def main():
    """Main function"""   
    repo_csv = "C:\\Users\pmedappa\Dropbox\HEC\\2014GithubRepoData_Latest\FullData_20190604IVT_COLAB_Test5.csv"
    with open(repo_csv, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        rcount = 1
        for repo_row in repo_handle:
            repo_id = repo_row[1]            
            name, owner = get_name(repo_id)  
            result = "0"
            if name:  
                result = run_query(name, owner)            
#                print(result)
                if result == '404':
                    print("Error runnig graphql")
            
main()