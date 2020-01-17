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
import json
MAX_ROWS_PERWRITE = 100
DF_REPO = pd.DataFrame()
DF_COUNT = 0

PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW\PW_GitHub3.csv'
LOG_CSV = 'C:\\Data\\092019 CommitInfo\\RepoCommit_log.csv'

def run_query(name, owner, NEWREPO_xl): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    headers = {"Authorization": "Bearer "+"dfb9844388015057b2bb8331c562068b04d9807f"}  
    endc = None
    commit_row = list() 
    end = False
    while not end:
        query = """
                    query($cursor:String){
                      rateLimit {
                        cost
                        remaining
                        resetAt
                      }
                      repository(name: "react", owner:"facebook") {
                        ref(qualifiedName: "master") {
                          target {
                            ... on Commit {
                              history(first:100 after:$cursor){
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
                                        parents(first:100){
                                          totalCount
                                          nodes{
                                            oid
                                            author {
                                              date
                                              email
                                              name
                                            }
                                          }
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
                                        committer {
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
        variables = {
                 "cursor" : endc}
            
        body = {
                "query": query,
                "variables": variables
                    }
        try:
            request = requests.post('https://api.github.com/graphql', json=body, headers=headers)
            req_json = request.json()
        except:
            print("Error running graphql")
            if req_json['data']['repository']['ref']['target']['history']['pageInfo']['hasNextPage']:     
                endc= req_json['data']['repository']['ref']['target']['history']['pageInfo']['endCursor']
                print("Endcursor : ", endc)
            else:
                end = True    
            break
        
        if req_json['data']['repository']['ref']['target']['history']['pageInfo']['hasNextPage']:     
            endc= req_json['data']['repository']['ref']['target']['history']['pageInfo']['endCursor']
            print("Endcursor : ", endc)
        else:
            end = True    
            
        commits = req_json['data']['repository']['ref']['target']['history']['edges']
        for commit in commits:
            del commit_row[:]
            commit_row.append("")
            commit_row.append(commit['node']['oid'])
            commit_row.append(commit['node']['author']['name']) 
            commit_row.append(commit['node']['author']['email']) 
            commit_row.append(commit['node']['author']['date']) 
            commit_row.append(commit['node']['committer']['name']) 
            commit_row.append(commit['node']['committer']['email']) 
            commit_row.append(commit['node']['committer']['date']) 
            commit_row.append(commit['node']['comments']['totalCount'])            
            commit_row.append(commit['node']['additions'])
            commit_row.append(commit['node']['deletions'])
            commit_row.append(commit['node']['changedFiles'])
            commit_row.append(commit['node']['parents']['totalCount']) 
            commit_row.append(commit['node']['message'])
            commit_row.append(commit['node']['parents']['nodes'])
            for parent in commit['node']['parents']['nodes']:
                commit_row.append(parent['oid'])
                commit_row.append(parent['author']['name'])
                commit_row.append(parent['author']['email'])
                commit_row.append(parent['author']['date'])                       
                appendrowindf(NEWREPO_xl, commit_row)
                del commit_row[-4:]
        

    return "404"
       


def get_name(repo_id):
    repo_url = "https://api.github.com/repositories/"+str(repo_id)
    repo_req = getGitHubapi(repo_url,PW_CSV,LOG_CSV)
    if repo_req:
        repo_json = repo_req.json()
        return repo_json['name'], repo_json['owner']['login']
    else:
        return None, None

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
    
def main():
    """Main function"""   
#    repo_csv = "C:\\Users\pmedappa\Dropbox\HEC\\2014GithubRepoData_Latest\FacebookReact.csv"
    NEWREPO_xl = 'C:\\Users\pmedappa\Dropbox\OSS Communities\Data\Commit_FacebookReact.xlsx'
    global DF_REPO 
    global DF_COUNT
    df_new = pd.DataFrame()
    df_new.to_excel(NEWREPO_xl, index = False) 
#    df_full = pd.DataFrame()
#    df_full.to_excel(NEWREPO_xl, index = False) 
#    with open(repo_csv, 'rt', encoding = 'utf-8') as repolist:
#        repo_handle = csv.reader(repolist)
#        rcount = 1
#        for repo_row in repo_handle:
#            appendrowindf(NEWREPO_xl, repo_row)
#            repo_id = repo_row[1]            
#            name, owner = get_name(repo_id)  
#            result = "0"
#            if name:  
    result = run_query("react", "facebook", NEWREPO_xl )    
    if DF_COUNT < MAX_ROWS_PERWRITE :
        df = pd.read_excel(NEWREPO_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(NEWREPO_xl, index = False) 
        DF_COUNT = 0
        DF_REPO = pd.DataFrame()        
#                print(result)
    if result == '404':
        print("Error runnig graphql")
            
main()