#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 16:54:21 2017

Search for all the PRs for a repository that have been created before 2015. Find the commit information for each PR and store in CSV file. 

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


PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW/\PW_GitHub.csv'
LOG_CSV = 'C:\\Data\\092019 CommitInfo\RepoCommit_log.csv'

def getmorecommitinfo(c_url):
    """Get data on individual commit"""
    commit_row = []
    del commit_row[:]
    commit_res = getGitHubapi(c_url,PW_CSV,LOG_CSV).json()

    if commit_res is None:
        print("No commit information available", c_url)
        return []
    commit_row = ghparse_row(commit_res,"stats*total", "stats*additions",prespace = 0)
    parents = commit_res['parents']
    p_no = 0
    for parent in parents:
        p_no= p_no+1    
    commit_row.append(p_no)    
    
    files = commit_res['files']
    f_name =[]
    f_stat =[]
    f_pat =[]
    f_no = 0
    del f_name[:]
    del f_stat[:]
    del f_pat[:]
    
    for file in files:
        f_no = f_no+1
        f_name.append(file['filename'])
        f_stat.append(file['status'])
        f_pat.append(file['patch'])
    commit_row.append(f_no)  
    commit_row.append(f_name)  
    commit_row.append(f_stat)  
    commit_row.append(f_pat)
    return commit_row

def getcommitinfo(repoid,df):
    commit_url = "https://api.github.com/repositories/"+str(repoid)+"/commits?per_page=100"
    while commit_url:
        commit_req = getGitHubapi(commit_url,PW_CSV,LOG_CSV)
        if commit_req:
            commit_json = commit_req.json()
            for commit in commit_json:
                commit_row = ghparse_row(commit,"sha", "commit*author*name","commit*author*email","commit*author*date", "commit*committer*name","commit*committer*email","commit*committer*date","commit*comment_count","url","commit*message", prespace = 1)
                c_list = getmorecommitinfo(commit['url'])                
                for e in c_list:
                    commit_row.append(e)
#                appendrowincsv(NEWREPO_CSV, commit_row) 
                df = appendrowindf(df, commit_row)
            commit_url = ghpaginate(commit_req)
        else:
            print("Error getting commit info ",commit_url)
            with open(LOG_CSV, 'at', encoding = 'utf-8', newline ="") as loglist:
                log_handle = csv.writer(loglist)
                log_handle.writerow(["Error getting commit",commit_url,"UNKNOWN"])
            return df
    return df                   

def appendrowincsv(csvfile, row):
    """This code appends a row into the csv file"""
    with open(csvfile, 'at', encoding = 'utf-8', newline='') as writelist:
        write_handle = csv.writer(writelist)
        write_handle.writerow(row)

def appendrowindf(df, row):
    """This code appends a row into the dataframe and returns the updated dataframe"""
    NEWREPO_xl = 'C:\\Data\\092019 CommitInfo\\RepoCommit_1.xlsx'
    df= df.append(pd.Series(row), ignore_index = True)
    df.to_excel(NEWREPO_xl) 
    return df
        
def main():
     
    # For WINDOWS 
    REPO_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\\2014GithubRepoData_Latest\FullData_TEST.csv'
#    NEWREPO_CSV = 'C:\\Data\\092019 CommitInfo\\RepoCommit_1.csv'
    NEWREPO_xl = 'C:\\Data\\092019 CommitInfo\\RepoCommit_1.xlsx'
    df_full = pd.DataFrame()
    
    with open(REPO_CSV, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        rcount = 1
        sheetno = 1
        for repo_row in repo_handle:
            repoid = repo_row[0]     
#            appendrowincsv(NEWREPO_CSV, repo_row)
            df_full = appendrowindf(df_full, repo_row)
            df_full = getcommitinfo(repoid,df_full)
            
#            getcommitinfo(repoid,NEWREPO_CSV)
#            if rcount == 500:
#                rcount = 1
#                sheetno = sheetno + 1
#                NEWREPO_CSV = 'C:\\Data\092019 CommitInfo\RepoCommit_'+str(sheetno)+'.csv'
#            rcount = rcount + 1
    df_full.to_excel(NEWREPO_xl)    
  
if __name__ == '__main__':
  main()
  