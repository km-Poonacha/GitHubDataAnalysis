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
from datetime import datetime

import pandas as pd
import numpy as np

MAX_ROWS_PERWRITE = 1000

DF_REPO = pd.DataFrame()
DF_COUNT = 0
PW_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\Python\PW\PW_GitHub3.csv'
LOG_CSV = 'C:\\Data\\092019 CommitInfo\RepoCommit_log.csv'

def getmorecommitinfo(c_url):
    """Get data on individual commit"""
    commit_row = []
    del commit_row[:]
    commit_res = getGitHubapi(c_url,PW_CSV,LOG_CSV)

    if commit_res is None:
        print("No commit information available", c_url)
        return ["","","","",""]
    commit_json = commit_res.json()
    commit_row = ghparse_row(commit_json,"stats*total", "stats*additions",prespace = 0)
    parents = commit_json['parents']
    p_no = 0
    for parent in parents:
        p_no= p_no+1    
    commit_row.append(p_no)    
    
    files = commit_json['files']
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
        if "patch" in file:
            f_pat.append(file['patch'])
        else: f_pat.append("")

    commit_row.append(f_no)  
    commit_row.append(f_name)  
    commit_row.append(f_stat)  
    commit_row.append(f_pat)
    return commit_row

def getcommitinfo(repoid,NEWREPO_xl,owner,name):
    commit_url = "https://api.github.com/repositories/"+str(repoid)+"/commits?per_page=100"
    while commit_url:
        commit_req = getGitHubapi(commit_url,PW_CSV,LOG_CSV)
        if (commit_req is None) and (commit_url == "https://api.github.com/repositories/"+str(repoid)+"/commits?per_page=100"):    
            print("Repo ID did not work. Trying owner/name")
            commit_url = "https://api.github.com/repos/"+str(owner)+"/"+str(name)+"/commits?per_page=100"
            commit_req = getGitHubapi(commit_url,PW_CSV,LOG_CSV)
        if commit_req:
            commit_json = commit_req.json()
            for commit in commit_json:
                commit_row = ghparse_row(commit,"sha", "commit*author*name","commit*author*email","commit*author*date", "commit*committer*name","commit*committer*email","commit*committer*date","commit*comment_count","commit*message", prespace = 1)
                commit_datetime = datetime.strptime(commit['commit']['author']['date'], "%Y-%m-%dT%H:%M:%SZ")
                if int(commit_datetime.year) < 2016:
                    c_list = getmorecommitinfo(commit['url'])                
                    for e in c_list:
                        commit_row.append(e)
                appendrowindf(NEWREPO_xl, commit_row)
            commit_url = ghpaginate(commit_req)
        else:
            print("Error getting commit info ",commit_url)
            with open(LOG_CSV, 'at', encoding = 'utf-8', newline ="") as loglist:
                log_handle = csv.writer(loglist)
                log_handle.writerow(["Error getting commit",commit_url,"UNKNOWN"])
            return 1
    return 0                   

def appendrowincsv(csvfile, row):
    """This code appends a row into the csv file"""
    with open(csvfile, 'at', encoding = 'utf-8', newline='') as writelist:
        write_handle = csv.writer(writelist)
        write_handle.writerow(row)

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
     
    # For WINDOWS 
    REPO_CSV = 'C:\\Users\pmedappa\Dropbox\HEC\\2014GithubRepoData_Latest\FullData_20190604IVT_COLAB_TEST.csv'
#    NEWREPO_CSV = 'C:\\Data\\092019 CommitInfo\\RepoCommit_1.csv'
    NEWREPO_xl = 'C:\\Data\\092019 CommitInfo\\RepoCommit_TEST.xlsx'
    df_full = pd.DataFrame()
    df_full.to_excel(NEWREPO_xl, index = False) 
    with open(REPO_CSV, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        rcount = 1
        sheetno = 1
        for repo_row in repo_handle:
            repoid = repo_row[1]     
            rowner = repo_row[4] 
            rname = repo_row[3] 
#            appendrowincsv(NEWREPO_CSV, repo_row)
            appendrowindf(NEWREPO_xl, repo_row)
            getcommitinfo(repoid,NEWREPO_xl,rowner,rname)
            if rcount == 1:
                if DF_COUNT <MAX_ROWS_PERWRITE :
                    df = pd.read_excel(NEWREPO_xl,error_bad_lines=False,header= 0, index = False)
                    df= df.append(DF_REPO, ignore_index = True)
                    df.to_excel(NEWREPO_xl, index = False) 
                rcount = 1
                sheetno = sheetno + 1
                NEWREPO_xl = 'C:\\Data\092019 CommitInfo\RepoCommit_TEST'+str(sheetno)+'.xlsx'
            rcount = rcount + 1
    if DF_COUNT < MAX_ROWS_PERWRITE:
        df = pd.read_excel(NEWREPO_xl,error_bad_lines=False,header= 0, index = False)
        df= df.append(DF_REPO, ignore_index = True)
        df.to_excel(NEWREPO_xl, index = False) 
  
if __name__ == '__main__':
  main()
  