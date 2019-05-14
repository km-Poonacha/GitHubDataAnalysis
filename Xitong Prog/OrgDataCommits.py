# -*- coding: utf-8 -*-
"""
Created on Fri Mar 22 10:37:38 2019

@author: kmpoo
"""

import csv
import sys
if "C:\\Users\\kmpoo\\Dropbox\\HEC\\Python\\CustomLib\\PooLIB" not in sys.path:
    sys.path.append('C:\\Users\kmpoo\Dropbox\HEC\Python\CustomLib\PooLIB')
    print(sys.path)
from poo_ghmodules import getGitHubapi
from poo_ghmodules import ghpaginate
from poo_ghmodules import ghparse_row

PW_CSV = 'C:/Users/kmpoo/Dropbox/HEC/Python/PW/PW_GitHub.csv'
LOG_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\MozillaLog_OrgRepo_20190514.csv'

def getcommitinfo(repoid,write_handle):
    commit_url = "https://api.github.com/repositories/"+str(repoid)+"/commits?per_page=100"
    while commit_url:
        commit_req = getGitHubapi(commit_url,PW_CSV,LOG_CSV)
        if commit_req:
            commit_json = commit_req.json()
            for commit in commit_json:
                commit_row = ghparse_row(commit,"sha", "commit*author*name","commit*author*email","commit*author*date", "commit*committer*name","commit*committer*email","commit*committer*date","commit*message","commit*comment_count","commit*verification","url","parents", prespace = 1)
                write_handle.writerow(commit_row)  
            commit_url = ghpaginate(commit_req)
        else:
            print("Error getting commit info ",commit_url)
            with open(LOG_CSV, 'at', encoding = 'utf-8', newline ="") as loglist:
                log_handle = csv.writer(loglist)
                log_handle.writerow(["Error getting commit",commit_url,"UNKNOWN"])
            return                    
        

def main():
     
    # For WINDOWS 
    REPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\MozillaRepo_20190514.csv'
    NEWREPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\MozillaRepoCommit_1.csv'
    with open(REPO_CSV, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        rcount = 1
        sheetno = 1
        with open(NEWREPO_CSV, 'at', encoding = 'utf-8', newline='') as writelist:
            write_handle = csv.writer(writelist)
            for repo_row in repo_handle:
                repoid = repo_row[1]     
                write_handle.writerow(repo_row)
                getcommitinfo(repoid,write_handle)
                if rcount == 200:
                    rcount = 1
                    sheetno = sheetno + 1
                    NEWREPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\MozillaRepoCommit_'+str(sheetno)+'.csv'
                    with open(NEWREPO_CSV, 'at', encoding = 'utf-8', newline='') as writelist:
                        write_handle = csv.writer(writelist)
                rcount = rcount + 1
    
#    getrepoinfo(NEWREPO_CSV) 
  
if __name__ == '__main__':
  main()
  