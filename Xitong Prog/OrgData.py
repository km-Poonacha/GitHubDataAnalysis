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
org_list = ['apache']

PW_CSV = 'C:/Users/kmpoo/Dropbox/HEC/Python/PW/PW_GitHub.csv'
LOG_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\ApacheLog_OrgRepo_20190514.csv'

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

def getrepoinfo(NEWREPO_CSV):
    """Update the repo inforation with,  PUSHED,STARS, SUBSCRIBERS,  FORKS, SIZE, LICENCE """
    with open(NEWREPO_CSV, 'wt', encoding = 'utf-8', newline='') as writelist:
        write_handle = csv.writer(writelist)
        for org in org_list:
            repo_url = 'https://api.github.com/orgs/'+org+'/repos?per_page=100&page=1'            
            while repo_url:
                repoid_req = getGitHubapi(repo_url,PW_CSV,LOG_CSV)
#                print(repoid_req.headers['link'])
                if repoid_req:
                    repo_json = repoid_req.json()
                    for repo in repo_json:
                        repo_row = ghparse_row(repo,"id", "full_name","description","fork","url","created_at","updated_at","pushed_at","homepage","size","stargazers_count","watchers_count","language","has_issues","has_projects","has_downloads","has_wiki","has_pages","forks_count","mirror_url","archived","disabled","open_issues_count","license*name","forks","open_issues","watchers","default_branch","permissions")
                        write_handle.writerow(repo_row)
                        # get commits
                        #getcommitinfo(repo['id'],write_handle)
                        #end get commits
                    repo_url = ghpaginate(repoid_req)
                else: break
                print(repo_url)
#                    write_handle.writerow(row)
                        
        

def main():
     
    # For WINDOWS    
    NEWREPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\MozillaRepo_20190514.csv'
    getrepoinfo(NEWREPO_CSV) 
  
if __name__ == '__main__':
  main()
  