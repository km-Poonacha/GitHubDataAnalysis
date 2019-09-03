#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 16:54:21 2017

Search for all the PRs for a repository that have been created before 2015. Find the commit information for each PR and store in CSV file. 

"""


import csv
import requests
import json
from time import sleep


PW_CSV = 'C:/Users/USEREN/Dropbox/HEC/Python/PW/PW_GitHub.csv'
TRIP = 0


def getGitHubapi(url):
    """This function uses the requests.get function to make a GET request to the GitHub api
    TRIP flag is used to toggle GitHub accounts. The max rate for searches is 30 per hr per account"""
    global TRIP
    """ Get PW info """
    PW_list = []
    
    with open(PW_CSV, 'rt', encoding = 'utf-8') as PWlist:
        PW_handle = csv.reader(PWlist)
        del PW_list[:]
        for pw in PW_handle:
            PW_list.append(pw)
    if TRIP == 0:
        repo_req = requests.get(url, auth=(PW_list[0][0], PW_list[0][1]))
        print(repo_req.status_code)
        TRIP = 1
    elif TRIP == 1:
        repo_req = requests.get(url, auth=(PW_list[1][0], PW_list[1][1]))
        print(repo_req.status_code)
        TRIP = 2
    else:
        repo_req = requests.get(url, auth=(PW_list[2][0], PW_list[2][1]))
        print(repo_req.status_code)
        TRIP = 0
        
    if repo_req.status_code == 200: 
        print(repo_req.headers['X-RateLimit-Remaining'])
        if int(repo_req.headers['X-RateLimit-Remaining']) <= 3:
            """  Re-try if Github limit is reached """
            print("************************************************** GitHub limit close t obeing reached.. Waiting for 10 mins" )
            """ Provide a 10 mins delay if the limit is close to being reached  """
            sleep(600)
            
        """ Return the requested data  """
        return repo_req
    else:
        print("Error accessing url = ",url)
        repo_json = repo_req.json()
        print("Error code = ",repo_req.status_code,". Error message = ",repo_json['message'])
        return 0   

def getPRinfo(pr_contents,repo_name,NEWPULL_CSV):
    """ This function gets the pull request information for a repository And calls the function to get commit level information """
    pr_row = []
    with open(NEWPULL_CSV, 'at', encoding = 'utf-8', newline='') as writelist:
        write_handle = csv.writer(writelist)

        pr_json = pr_contents.json()
        print(pr_json['total_count'])
        """ For each PR, save the PR info before colelcting the commit information"""
        del pr_row[:]        
        for items in pr_json['items']:
            pr_row.append("PullRequestEvent")
            pr_row.append(items['number'])
            pr_row.append(items['user']['login'])
            pr_row.append(items['state'])
            pr_row.append(items['created_at'])
            pr_row.append(items['updated_at'])
            pr_row.append(items['closed_at'])
            pr_row.append(items['title'])
            pr_row.append(items['body'])
            """Find additional PR deails for each PR """
            pr_url = "https://api.github.com/repos/"+repo_name+"/pulls/"+str(items['number']) 
            pr_contents = getGitHubapi(pr_url)
            pr_json = pr_contents.json()            
            pr_row.append(pr_json['id'])
            pr_row.append(pr_json['commits'])
            pr_row.append(pr_json['additions'])
            pr_row.append(pr_json['deletions'])
            pr_row.append(pr_json['changed_files'])
            pr_row.append(pr_json['merged'])
            pr_row.append(pr_json['merged_at'])
            if pr_json['merged_by'] is not None:
                pr_row.append(pr_json['merged_by']['login'])
                pr_row.append(pr_json['merged_by']['site_admin'])
            """ Write the PR details into the destination CSV """
           
            write_handle.writerow(pr_row)  
            del pr_row[:]
             
            """Find each PR commiter involved and get their commit information """
            
            commit_url = "https://api.github.com/repos/"+repo_name+"/pulls/"+str(items['number'])+"/commits"
            getCommitterinfo(commit_url,write_handle)
            
            
def getCommitterinfo(commit_url,write_handle):     
    """ This function finds the commiter information for the PR and updates it in CSV. Takes care of pagination """
    rel = 'next' 

    while rel == "next":
        print(rel)
        print(commit_url)
        commit_info = getGitHubapi(commit_url)
        
        """ In case the search returns no results """
        if commit_info== 0 or commit_info == None:
            print("************************* Error Finding Commits for Pull request on URL - "+commit_url)
            return 1
            
        commit_json = commit_info.json()
        commit_row = []
        for commits in commit_json:
            commit_row.append("")
            commit_row.append(commits['sha'])
            commit_row.append(commits['commit']['author']['name'])
            commit_row.append(commits['commit']['author']['email'])
            commit_row.append(commits['commit']['author']['date'])
            commit_row.append(commits['commit']['committer']['name'])
            commit_row.append(commits['commit']['committer']['email'])
            commit_row.append(commits['commit']['committer']['date'])
            commit_row.append(commits['commit']['message'])
            write_handle.writerow(commit_row)
            del commit_row[:]
            
        """ While there exists a next page, do this """        
        rel = ""
        link = commit_info.headers.get('link',None)    

        if link is not None:
            rel_temp = link.split("rel")[1]
            rel = rel_temp[2:6]
            next_url = link.partition('>;')[0]
            commit_url = next_url[1:]
*************    CHECK PAGINATION THERE ARE ISSUES         **********************                
    return 0
    
    
def getCommitInfoMain(REPO_CSV,NEWPULL_CSV):
    """This is the main function that find the repo required and gets the PR and corresponding commit information for each repo"""
    with open(REPO_CSV, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)

        for repo_row in repo_handle:
            page_no = 1
            
            if repo_row[0] != "REPO_ID" and repo_row[0] != "": 
                repo_id = repo_row[0]
                page_no = 1 
                repoid_url = "https://api.github.com/repositories/"+repo_id
                repoid_req = getGitHubapi(repoid_url)
                if repoid_req == 0 or repoid_req == None:
                    print("************************* Error Searching for repo name for repo - "+ repo_id)
                    continue
                repoid_json = repoid_req .json()
                repo_name = repoid_json['full_name']
                repo_url = "https://api.github.com/search/issues?q=repo:"+repo_name+"+type:pr+created:<2015-01-01&page="+str(page_no)
#For 2015 data -                repo_url = "https://api.github.com/search/issues?q=repo:"+repo_name+"+type:pr+created:2015-01-01..2016-01-01&page="+str(page_no)

                print(repo_url)
                with open(NEWPULL_CSV, 'at', encoding = 'utf-8', newline='') as writelist:
                    write_handle = csv.writer(writelist)
                    write_handle.writerow(repo_row)

                repo_req = getGitHubapi(repo_url)
                """ In case the search returns no results """
                if repo_req == 0 or repo_req == None:
                    print("************************* Error Searching for Pull Requests for repo - "+ repo_name + " Page No - " + str(page_no))
                    continue
                
                getPRinfo(repo_req,repo_name,NEWPULL_CSV)
                
                """ The section below checks the received header to traverse to the next pages. """
                
                link = repo_req.headers.get('link',None)
                print(link)
                rel = ""
                if link is not None:
                    rel_temp = link.split("rel")[1]
                    rel = rel_temp[2:6]
                
                while rel == "next": 
                    """ While there exists a next page, do this """
                    page_no = page_no + 1
                    repo_url = link.partition('>;')[0]
                    link_next = repo_url[1:]
                    print(link_next)
                    repo_req = getGitHubapi(link_next)
                    """ In case the search returns no results """
                    if repo_req == 0 or repo_req == None:
                        print("************************* Error Searching for Pull Requests for repo - "+ repo_name + " Page No - " + str(page_no))
                        continue                
                    getPRinfo(repo_req,repo_name,NEWPULL_CSV)
                
                    link = repo_req.headers.get('link',None)
                    rel = ""
                    if link is not None:
                        rel_temp = link.split("rel")[1]
                        rel = rel_temp[2:6]
                        print(rel)

def main():
     
    REPO_CSV = '/Users/medapa/Dropbox/HEC/Data GitHub/2014/Run 9-2/RepoList2014_New.csv'
    NEWPULL_CSV = '/Users/medapa/Dropbox/HEC/Data GitHub/2014/Run 9-2/UpdateCommit/CommitPullRequestList2014.csv'
    getCommitInfoMain(REPO_CSV,NEWPULL_CSV) 

    """
    # For WINDOWS
    
    REPO_CSV = 'C:/Users/USEREN/Dropbox/HEC/Data GitHub/2014/Run 1000/RepoList2014_New.csv'
    NEWPULL_CSV = 'C:/Users/USEREN/Dropbox/HEC/Data GitHub/2014/Run 1000/UpdateCommit/CommitPullRequestList2014.csv'
    getCommitInfoMain(REPO_CSV,NEWPULL_CSV) 
    
    """
     
if __name__ == '__main__':
  main()
  
