# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 14:57:43 2017
Change a) line 5 b) lines 72-77 c) lines 110 & 111
Updated 100718 ** Note: Microsoft bought GitHub on 040618**
@author: USEREN
"""

import csv
import requests
from time import sleep


PW_CSV = 'C:/Users/Student/Dropbox/HEC/Python/PW/PW_GitHub.csv'
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

def getrepoinfo(REPO_CSV,NEWREPO_CSV) :
    """Update the repo inforation with,  PUSHED,STARS, SUBSCRIBERS,  FORKS, SIZE, LICENCE """
    with open(NEWREPO_CSV, 'wt', encoding = 'utf-8', newline='') as writelist:
        write_handle = csv.writer(writelist)

        with open(REPO_CSV, 'rt', encoding = 'Latin-1') as repolist:
            repo_handle = csv.reader(repolist)
            
            for row in repo_handle:
                repo_id = row[1]
                repoid_url = "https://api.github.com/repositories/"+repo_id
                repoid_req = getGitHubapi(repoid_url)
                if repo_id == "REPO_ID":
                    row.append("PUSHED_0718")
                    row.append("STARS_0718")
                    row.append("SUBSCRIBERS_0718")
                    row.append("FORKS_0718")
                    row.append("SIZE_0718")
                    row.append("LICENCE_0718")
                    write_handle.writerow(row)                    
                elif repoid_req == 0 or repoid_req == None:
                    print("************************* Error Searching for repo name for repo - "+ repo_id)
                    row.append("")
                    row.append("")
                    row.append("")
                    row.append("")
                    row.append("")
                    row.append("")
                    write_handle.writerow(row)
                else:
                    repoid_json = repoid_req .json()
                    row.append(repoid_json['pushed_at'])
                    row.append(repoid_json['stargazers_count'])
                    row.append(repoid_json['subscribers_count'])
                    row.append(repoid_json['forks'])
                    row.append(repoid_json['size'])
                    lic_url = repoid_json['url']+"/license"
                    lic_req = getGitHubapi(lic_url)
                    if lic_req == 0 or lic_req == None:
                        row.append("Not Found")
                    else:
                        lic_json = lic_req .json()
                        row.append(lic_json['license']['name'])
                    write_handle.writerow(row)
                        
        

def main():
     
    # For WINDOWS
    
    REPO_CSV = 'C:/Users/Student/Dropbox/HEC/Data GitHub/2014/Integrated Super 2017 RR/PDIntegratedUpCommitSuper2014_24_Surv20180605.csv'
    NEWREPO_CSV = 'C:/Users/Student/Dropbox/HEC/Data GitHub/2014/Integrated Super 2017 RR/PDIntegratedUpCommitSuper2014_24_Surv20180710.csv'
    getrepoinfo(REPO_CSV,NEWREPO_CSV) 
     
if __name__ == '__main__':
  main()
  

