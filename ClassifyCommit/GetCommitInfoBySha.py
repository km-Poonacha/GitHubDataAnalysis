# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 18:53:53 2020

@author: pmedappa
"""

import csv
import pandas as pd
import sys
if r"C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLIB" not in sys.path:
    sys.path.append(r'C:\Users\pmedappa\Dropbox\Code\CustomLib\PooLIB')
    print(sys.path)
from poo_ghmodules import getGitHubapi
import requests
from poo_ghmodules import ghparse_row
from poo_ghmodules import gettoken

TRAIN_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\LabelData/Java Commit Creativity - Full 2023.csv'
PW_CSV = r'C:\Users\pmedappa\Dropbox\Code\PW\PW_GitHub.csv'
LOG_CSV = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\LabelData/RepoCommit_log.csv'
NEWREPO_xl = r'C:\Users\pmedappa\Dropbox\Data\092019 CommitInfo\JavaSampling\LabelData/Java Commit Creativity - Full 2023_Details.xlsx'

TOKEN = gettoken(r"C:\Users\pmedappa\Dropbox\Code\PW\GHtoken.txt")
headers = {"Authorization": "Bearer "+ TOKEN } 

with open(TRAIN_CSV, 'rt', encoding = 'utf-8' ) as rdobj:
    rd_repo = csv.reader(rdobj,delimiter =';')
    df = pd.DataFrame()
    for row in rd_repo:
        write_row = row
        print(row[0])
        sp_url = row[0].split('/')
        if len(sp_url) > 5:
            commit_url = 'https://api.github.com/repos/'+sp_url[3]+'/'+sp_url[4]+'/commits/'+sp_url[6]
            print(commit_url)
            # commit_req = getGitHubapi(commit_url,PW_CSV,LOG_CSV,header = headers)
            commit_req = requests.get(commit_url, headers = headers)
            if commit_req:
                commit = commit_req.json()

                commit_row = ghparse_row(commit,"sha","commit*author*date", "commit*message","commit*comment_count","commit*author*name","commit*author*email","commit*committer*date","parents","stats","files", prespace = 0)
                # commit_row[-1] = len(commit_row[-1])
                write_row.extend(commit_row)
  
                df = df.append(pd.Series(write_row),sort = False, ignore_index = True)

            else:
                print("Error getting commit info ",commit_url)
                commit_row.append('')
                commit_row.append('')
                commit_row.append('')

                df = df.append(pd.Series(write_row),sort = False, ignore_index = True)

                with open(LOG_CSV, 'at', encoding = 'utf-8', newline ="") as loglist:
                    log_handle = csv.writer(loglist)
                    log_handle.writerow(["Error getting commit",commit_url,"UNKNOWN"])
                continue
df.to_excel(NEWREPO_xl)