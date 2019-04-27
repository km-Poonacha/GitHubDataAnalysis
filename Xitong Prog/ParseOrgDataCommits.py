# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 18:42:31 2019

@author: kmpoo
"""

import csv
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

def main():
     
    # For WINDOWS 

    REPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\IBMRepoCommit_1.csv'
#    NEWREPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 6 - MS Acquire Github Allies and Competitors\Data\IBMRepo_20190425_2.csv'
    
    with open(REPO_CSV, 'rt', encoding = 'utf-8') as repolist:
        repo_handle = csv.reader(repolist)
        for row in repo_handle:
            if row[1]:
                parse_datetime = datetime.strptime(row[6], '%Y-%m-%dT%H:%M:%SZ') 
                print((parse_datetime.year - 2000)*12 + parse_datetime.month )
#    getrepoinfo(NEWREPO_CSV) 
  
if __name__ == '__main__':
  main()