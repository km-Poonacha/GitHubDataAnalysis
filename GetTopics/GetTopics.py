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
from poo_ghmodules import ghparse_row
from poo_ghmodules import getGitHubapi

PW_CSV = 'C:/Users/kmpoo/Dropbox/HEC/Python/PW/PW_GitHub.csv'
LOG_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\Log_Gettopics_20190517.csv'


def main():
    """Get topics for each repo"""    
    REPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\ExportedISRDataCollab_LocUpdates 20190425.csv'
    NEWREPO_CSV = 'C:\\Users\kmpoo\Dropbox\HEC\Project 2 -   License\EJIS\Data\FullData_20190517.csv'
    d_header = {
                'Accept': 'application/vnd.github.mercy-preview+json'
             }
    new_row = list()
    with open(NEWREPO_CSV, 'wt', encoding= 'utf-8', newline ='') as writeobj:
        repo_writer = csv.writer(writeobj)
        with open( REPO_CSV, 'rt', encoding= 'utf-8') as fileobj:
            repo_struct = csv.reader(fileobj)
            for row in repo_struct:
                del new_row[:]
                new_row= row
                repo_id = row[0]
                repo_url = 'https://api.github.com/repositories/'+repo_id+'/topics'            
                topic_req = getGitHubapi(repo_url,PW_CSV,LOG_CSV , header = d_header)
                if topic_req:
                    j_topics = topic_req.json() 
                    new_row.append(j_topics['names'])
                    if j_topics['names']:
                        new_row.append(j_topics['names'][0])
                    else:
                        new_row.append("")
                else:
                    new_row.append("")
                    new_row.append("")
                repo_writer.writerow(new_row)
if __name__ == '__main__':
  main()
  