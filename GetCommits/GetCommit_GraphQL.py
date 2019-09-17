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

def run_query(name, owner): 
    """ A simple function to use requests.post to make the API call. Note the json= section."""
    headers = {"Authorization": "Bearer "+"dfb9844388015057b2bb8331c562068b04d9807f"}  
    query = """
                query{
                  rateLimit {
                    cost
                    remaining
                    resetAt
                  }
                  repository(name: """+name+""", owner: """+owner+""") {
                    ref(qualifiedName: "master") {
                      target {
                        ... on Commit {
                          history(first:100 until: "2016-01-01T00:00:00Z"){
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
                                    parents{
                                      totalCount
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
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """
    try:
        request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        return request.json()
    except:
        return '404'

def get_name(repo_id):
    return

def main():
    """Main function"""     
#    name, owner = get_name(repo_id)  
    name = "sickvim"
    owner = "jonathansick"
    result = "0"
    if name:                
        result = run_query(name, owner)
    
    print(result)
    
main()