# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 17:04:10 2019

@author: pmedappa
"""
import requests

def run_query(query): # A simple function to use requests.post to make the API call. Note the json= section.
    try:
        request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        return request.json()
    except:
        return '404'


query = """
            query{
              rateLimit {
                cost
                remaining
                resetAt
              }
              repository(name: "sickvim", owner: "jonathansick") {
                ref(qualifiedName: "master") {
                  target {
                    ... on Commit {
                      history(first:1 until: "2016-01-01T00:00:00Z"){
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

headers = {"Authorization": "Bearer "+"dfb9844388015057b2bb8331c562068b04d9807f"}                        
result = run_query(query)

print(result)