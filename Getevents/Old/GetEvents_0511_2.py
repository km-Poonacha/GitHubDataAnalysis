# in a new environment we have to do a pip install httplib2, pip install uritemplate, pip install oauth2client

import httplib2 
import pprint
import sys
import json
import csv
import requests
import urllib2
import urlparse
import time

from datetime import date
from datetime import datetime
#from googleapiclient.discovery import build
#from googleapiclient.errors import HttpError

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client.tools import argparser
from dateutil.parser import parse

# Enter your Google Developer Project number
PROJECT_NUMBER = <<Enter project no>>
PROJECT_ID = <<Enter project no>>

# For a user you need to create and download the client_secrets.jason file. Go to your Google API consol -> click on APIs and Auth -> Click on credentials -> Click on OAUth2.0 Client ID  -> Click Other
FLOW = flow_from_clientsecrets(<<client secret path>>,
                               scope='https://www.googleapis.com/auth/bigquery')                             
NULL = 0
REPOLIST_CSV = '/Users/medapa/Dropbox/HEC/Summer Project/Data/RepoList2014.csv'
EVENTLIST_CSV = '/Users/medapa/Dropbox/HEC/Summer Project/Data/EventList2014.csv'


def query_gitarchive(query_command):
# Define a storage object which can be accessed by multiple processes or threads.

#For a new project, or if there are big permission issues. Delete the storage object and try again. I am not sure if the IF statement is working too well 
  storage = Storage('bigquery_credentials.dat')
  credentials = storage.get()
  flags = argparser.parse_args(args=[])
# flags is a bunch of parameters necessary for run_flow(). The contents of flags are : 
# Namespace(auth_host_name='localhost', auth_host_port=[8080, 8090], logging_level='ERROR', noauth_local_webserver=False)
 
  if credentials is None or credentials.invalid:

#The run() function is called from your application and runs through all the steps to obtain credentials.
#It takes a Flow argument and attempts to open an authorization server page in the user's default web browser. The server asksthe user to grant your application access to the user's data.      
#Args: flow: Flow, an OAuth 2.0 Flow to step through. storage: Storage, a Storage to store the credential in.flags: argparse.ArgumentParser, the command-line flags.
      
      credentials = run_flow(FLOW, storage,flags) #Storage is the object where the credentials retruned will be stored 
      print "credentials invalid"

# class httplib2.Http([cache=None][, timeout=None]) The class that represents a client HTTP interface. The cache parameter is either the name of a directory to be used as a flat file cache, or it must an object that implements the required caching interface. The timeout parameter is the socket level timeout. 
  http = httplib2.Http()
  http = credentials.authorize(http)
#Use the authorize() function of the Credentials class to apply necessary credential headers to all requests made by an httplib2.Http instance:
#Before you can make requests, you first need to initialize an instance of the Google Compute Engine service using the API client library's build method: 
  
  bigquery_service = build('bigquery', 'v2', http=http)

  try:
    query_request = bigquery_service.jobs()
    query_data = {
    'query': (query_command)
    }

    query_response = query_request.query(projectId= PROJECT_ID,body=query_data).execute()
    return query_response


  except HttpError as err:
    print 'Error in query:', pprint.pprint(err.content)
    return NULL
    
  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")
    return NULL

def get_repo():

# the search url for running the GitHub search and finding the relevant repos 
# Note only 1000 results are displayed for each search, with 30 repos in one page 
    page_no = 1
    search_url = 'https://api.github.com/search/repositories?q=created:2014-02-11..2014-03-20%20stars:4%20fork:false%20forks:%3E1%20&sort=stars&order=asc&page='+str(page_no)
    print search_url
    r = requests.get(search_url, auth=('randomeshwar', 'rand276eshwar'))
    print r.status_code
    print r.headers['X-RateLimit-Remaining']
    description = str
        
    if r.status_code == 200 :
        json_values = r.json()
        search_count = json_values['total_count']
        print 'Total search result count =', search_count
#Limit the search results to 1000        
        if search_count > 1000: 
            print "********************************************************** Search count greater than 1000"
            search_count = 1000
        i=0
        j=0

#Capture the important fields of the repositories in CSV format
#Note only 30 results are shown per page
        with open(REPOLIST_CSV, 'wb') as git_repo:
            while j+i<search_count :
                items_values = json_values['items'][i]
                repo_licence = 'LicenceName'
                subscribers =0
                language = []
                language_no = 0
                size = 0
                license_url = items_values['url']+'/license'
                repo_url = items_values['url']
                language_url = items_values['languages_url']
                
                parsed_url = urlparse.urlparse(license_url) # check if the url schema is correct
                
                if bool(parsed_url.scheme) == 0 : 
                    repo_licence = 'URL_Error'
                   
                else:
                    r = requests.get(license_url, auth=('km-poonacha', 'rand276eshwar'))

                    print r.headers['X-RateLimit-Remaining']

                    if r.status_code == 200 :
                        values = r.json()
                        repo_licence = str(values['license']['name'])
                    elif r.status_code == 404 : 
                        repo_licence = str("Not Found")
                    else: 
                        repo_licence = str("Unknown Error")
#GEt subscribers info                         
                parsed_url = urlparse.urlparse(repo_url) # check if the url schema is correct
                
                if bool(parsed_url.scheme) == 0 : 
                    print 'Repo URL_Error'
                   
                else:
                    r = requests.get(repo_url, auth=('km-poonacha', 'rand276eshwar'))

                    print r.headers['X-RateLimit-Remaining']

                    if r.status_code == 200 :
                        values = r.json()
                        subscribers = values['subscribers_count']
                    elif r.status_code == 404 : 
                        print "Subscribers Not Found"
                    else: 
                        print "Unknown Error while finding subscribers"
                
                
                
#GEt language and size information
                parsed_url = urlparse.urlparse(language_url) # check if the url schema is correct
                
                if bool(parsed_url.scheme) == 0 : 
                    print 'Language URL_Error'
                   
                else:
                    r = requests.get(language_url, auth=('km-poonacha', 'rand276eshwar'))

                    print r.headers['X-RateLimit-Remaining']

                    if r.status_code == 200 :
                        language = r.json()
                        language_no = 0
                        size = 0

                        if len(language) == 0: 
                            language_no = 0
                            size = 0
                        else: 
                            for row in language:
                                language_no = language_no + 1
                                size = size + language[row]
                    elif r.status_code == 404 : 
                        print "Language Not Found"
                    else: 
                        print "Unknown Error while finding subscribers"
  
 #Get the description of the repo. Note: For NULL values of description running encode throws error
                if items_values['description'] is None:
                    description = 'None'                    
                else: 
                    description = items_values['description'].encode('utf8', 'replace')
 #Write fields into the CSV file 
                git_archive_write = csv.writer(git_repo)        
                git_archive_write.writerow([items_values['id'],
                                            items_values['name'],
                                            items_values['owner']['login'],
                                            items_values['owner']['type'],
                                            items_values['size'],
                                            items_values['created_at'],
                                            items_values['pushed_at'],
                                            items_values['language'],
                                            language_no,
                                            size,
                                            items_values['stargazers_count'],
                                            items_values['watchers_count'],
                                            subscribers,
                                            items_values['open_issues'],
                                            items_values['forks_count'],                                           
                                            repo_licence,
                                            items_values['url'],
                                            description,
                                            language ])
                i=i+1
                
 #search results are displayed 30 items per page. The following section of code helps chnage the page                
                if i == 30:
                   page_no = page_no + 1
                   i = 0
                   j = j + 30 
                   search_url = 'https://api.github.com/search/repositories?q=created:2014-02-11..2014-03-20%20stars:4%20fork:false%20forks:%3E1%20&sort=stars&order=asc&page='+str(page_no)
   
                   print 'Search Page no =', page_no
                   print search_url
                   r = requests.get(search_url, auth=('randomeshwar', 'rand276eshwar'))
                   print r.status_code
                   print r.headers['X-RateLimit-Remaining']
                        
                   if r.status_code == 200 :
                        json_values = r.json()                        
                        i=0
                   else : 
                       print 'Error getting search page no = ',page_no
                       return
                   
    else : return NULL

        
def main():
  
# Search GitHUb and get the list of repos. The repos are filled into a CSV (RepoList2014.csv)
  return_code = get_repo()
#  If error in search return from main  
  if return_code == NULL:
      print "Error running the search and getting the repos. Returning from main()"
      return 
  job_no = 1  
# Read the repo list
  with open(REPOLIST_CSV, 'rb') as repolist_read:
    repolist_data = csv.reader(repolist_read) 
  
    for row in repolist_data:        

        time_diff = 0
        repo_name = row[1]
        repo_owner = row[2]
        print repo_name,repo_owner
# Let the repodetails be the first line of the CSV       
        with open(EVENTLIST_CSV, 'ab' ) as csv_append:
          event_append = csv.writer(csv_append)
          event_append.writerow(row) 
          
#Ensure there is some programming language associated with the projects
          if row[7] == '' : continue

# Queries are executed only if a programming language is associated with the repo
# the query to be executed on BigQuery database 
# Download events are no longer created and hence not captured  
# I have set a limit of 22000 for a query result since an exception had crept up when the results exceeded this amount
          query = "SELECT  actor, type, repository_pushed_at, created_at, payload_pull_request_id, payload_pull_request_created_at, payload_pull_request_merged_at, payload_pull_request_merged, payload_action, payload_commit_flag, payload_commit_email,payload_pull_request_mergeable, payload_pull_request_additions, payload_pull_request_changed_files, payload_pull_request_commits FROM [githubarchive:year.2014] WHERE repository_owner = '"+repo_owner+"' AND repository_name = '"+repo_name+"' AND (type = 'PushEvent' OR type = 'PullRequestEvent' OR type = 'PullEvent' OR type = 'CreateEvent') ORDER BY repository_pushed_at, created_at LIMIT 22000"
#          print query
          query_response = query_gitarchive(query) 
          print "Job Complete :", query_response['jobComplete'], "Job number", job_no
          job_no = job_no +1
                       
# In case there is an error in query or credentials appropriate message is displayed the function returns a NULL
          if query_response['jobComplete'] != True: 
            print "First try failed***************************************************************"
            tries = 0
            while tries < 3: 
                query_response = query_gitarchive(query)
               
                tries = tries + 1  
                if query_response['jobComplete'] == True : break
                elif tries == 3 :
                    print "Returning from main(). Check if the query syntax is correct and if the credentials sent to BigQuery is accurate"
                    return 
                else:
                    print "**************************************************Re-try query number", tries
                    
# If query i successful print the number of rows             
          total_rows = query_response['totalRows']   
          print ("Total rows from query =" + total_rows + ".")
          if int(total_rows) == 22000 : print "************************************************ Query Result Size Limit Reached"
          i = 0 
          event_count = 0

# On successful query, the number of rows from the query is printed and the CSV is appended with the data    

          while i < (int(total_rows)) :
            row_data = query_response['rows'][i]
            j = 0
            event_data = ['']
                        
# store the 15 fields of event data that is being retrieved from bigquery

            event_data.append(row_data['f'][0]['v'])
            event_data.append(row_data['f'][1]['v'])
            if row_data['f'][2]['v'] == '' or row_data['f'][2]['v'] == None : repo_pushed_at = ''
            else: repo_pushed_at = datetime.strptime(row_data['f'][2]['v'],'%Y-%m-%d %H:%M:%S')
            event_data.append(repo_pushed_at)
            if row_data['f'][3]['v']== '' or row_data['f'][3]['v']== None: event_created_at = ''
            else: event_created_at = datetime.strptime((row_data['f'][3]['v']),'%Y-%m-%d %H:%M:%S')
            event_data.append(event_created_at)
            event_data.append(row_data['f'][4]['v'])
            if row_data['f'][5]['v'] == '' or row_data['f'][5]['v'] == None:pullreq_created_at = ''
            else: pullreq_created_at = datetime.strptime(row_data['f'][5]['v'],'%Y-%m-%d %H:%M:%S')
            event_data.append(pullreq_created_at)
            if row_data['f'][6]['v'] == '' or row_data['f'][6]['v'] == None: pullreq_merged_at = ''
            else: pullreq_merged_at = datetime.strptime(row_data['f'][6]['v'],'%Y-%m-%d %H:%M:%S')
            event_data.append(pullreq_merged_at)
            event_data.append(row_data['f'][7]['v'])
            event_data.append(row_data['f'][8]['v'])
            event_data.append(row_data['f'][9]['v'])
            
            if row_data['f'][10]['v'] is None:
                commit_url = ''                    
            else: 
                commit_url = row_data['f'][10]['v'].encode('utf8', 'replace')            
            event_data.append(commit_url)
            event_data.append(row_data['f'][11]['v'])
            event_data.append(row_data['f'][12]['v'])
            event_data.append(row_data['f'][13]['v'])
            event_data.append(row_data['f'][14]['v'])
              

############## Task identification ##############
# Event count (A new repo stable version is when atleast 300 mins is passed between the creation of the old version and the new push)
#WHAT VALUE SHOULD WE CHOOSE ???            
            if i > 0:
              
              prev_row_data = query_response['rows'][i-1]
              old_datetime = datetime.strptime(prev_row_data['f'][3]['v'], '%Y-%m-%d %H:%M:%S')
              new_datetime = datetime.strptime(row_data['f'][3]['v'], '%Y-%m-%d %H:%M:%S')
              time_delta = new_datetime - old_datetime
              time_diff = time_delta.total_seconds() / 60
             
              if(time_diff < 60): 
                  event_count = event_count
                    
              else:    
                  event_count = event_count + 1
                    
                
            event_data.append(event_count)
            event_data.append(time_diff/60)
            event_append.writerow(event_data) 
            i = i + 1   
                                                 
                       
# Calculate total number of contributors  
if __name__ == '__main__':
  main()
