# in a new environment we have to do a pip install httplib2, pip install uritemplate, pip install oauth2client

import httplib2 
import pprint
import csv

from datetime import datetime
#I do not know what the problem is but i am not able to pip install googleapiclient. I had to do $sudo pip install --upgrade google-api-python-client
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client.tools import argparser

# Enter your Google Developer Project number
PROJECT_NUMBER = '1001638736535'
PROJECT_ID = 'avid-stratum-167209'

# For a user you need to create and download the client_secrets.jason file. Go to your Google API consol -> click on APIs and Auth -> Click on credentials -> Click on OAUth2.0 Client ID  -> Click Other
FLOW = flow_from_clientsecrets('C:/Users/USEREN/Dropbox/HEC/Python/GoogleAPIKeys/poonacha.k.medappa_cs.json',
                               scope='https://www.googleapis.com/auth/bigquery')                             
NULL = 0
#REPOLIST_CSV = '/Users/medapa/Dropbox/HEC/Data GitHub/2015/Run 19000/EXPORT0317_FULLDATA_2015Up_CLEAN_19000.csv'
#EVENTLIST_CSV = '/Users/medapa/Dropbox/HEC/Data GitHub/2015/Run 19000/EVENTLIST2015_19000.csv'
REPOLIST_CSV = 'C:/Users/USEREN/Dropbox/HEC/Data GitHub/2015/Run 30692/EXPORT0317_FULLDATA_2015Up_CLEAN_30692.csv'
EVENTLIST_CSV = 'D:/Mes Documents Sauvegardés/Poo/DataBackup2015/Run 30692/EVENTLIST2015_30692.csv'



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
      print ("credentials invalid")

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
    print ('Error in query:', pprint.pprint(err.content))
    return NULL
    
  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")
    return NULL


        
def main():
    
    job_no = 1  
# Read the repo list
    with open(EVENTLIST_CSV, 'at', encoding = 'utf-8', newline='') as csv_append:
        event_append = csv.writer(csv_append)
        with open(REPOLIST_CSV, 'rt', encoding = 'utf-8') as repolist_read:
            repolist_data = csv.reader(repolist_read) 
  
            for row in repolist_data:        
                repo_id = row[1]
                print (repo_id)
# Let the repodetails be the first line of the CSV       

                event_append.writerow(row) 
          
# the query to be executed on BigQuery database 
# Download events are no longer created and hence not captured  
# I have set a limit of 22000 for a query result since an exception had crept up when the results exceeded this amount
#          query = "SELECT  actor, type, repository_pushed_at, created_at, payload_pull_request_id, payload_pull_request_created_at, payload_pull_request_merged_at, payload_pull_request_merged, payload_action, payload_commit_flag, payload_commit_email,payload_pull_request_mergeable, payload_pull_request_additions, payload_pull_request_changed_files, payload_pull_request_commits FROM [githubarchive:year.2015] WHERE repository_owner = '"+repo_owner+"' AND repository_name = '"+repo_name+"' AND (type = 'PushEvent' OR type = 'PullRequestEvent' OR type = 'PullEvent' OR type = 'CreateEvent') ORDER BY repository_pushed_at, created_at LIMIT 22000"
#          print query
                query = "SELECT  type, id, created_at, actor.login,  actor.id, actor.url,org.login, org.id, org.url, payload FROM [githubarchive:year.2015] WHERE repo.id = "+repo_id+" AND (type = 'PushEvent' OR type = 'PullRequestEvent' OR type = 'PullEvent' OR type = 'CreateEvent' OR type = 'ForkEvent' OR type = 'GhollumEvent') ORDER BY created_at LIMIT 22000"
                print(query)
                try: 
                    query_response = query_gitarchive(query) 
                    print ("Job Complete :", query_response['jobComplete'], "Job number", job_no)
                    job_no = job_no +1
                except: 
                    print("Error running the query ***", query_response )
                    continue
# In case there is an error in query or credentials, appropriate message is displayed the function returns a NULL
                if query_response['jobComplete'] != True: 
                    print ("First try failed***************************************************************")
                    tries = 0
                    while tries < 3: 
                        query_response = query_gitarchive(query)              
                        tries = tries + 1  
                        if query_response['jobComplete'] == True : break
                        elif tries == 3 :
                            print ("Returning from main(). Check if the query syntax is correct and if the credentials sent to BigQuery is accurate")
                            return 
                        else:
                            print ("**************************************************Re-try query number", tries)
                    
# If query i successful print the number of rows             
                total_rows = query_response['totalRows']   
                print ("Total rows from query =" + total_rows + ".")
                if int(total_rows) >= 22000 : print ("************************************************ Query Result Size Limit Reached")


# On successful query, the number of rows from the query is printed and the CSV is appended with the data    
                if int(total_rows) > 0 : 
                    for query_row in query_response['rows']:
                
                        event_data = ['']
                        event_data.append(query_row['f'][0]['v'])
                        event_data.append(query_row['f'][1]['v'])
                        if query_row['f'][2]['v'] == '' or query_row['f'][2]['v'] == None : event_data.append(' ')
                        else: event_data.append(datetime.fromtimestamp(float(query_row['f'][2]['v'])))
                        event_data.append(query_row['f'][3]['v'])
                        event_data.append(query_row['f'][4]['v'])
                        event_data.append(query_row['f'][5]['v'])
                        event_data.append(query_row['f'][6]['v'])
                        event_data.append(query_row['f'][7]['v'])
                        event_data.append(query_row['f'][8]['v'])
                        event_data.append(query_row['f'][9]['v'])
                        event_append.writerow(event_data)
                else: 
                    print ("************************************************ Query returned zero records. ")

                    
# store the 10 fields of event data that is being retrieved from bigquery
"""              

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
"""                                                 
if __name__ == '__main__':
  main()