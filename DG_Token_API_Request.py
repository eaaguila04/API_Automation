import requests
import time
import logging
import sys


logging.basicConfig(filename='dataops_api.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

###API to get token to connect to the Datagaps backend database server###
get_token_url = 'https://poc.datagaps.com/dataopssecurity/oauth2/token'
get_token_payload = {'grant_type': 'password', 'username': 'soubhagya.patra@accenture.com', 'password': 'U2FsdGVkX18H16OH+X+By9HTBzjF4rw2JdsFlWxrmds='}
get_token_files = [ ]
get_token_headers = { 'Authorization': 'Basic ZGF0YW9wc3N1aXRlLWFwcGxpY2F0aW9uLWNsaWVudDpzcHJpbmctc2VjdXJpdHktb2F1dGgyLXJlYWQtd3JpdGUtY2xpZW50LXBhc3N3b3JkMTIzNA=='}



def get_token(get_token_url, get_token_payload, get_token_files, get_token_headers):
              response = requests.post(url = get_token_url, headers = get_token_headers, data = get_token_payload, files = get_token_files, verify = False)
              logging.debug('%s Token API Response', response)
              if response.status_code != 200:
                  logging.debug('Unable to obtain token. Exiting with error')
                  sys.exit(0)
              return response.json()
              
token_func_result = get_token(get_token_url, get_token_payload, get_token_files, get_token_headers)
token = token_func_result['access_token']
logging.debug(token)


###API to run a dataflow and return the dataflow RunID###
runid_url = 'https://poc.datagaps.com/DataFlowService/api/v1.0/dataFlows/executeDataFlow?dataflowId=046b4da1-c00e-425b-af9e-074bea575229&livyid=273'
runid_headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    'Authorization': 'Bearer ' + token}


def getRunid(runid_url, runid_headers):
    response = requests.post(url = runid_url, headers=runid_headers, verify=False)
    logging.debug('%s Get Run ID API Response', response)
    if response.status_code != 200:
        logging.debug('Unable to obtain DataFlow RunID. Exiting with error')
        sys.exit(0)
    return response.json()
getid = getRunid(runid_url, runid_headers)
dataFlowRunId = getid['dataFlowRunId']
logging.debug('%s Dataflow RunID', dataFlowRunId)




###API to get a dataflow run status###
status_url = 'https://poc.datagaps.com/DataFlowService/api/v1.0/dataFlows/dataflow-status?'
status_params = {'dataFlowRunId':dataFlowRunId}
status_headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    'Authorization': 'Bearer ' + token}

def getStatus(status_url, status_headers):
    response = requests.get(url=status_url, params=status_params, headers=status_headers, verify=False)
    logging.debug('%s Get Status API Response',response)
    if response.status_code != 200:
        logging.debug('Unable to obtain Dataflow Status. Exiting with error')
        sys.exit(0)
    return response.json()
getstatus = getStatus(status_url, status_headers)
df_status = getstatus['status']

logging.debug('getting Dataflow status. Please be patient!')
while df_status == 'Started' or df_status == 'Running':
    time.sleep(30)
    pre_status = getStatus(status_url, status_headers)
    df_status = pre_status['status']
else:
    if df_status == 'Error':
        logging.debug('One or more dataflow component did not complete successfully. Please review the logs to see wich component is in error')
        logging.debug('%s Dataflow API Response', pre_status)
        logging.debug('%s Dataflow Status', df_status)
    else:
        logging.debug('%s Dataflow API Response', pre_status)
        logging.debug('%s Dataflow Status', df_status)