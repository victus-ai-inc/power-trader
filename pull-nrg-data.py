# Python 3.7
token = "valid token"
server = 'api.nrgstream.com'
# NOTE: streamId '1' is in the NRG Stream 'Alberta Power' Product ... so the token must be for a user that has access to Alberta Power product,
# if not the call will result in a NotFound
# ...in otherwords, only data for the streams the user has access to can be returned
path = '/api/StreamData/1?fromDate=09/21/2018&toDate=09/21/2018'

# Get StreamdData in CVS format
headers = {
    'Accept': 'text/csv',
    'Authorization': f'Bearer {token}'
}

import http.client
import certifi
import ssl
context = ssl.create_default_context(cafile=certifi.where())
conn = http.client.HTTPSConnection(server,context=context)
conn.request('GET', path, None, headers)
res = conn.getresponse()

print('========= Get StreamdData in CVS format')
print(res.code)
print(res.headers)
print(res.read().decode('utf-8'))

# Get StreamdData in JSON format
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {token}'
}

import json
context = ssl.create_default_context(cafile=certifi.where())
conn = http.client.HTTPSConnection(server,context=context)
conn.request('GET', path, None, headers)
res = conn.getresponse()

pretty_json = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)

print('========= Get StreamdData in JSON format')
print(res.code)
print(res.headers)
print(pretty_json)

# Release to token for another user / process
path = '/api/ReleaseToken'
headers = {
'Authorization': f'Bearer {token}'
}
context = ssl.create_default_context(cafile=certifi.where())
conn = http.client.HTTPSConnection(server,context=context)
conn.request('DELETE', path, None, headers)
res = conn.getresponse()