from __future__ import print_function

from facepy import GraphAPI
import decimal
import json
import pprint
import time

SAVE_DIR = "BigDataMyData"

# http://stackoverflow.com/questions/16957275/python-to-json-serialization-fails-on-decimal
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

# the easiest way to obtain an oauth token is to do it by hand
# in https://developers.facebook.com/tools/explorer, then save
# the token into the file specified here:
oauth_access_token_file = 'oauth_file'
with open(oauth_access_token_file, 'r') as oauth_fd:
	oauth_access_token = oauth_fd.readlines()[0]

graph = GraphAPI(oauth_access_token)

start_time_ms = int(time.time() * 1000)

seq = 1
data = graph.get('497068793653308/feed', page=True)
for page in data:
	page_file_name = "{}/bigdatamy_feed_{}_{}.json".format(SAVE_DIR, start_time_ms, seq)
	try:
		with open(page_file_name, "w") as page_save_fd:
			json.dump(page, page_save_fd, default=decimal_default)
		nominal_date = page['data'][0]['created_time']
		print("Saved page {} (containing data from around {}) to {}".format(seq, nominal_date, page_file_name))
	except Exception:
		pprint.pprint(page)
		raise
	seq += 1

