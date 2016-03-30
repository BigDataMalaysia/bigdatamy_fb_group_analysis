from __future__ import print_function

import decimal
import json
import pdb
import pprint
import time

from facepy import GraphAPI

SAVE_DIR = "BigDataMyData"
GROUP_ID = "497068793653308"

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
data = graph.get('{}/feed'.format(GROUP_ID), page=True)
posts = []
for page in data:
    print("new page")
    if 'data' in page:
        print("page has {} posts".format(len(page['data'])))
        posts += [p for p in page['data']]
        most_recent_post = posts[-1]
        most_recent_post_timestamp = most_recent_post['created_time']
        print("current accumulated posts count: {}, oldest timestamp: {}".format(len(posts), most_recent_post_timestamp))

for post in posts:
    post_link = [x['link'] for x in post['actions'] if x['name'] == 'Comment'][0]
    if 'comments' in post:
        post_comments = post['comments']
        if 'next' in post_comments['paging']:
            print("Post {} needs paging for comments".format(post_link))
    if 'likes' in post:
        post_likes = post['likes']
        if 'next' in post_likes['paging']:
            print("Post {} needs paging for likes".format(post_link))

pdb.set_trace()
#for page in data:
#	if len(page['data']) > 0:
#
#		# page in long comment threads if needed
#		for post_index in xrange(len(page['data'])):
#			post = page['data'][post_index]
#			page_in_id = None
#			try:
#				if 'paging' in post['comments']:
#					page_in_id = post['id']
#			except KeyError:
#				pass
#			if page_in_id:
#				print("Paging in {}".format(page_in_id))
#				all_comments = list(graph.get('{}/comments'.format(page_in_id), page=True))
#				page['data'][post_index]['comments'] = all_comments
#
#		page_file_name = "{}/bigdatamy_feed_pages_{}_{}.json".format(SAVE_DIR, start_time_ms, seq)
#		try:
#			with open(page_file_name, "w") as page_save_fd:
#				json.dump(page, page_save_fd, default=decimal_default)
#			nominal_date = page['data'][0]['created_time']
#			print("Saved page {} (containing data from around {}) to {}".format(seq, nominal_date, page_file_name))
#		except Exception:
#			print("###############################")
#			pprint.pprint(page)
#			raise
#		seq += 1
#
