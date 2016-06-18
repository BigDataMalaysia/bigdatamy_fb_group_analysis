"""
Process all the pages pulled down by pull_feed.py and flatten the
relevant bits into a list.

Prints json output. Redirect to a file to save.
"""
from __future__ import print_function
from __future__ import division

import os
import json

SAVE_DIR = "BigDataMyData"

def read_the_feed(save_dir):
	"""
	Read all the JSON files in save_dir and return a list with all the "interesting"
	data items (paging elements will be excluded).
	"""

	expected_keys = set(['data', 'paging'])
	required_keys = set(['data'])

	all_the_files = ["{}/{}".format(save_dir, x) for x in os.listdir(save_dir) if '.json' in x]

	big_data = []
	for file_path in all_the_files:
		with open(file_path, "r") as read_fd:
			page = json.load(read_fd)

		# Check that the feed is in the expected format
		found_keys = set(page.keys())
		if required_keys.issubset(found_keys) and not found_keys.difference(expected_keys):
			big_data += page['data']
		else:
			raise Exception("File {} contains unexpected key set {}".format(file_path, found_keys))
	return big_data

big_data = read_the_feed(SAVE_DIR)
print(json.dumps(big_data))


