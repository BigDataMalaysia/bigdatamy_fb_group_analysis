#!/usr/bin/env python
import argparse
import logging
import time

import fbgroup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Download Facebook Group data")
    parser.add_argument("--last-n-pages", action="store", type=int, default=None,
                        help="Only fetch the last N most recent pages worth of data.")
    parser.add_argument("--group-id", action="store", type=str, default="497068793653308",
                        help="Group ID (default is the group id of https://www.facebook.com/groups/bigdatamy/.)")
    parser.add_argument("--out-file", action="store", type=str, default=None,
                        help="File to save to; default: fbg_<group-id>_<epoch>.dat")
    parser.add_argument("--oauth-token-file", action="store", type=str, default="oauth_file",
                        help="Path to file containing oauth token")
    args = parser.parse_args()

    if args.out_file:
        output_file_path = args.out_file
    else:
        output_file_path = "fbg_{}_{}.dat".format(args.group_id, int(time.time()))

    fbg = fbgroup.Group(args.group_id)
    logging.info("Fetching info for Facebook Group ID %s", args.group_id)
    with open(args.oauth_token_file, 'r') as oauth_fd:
        oauth_access_token = oauth_fd.readlines()[0]
    fbg.fetch(oauth_access_token,
              args.last_n_pages)
    logging.info("Saving to %s", output_file_path)
    fbg.pickle_posts(output_file_path)
    return

if __name__ == "__main__":
    main()
