#!/usr/bin/env python
from __future__ import print_function
import argparse
import logging
import time

from matplotlib import pyplot as plt
from matplotlib_venn import venn3

import fbgroup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Facebook Group membership stuff (WIP)")
    parser.add_argument("--oauth-token-file", action="store", type=str, default="oauth_file",
                        help="Path to file containing oauth token")
    args = parser.parse_args()

    with open(args.oauth_token_file, 'r') as oauth_fd:
        oauth_access_token = oauth_fd.readlines()[0]

    # I use these to find out group IDs: https://lookup-id.com/
    bdmy = fbgroup.Group("497068793653308")
    bdsg = fbgroup.Group("208146119270477")
    dssg = fbgroup.Group("157938781081987")

    bdmy_members = bdmy.fetch_membership(oauth_access_token)
    bdsg_members = bdsg.fetch_membership(oauth_access_token)
    dssg_members = dssg.fetch_membership(oauth_access_token)

    bdmy_ids = set([x["id"] for x in bdmy_members])
    bdsg_ids = set([x["id"] for x in bdsg_members])
    dssg_ids = set([x["id"] for x in dssg_members])

    print("Number of members:")
    print("\tBDMY : {}".format(len(bdmy_ids)))
    print("\tBDSG : {}".format(len(bdsg_ids)))
    print("\tDSSG : {}".format(len(dssg_ids)))
    print("\tUnion: {}".format(len(bdmy_ids.union(bdsg_ids.union(dssg_ids)))))

    v = venn3([bdmy_ids, bdsg_ids, dssg_ids], ["BDMY", "BDSG", "DSSG"])
    plt.show()

    return

if __name__ == "__main__":
    main()
