#!/usr/bin/env python
from __future__ import print_function
import argparse
import logging
import pandas
import time

from matplotlib import pyplot as plt
from matplotlib_venn import venn3

import fbgroup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Facebook Group activity stuff (WIP)")
    parser.add_argument("--oauth-token-file", action="store", type=str, default="oauth_file",
                        help="Path to file containing oauth token")
    args = parser.parse_args()

    with open(args.oauth_token_file, 'r') as oauth_fd:
        oauth_access_token = oauth_fd.readlines()[0]

    # I use these to find out group IDs: https://lookup-id.com/
    groups = {}
    groups["BDSG"] = fbgroup.Group("208146119270477")
    groups["BDMY"] = fbgroup.Group("497068793653308")
    groups["DSSG"] = fbgroup.Group("157938781081987")
    #groups["WCKL"] = fbgroup.Group("179139768764782")

    groups["BDSG"].unpickle_posts_from_file("bdsg_fbg_208146119270477_1474960701.dat")
    groups["BDMY"].unpickle_posts_from_file("bdmy_fbg_497068793653308_1474955119.dat")
    groups["DSSG"].unpickle_posts_from_file("dssg_fbg_157938781081987_1474977355.dat")
    #groups["WCKL"].unpickle_posts_from_file("wckl_fbg_179139768764782_1475021971.dat")

    #index = pandas.to_datetime([p.updated_date for p in groups[groups.keys()[0]].posts])
    for group_name in groups.keys():
        group = groups[group_name]
        index = pandas.to_datetime([p.updated_date for p in group.posts])
        series_engagement_cnt = pandas.Series([p.get_all_engagements_count() for p in group.posts],
                                              index=index)
        series_unique_engagers_cnt = pandas.Series([len(set(p.get_all_engager_ids())) for p in group.posts],
                                                   index=index)
        resample_engagement_cnt_daily = series_engagement_cnt.resample('1D',
                                                                       how='sum').fillna(0)
        resample_unique_engagers_cnt_daily = series_unique_engagers_cnt.resample('1D',
                                                                                 how='sum').fillna(0)
        rolling_average_30d_engagement_cnt = pandas.rolling_mean(resample_engagement_cnt_daily,
                                                                 window=30)
        rolling_average_30d_unique_engagers_cnt_daily = pandas.rolling_mean(resample_unique_engagers_cnt_daily,
                                                                            window=30)
        #ax = resample_engagement_cnt_daily.plot(style="bo-",
        #                                        title="Engagements (posts, comments, reactions, comment likes)",
        #                                        legend=True,
        #                                        label='Engagements daily agg')
        ax = rolling_average_30d_engagement_cnt.plot(#ax=ax,
                                                #style="m-",
                                                style="-",
                                                linewidth=3.0,
                                                legend=True,
                                                label="{} engagements 30 day moving ave".format(group_name))
        #resample_unique_engagers_cnt_daily.plot(ax=ax,
        #                                        style="go-",
        #                                        legend=True,
        #                                        label="Unique engagers daily agg")
        rolling_average_30d_unique_engagers_cnt_daily.plot(ax=ax,
                                                           #style="y-",
                                                           style="-",
                                                           linewidth=3.0,
                                                           legend=True,
                                                           label="{} unique engagers 30 day moving ave".format(group_name))

        #resample_engagement_ave_daily = series_engagement_cnt.resample('1D', how='mean').fillna(0)
        #rolling_average_30d_engagement_ave_daily = pandas.rolling_mean(resample_engagement_ave_daily, window=30)
        #rolling_average_30d_engagement_ave_daily.plot(ax=ax,
        #                                              style="c-",
        #                                              linewidth=3.0,
        #                                              legend=True,
        #                                              label="Engagements per-post 30 day moving ave")


        #resample_engagement_uq_daily = series_engagement_cnt.resample('1D', how=lambda x: x.mean() + x.std()).fillna(0)
        #rolling_average_30d_engagement_uq_daily = pandas.rolling_mean(resample_engagement_uq_daily, window=30)
        #rolling_average_30d_engagement_uq_daily.plot(ax=ax,
        #                                              style="b-",
        #                                              linewidth=3.0,
        #                                              legend=True,
        #                                              label="Engagements per-post 30 day moving mean+1std")

        ax.set_xlabel("Update date of post")
        ax.set_ylabel("Number of engagement events")
    plt.show()

    return

if __name__ == "__main__":
    main()
