from __future__ import print_function

import argparse
import datetime
import dateutil.parser
import decimal
import facepy
import json
import logging
import numpy
import pandas
import pickle
import pdb
import pprint
import sys
import time
import traceback
import weakref

import matplotlib.pyplot as plt


logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

def main():

    parser = argparse.ArgumentParser(description="BDMY FB group data wrangler")
    parser.add_argument("--last-n-pages", action="store", type=int, default=None,
                        help="Only fetch the last N most recent pages worth of data. Not applicable if loading from file.")
    parser.add_argument("--group-id", action="store", type=str, default="497068793653308",
                        help="Group ID (default is the group id of https://www.facebook.com/groups/bigdatamy/.)")
    parser.add_argument("--load-from-file", action="store", type=str, default=None,
                        help="File to unpickle from; if not specified, download from Facebook servers.")
    args = parser.parse_args()

    wckl = Group("179139768764782")
    wckl.unpickle_posts_from_file("wckl-2016-06-24.dat")
    wckl.generate_standard_data_sets()
    wckl_resample_engagement_cnt_daily = wckl.series_engagement_cnt.resample('1D',
                                                                             how='sum').fillna(0)
    wckl_resample_unique_engagers_cnt_daily = wckl.series_unique_engagers_cnt.resample('1D',
                                                                                       how='sum').fillna(0)
    wckl_rolling_average_30d_engagement_cnt = pandas.rolling_mean(wckl_resample_engagement_cnt_daily,
                                                                  window=30)
    wckl_rolling_average_30d_unique_engagers_cnt_daily = pandas.rolling_mean(wckl_resample_unique_engagers_cnt_daily,
                                                                            window=30)
    wckl_resample_engagement_ave_daily = wckl.series_engagement_cnt.resample('1D', how='mean').fillna(0)
    wckl_rolling_average_30d_engagement_ave_daily = pandas.rolling_mean(wckl_resample_engagement_ave_daily, window=30)
    wckl_resample_engagement_uq_daily = wckl.series_engagement_cnt.resample('1D', how=lambda x: x.mean() + x.std()).fillna(0)
    wckl_rolling_average_30d_engagement_uq_daily = pandas.rolling_mean(wckl_resample_engagement_uq_daily, window=30)

    try:
        bdmy = Group(args.group_id)
        if args.load_from_file is not None:
            bdmy.unpickle_posts_from_file(args.load_from_file)
        else:
            oauth_access_token_file = 'oauth_file'
            with open(oauth_access_token_file, 'r') as oauth_fd:
                oauth_access_token = oauth_fd.readlines()[0]
            bdmy.fetch(oauth_access_token,
                       args.last_n_pages)

        bdmy.generate_standard_data_sets()
        print("Close plot to enter REPL...")
        resample_engagement_cnt_daily = bdmy.series_engagement_cnt.resample('1D',
                                                                            how='sum').fillna(0)
        resample_unique_engagers_cnt_daily = bdmy.series_unique_engagers_cnt.resample('1D',
                                                                                      how='sum').fillna(0)
        rolling_average_30d_engagement_cnt = pandas.rolling_mean(resample_engagement_cnt_daily,
                                                                 window=30)
        rolling_average_30d_unique_engagers_cnt_daily = pandas.rolling_mean(resample_unique_engagers_cnt_daily,
                                                                            window=30)
        #ax = resample_engagement_cnt_daily.plot(style="bo-",
        #                                        title="BDMY: Engagements (posts, comments, reactions, comment likes)",
        #                                        legend=True,
        #                                        label='Engagements daily agg')
        ax = rolling_average_30d_engagement_cnt.plot(#ax=ax,
                                                style="r-",
                                                linewidth=3.0,
                                                legend=True,
                                                label="BDMY: Engagements 30 day moving ave")
        wckl_rolling_average_30d_engagement_cnt.plot(ax=ax,
                                                style="b-",
                                                linewidth=3.0,
                                                legend=True,
                                                label="WCKL: Engagements 30 day moving ave")
        #resample_unique_engagers_cnt_daily.plot(ax=ax,
        #                                        style="go-",
        #                                        legend=True,
        #                                        label="Unique engagers daily agg")
        rolling_average_30d_unique_engagers_cnt_daily.plot(ax=ax,
                                                           style="y-",
                                                           #linewidth=3.0,
                                                           legend=True,
                                                           label="BDMY: Unique engagers 30 day moving ave")
        wckl_rolling_average_30d_unique_engagers_cnt_daily.plot(ax=ax,
                                                           style="g-",
                                                           #linewidth=3.0,
                                                           legend=True,
                                                           label="WCKL: Unique engagers 30 day moving ave")

        resample_engagement_ave_daily = bdmy.series_engagement_cnt.resample('1D', how='mean').fillna(0)
        rolling_average_30d_engagement_ave_daily = pandas.rolling_mean(resample_engagement_ave_daily, window=30)
        rolling_average_30d_engagement_ave_daily.plot(ax=ax,
                                                      style="c-",
                                                      #linewidth=3.0,
                                                      legend=True,
                                                      label="BDMY: Engagements per-post 30 day moving ave")


        resample_engagement_uq_daily = bdmy.series_engagement_cnt.resample('1D', how=lambda x: x.mean() + x.std()).fillna(0)
        rolling_average_30d_engagement_uq_daily = pandas.rolling_mean(resample_engagement_uq_daily, window=30)
        #rolling_average_30d_engagement_uq_daily.plot(ax=ax,
        #                                              style="b-",
        #                                              linewidth=3.0,
        #                                              legend=True,
        #                                              label="BDMY: Engagements per-post 30 day moving mean+1std")

        wckl_rolling_average_30d_engagement_ave_daily.plot(ax=ax,
                                                      style="m-",
                                                      #linewidth=3.0,
                                                      legend=True,
                                                      label="WCKL: Engagements per-post 30 day moving ave")


        ax.set_xlabel("Update date of post")
        ax.set_ylabel("Number of engagement events")
        plt.show()

        popular_posts = []
        for post in bdmy.posts:
            if post.get_all_engagements_count() > rolling_average_30d_engagement_uq_daily[post.updated_date.replace(hour=0, minute=0, second=0)]:
                popular_posts.append(post)

    except:
        traceback.print_exc()
    finally:
        print("Entering REPL. To interact with current dataset, play with the bdmy object.")
        if not args.load_from_file:
            print("Tip: save your data for reuse with the --load-from-file arg, by calling the pickle_posts method on the bdmy object.")
        pdb.set_trace()


class Engagement(object):

    def __init__(self, raw_info):
        self._raw_info = raw_info
        print("ENGAGEMENT:")
        pprint.pprint(self._raw_info)


class Reaction(object):
    reaction_types = set()

    def __init__(self, raw_info, is_like=False):
        self._raw_info = raw_info
        if is_like:
            self.add_reaction_type("LIKE")
        else:
            self.add_reaction_type(self._raw_info['type'])

    def get_reactor_id(self):
        return self._raw_info['id']

    def add_reaction_type(self, reaction_type_string):
        rtype = str(reaction_type_string).upper()
        self.reaction_type = rtype
        self.add_seen_reaction_type(rtype)

    @classmethod
    def add_seen_reaction_type(cls, reaction_type_string):
        cls.reaction_types.add(str(reaction_type_string).upper())


class Comment(object):
    reactions = None

    def __init__(self, raw_info):
        self._raw_info = raw_info
        self.fb_id = self._raw_info["id"]
        self.reactions = []

    def add_reaction(self, reaction):
        self.reactions.append(reaction)

    def get_commenter_id(self):
        return self._raw_info['id']

    def get_reactor_ids(self):
        return frozenset([r.get_reactor_id() for r in self.reactions])


class Post(object):
    comments = None
    reactions = None

    def __init__(self, raw_info):
        self._base_info = raw_info
        self.fb_id = self._base_info["id"]
        self.updated_date = dateutil.parser.parse(self._base_info["updated_time"])
        self.reactions = []
        self.comments = []
        self.url_post_resource = "/permalink/{}/".format(self._base_info['id'].split('_')[1])

    def get_poster(self):
        return self.poster_id

    def get_all_engager_ids(self):
        """
        Returns a deduped set of user IDs for everyone who has engaged with the post - poster, commenters, reactors, and comment reactors.
        """
        commenters = frozenset([c.get_commenter_id() for c in self.comments])
        comment_reactors = reduce(frozenset().union, [c.get_reactor_ids() for c in self.comments], frozenset())
        post_reactors = frozenset([r.get_reactor_id() for r in self.reactions])
        all_engagers = set()
        all_engagers = all_engagers.union(commenters)
        all_engagers = all_engagers.union(comment_reactors)
        all_engagers = all_engagers.union(post_reactors)
        all_engagers.add(self.get_poster())
        return frozenset(all_engagers)

    def get_all_engagements_count(self):
        count = len(self.reactions)
        for comment in self.comments:
            count += 1
            count += len(comment.reactions)
        return count + 1  # +1 because the very existence of this post is an engagement

    def add_comment(self, comment):
        self.comments.append(comment)

    def add_reaction(self, reaction):
        self.reactions.append(reaction)


class Group(object):
    posts = None

    def __init__(self, group_id):
        logging.info("created Group object for group_id %s", str(group_id))
        self.group_id = group_id
        self.posts = []
        self.oauth_access_token = None
        self.graph = None

    def add_post(self, post):
        self.posts.append(post)

    def make_url(self, post_obj=None):
        url = "https://www.facebook.com/groups/{}".format(self.group_id)
        if post_obj:
            url += post_obj.url_post_resource
        return url

    def pickle_posts(self, filename):
        with open(filename, "wb") as pickle_dst:
            pickle.dump(self.posts, pickle_dst)

    def unpickle_posts_from_file(self, filename):
        with open(filename, "rb") as pickle_src:
            self.posts = pickle.load(pickle_src)

    def generate_standard_data_sets(self):
        self.time_index = pandas.to_datetime([p.updated_date for p in self.posts])
        self.series_engagement_cnt = pandas.Series([p.get_all_engagements_count() for p in self.posts],
                                                   index=self.time_index)
        self.series_unique_engagers_cnt = pandas.Series([len(set(p.get_all_engager_ids())) for p in self.posts],
                                                         index=self.time_index)

        logging.info("Validating time index")
        last_time = None
        for next_time in reversed(self.time_index):
            if last_time:
                assert not last_time > next_time, "Integrity issue with time index: last_time = {}, next_time = {}".format(last_time, next_time)
                if last_time == next_time:
                    logging.warning("Weirdness in time index: last_time = {}, next_time = {}".format(last_time, next_time))
            last_time = next_time

        logging.info("Making time range pairs sequence")
        time_range_pairs = list(reversed([(self.time_index[x], self.time_index[x+1]) for x in range(len(self.time_index))[:-1]]))
        time_range_pairs.insert(0, (time_range_pairs[0][1], (time_range_pairs[0][1]- datetime.timedelta(days=1))))
        assert len(time_range_pairs) == len(self.time_index)
        unique_engagers_cum_cnt = []
        seen_engagers = set()

        end_time, start_time = time_range_pairs.pop(0)
        logging.info("Making cumulative engagers count over posts")
        logging.debug("Updating time range; initial range is {} -> {}".format(start_time, end_time))
        expect_more_posts = True
        for post in list(reversed(self.posts)):
            assert expect_more_posts
            logging.debug("post updated_date={}".format(post.updated_date))
            assert post.updated_date >= start_time
            for engager_id in post.get_all_engager_ids():  # this MUST come before the end_time check
                seen_engagers.add(engager_id)
            if post.updated_date == end_time:
                unique_engagers_cum_cnt.append(len(seen_engagers))
                if time_range_pairs:
                    logging.debug("Updating time range; current range is {} -> {}".format(start_time, end_time))
                    end_time, start_time = time_range_pairs.pop(0)
                    logging.debug("Updated time range; new range is {} -> {}".format(start_time, end_time))
                    assert end_time >= start_time
                else:
                    expect_more_posts = False
        assert len(unique_engagers_cum_cnt) == len(self.time_index)
        self.series_unique_engagers_cum_cnt = pandas.Series(list(reversed(unique_engagers_cum_cnt)), self.time_index)

        self.engagers_engagement_cnt = {}
        for post in self.posts:
            for engager in post.get_all_engager_ids():
                if engager in self.engagers_engagement_cnt:
                    self.engagers_engagement_cnt[engager] += 1
                else:
                    self.engagers_engagement_cnt[engager] = 1

    def graph_get_with_oauth_retry(self, url, page, max_retry_cycles=3):
        """a closure to let the user deal with oauth token expiry"""
        assert max_retry_cycles > 0
        retry_cycle = 0
        while True:
            if retry_cycle >= max_retry_cycles:
                logging.error("Giving up on query {} after {} tries; last exception was {}/{}".format(url,
                                                                                                      retry_cycle,
                                                                                                      type(last_exc),
                                                                                                      last_exc))
                return list()
            retry_cycle += 1
            try:
                return list(self.graph.get(url, page=page))
            except Exception as exc:
                last_exc = exc
                logging.error(exc)
                if "unknown error" not in exc.message:
                    # might be able to recover with a retry or a new token
                    logging.info("Failed with {}/{}: doing simple retry".format(type(exc), exc))
                    try:
                        time.sleep(3)
                        return list(self.graph.get(url, page=page))
                    except facepy.exceptions.OAuthError as exc:
                        logging.error("Retry {} failed; {}/{}".format(retry_cycle, type(exc), exc))
                        logging.info("Update your token; generate a new token by visiting {}".format("https://developers.facebook.com/tools/explorer"))
                        logging.info("Waiting for user to enter new oauth access token...")
                        self.oauth_access_token = raw_input("Enter new oath access token: ")
                        self.oauth_access_token = self.oauth_access_token.strip()
                        self.graph = facepy.GraphAPI(self.oauth_access_token)

    def fetch(self, oauth_access_token, max_pages=None):
        """
        For testing purposes one may limit max_pages.
        """
        self.oauth_access_token = oauth_access_token
        self.graph = facepy.GraphAPI(self.oauth_access_token)

        data = self.graph_get_with_oauth_retry('/v2.6/{}/feed'.format(self.group_id), page=True)
        raw_post_data = []
        page_count = 0
        print("foo")
        for page in data:
            if max_pages and page_count >= max_pages:
                break
            page_count += 1
            try:
                logging.debug("new page")
                if "data" in page:
                    logging.debug("page has %s posts", len(page['data']))
                    raw_post_data += [p for p in page['data']]
                    logging.info("current accumulated posts count: %d, oldest timestamp: %s",
                                 len(raw_post_data),
                                 raw_post_data[-1]["updated_time"])
            except:
                pprint.pprint(page)
                raise

        for post in raw_post_data:
            try:
                post_obj = Post(post)
            except:
                logging.error("Problem with raw post data: %s", pprint.pformat(post))
                raise

            self.add_post(post_obj)

            try:
                logging.info("Fleshing out post {} of {}; {}".format(len(self.posts), len(raw_post_data), self.make_url(post_obj)))
                # TODO sort out this horrible boilerplate

                # Step 0: get post from
                logging.info("Fleshing out post {} of {}; {} -- getting from info".format(len(self.posts), len(raw_post_data), self.make_url(post_obj)))
                post_obj.from_info = self.graph_get_with_oauth_retry('/v2.6/{}?fields=from'.format(post_obj.fb_id), page=True)
                assert len(post_obj.from_info) == 1, post_obj.from_info
                post_obj.poster_id = post_obj.from_info[0]['from']['id']

                # Step 1: extract post reactions
                logging.info("Fleshing out post {} of {}; {} -- getting reactions".format(len(self.posts), len(raw_post_data), self.make_url(post_obj)))
                reactions_pages = list(self.graph_get_with_oauth_retry('/v2.6/{}/reactions'.format(post_obj.fb_id), page=True))
                logging.debug("reactions: %d, %s", len(reactions_pages), pprint.pformat(reactions_pages))

                reactions = []
                try:
                    if reactions_pages and reactions_pages[-1]:
                        for reactions_page in reactions_pages:
                            reactions += reactions_page['data']
                        if 'paging' in reactions_pages[-1]:
                            if 'next' in reactions_pages[-1]['paging']:
                                raise Exception("well that was unexpected")
                except:
                    logging.error("Tripped up on {}".format(pprint.pformat(reactions_pages)))
                    raise

                for reaction_data in reactions:
                    post_obj.add_reaction(Reaction(reaction_data))

                # Step 2: extract post comments, along with their likes
                logging.info("Fleshing out post {} of {}; {} -- getting comments".format(len(self.posts), len(raw_post_data), self.make_url(post_obj)))
                comments_pages = list(self.graph_get_with_oauth_retry('/v2.6/{}/comments?fields=from,created_time,message,id,likes'.format(post_obj.fb_id), page=True))
                logging.debug("comments: %d, %s", len(comments_pages), pprint.pformat(comments_pages))
                comments = []
                try:
                    if comments_pages and comments_pages[-1]:
                        for comments_page in comments_pages:
                            comments += comments_page['data']
                        if 'paging' in comments_pages[-1]:
                            if 'next' in comments_pages[-1]['paging']:
                                raise Exception("well that was unexpected")
                except:
                    logging.error("Tripped up on {}".format(pprint.pformat(comments_pages)))
                    raise

                for comments_data in comments:
                    comment_obj = Comment(comments_data)
                    post_obj.add_comment(comment_obj)

                    # Step 3: extract post comment reactions
                    if 'likes' in comments_data:
                        for like_info in comments_data["likes"]["data"]:
                            comment_obj.add_reaction(Reaction(like_info, is_like=True))

            except Exception:
                logging.warn("Problem fleshing out post data: %s - skipping and continuing", pprint.pformat(post_obj._base_info))
                traceback.print_exc()


if __name__ == "__main__":
    main()
