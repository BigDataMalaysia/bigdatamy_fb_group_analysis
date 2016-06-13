from __future__ import print_function

import argparse
import dateutil.parser
import decimal
import facepy
import json
import logging
import pdb
import pprint
import sys
import time
import weakref

import matplotlib.pyplot as plt


SAVE_DIR = "BigDataMyData"
GROUP_ID = "497068793653308"

logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

def main():

    parser = argparse.ArgumentParser(description="BDMY FB group data wrangler")
    parser.add_argument("--last-n-pages", action="store", type=int, default=None,
                        help="Only fetch the last N most recent pages worth of data. Not applicable if loading from file.")
    parser.add_argument("--load-from-file", action="store", type=str, default=None,
                        help="File to unpickle from; if not specified, download from Facebook servers.")
    args = parser.parse_args()

    bdmy = Group(GROUP_ID)
    if args.load_from_file is not None:
        raise Exception("TODO")
    else:
        oauth_access_token_file = 'oauth_file'
        with open(oauth_access_token_file, 'r') as oauth_fd:
            oauth_access_token = oauth_fd.readlines()[0]
        try:
            bdmy.fetch(oauth_access_token,
                       args.last_n_pages)
        except facepy.exceptions.OAuthError as exc:
            logging.error(exc)
            logging.info("Update your token in {}; generate a new token by visiting {}".format(oauth_access_token_file,
                                                                                               "https://developers.facebook.com/tools/explorer"))
            sys.exit(1)

    plt.plot_date(x=[x.updated_date for x in bdmy.posts],
                  y=[x.get_all_engagements_count() for x in bdmy.posts],
                  fmt="ro-")
    plt.title("Engagements (posts, comments, reactions, likes on comments)")
    plt.ylabel("Number of engagement events")
    plt.grid(True)
    plt.show()
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
        self._raw_info = raw_info
        self.fb_id = self._raw_info["id"]
        self.updated_date = dateutil.parser.parse(self._raw_info["updated_time"])
        self.reactions = []
        self.comments = []
        self.url = "https://www.facebook.com/groups/bigdatamy/permalink/{}/".format(self._raw_info['id'].split('_')[1])

    def get_poster(self):
        raise Exception("TODO")

    def get_all_engager_ids(self):
        """
        Returns a deduped set of user IDs for everyone who has engaged with the post - poster, commenters, reactors, and comment reactors.
        """
        commenters = frozenset([c.get_commenter_id() for c in self.comments])
        comment_reactors = reduce(frozenset().union, [c.get_reactor_ids() for c in self.comments])
        post_engagers = frozenset([r.get_reactor_id() for r in self.reactions])
        all_engagers = set()
        all_engagers.union(commenters)
        all_engagers.union(comment_reactors)
        all_engagers.union(post_engagers)
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
        logging.info("created Group object for group_id %d", group_id)
        self.group_id = group_id
        self.posts = []

    def add_post(self, post):
        self.posts.append(post)

    def fetch(self, oauth_access_token, max_pages=None):
        """
        For testing purposes one may limit max_pages.
        """
        graph = facepy.GraphAPI(oauth_access_token)
        data = graph.get('/v2.6/{}/feed'.format(self.group_id), page=True)
        raw_post_data = []
        page_count = 0
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

            logging.info("Fleshing out post {} of {}; {}".format(len(self.posts), len(raw_post_data), post_obj.url))
            # TODO sort out this horrible boilerplate

            # Step 1: extract post reactions
            reactions_pages = list(graph.get('/v2.6/{}/reactions'.format(post_obj.fb_id), page=True))
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

            # Step 2: extract post comments
            comments_pages = list(graph.get('/v2.6/{}/comments'.format(post_obj.fb_id), page=True))
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
                comment_reactions_pages = list(graph.get('/v2.6/{}/likes'.format(comment_obj.fb_id), page=True))
                logging.debug("comment reactions: %d, %s", len(comment_reactions_pages), pprint.pformat(comment_reactions_pages))
                comment_reactions = []
                try:
                    if comment_reactions_pages and comment_reactions_pages[-1]:
                        for comment_reactions_page in comment_reactions_pages:
                            comment_reactions += comment_reactions_page['data']
                        if 'paging' in comment_reactions_pages[-1]:
                            if 'next' in comment_reactions_pages[-1]['paging']:
                                raise Exception("well that was unexpected")
                except:
                    logging.error("Tripped up on {}".format(pprint.pformat(comment_reactions_pages)))
                    raise

                for comment_reaction_data in comment_reactions:
                    comment_obj.add_reaction(Reaction(comment_reaction_data, is_like=True))


if __name__ == "__main__":
    main()
