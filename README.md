# bigdatamy_fb_group_analysis
Tools to analyse the content of the Big Data Malaysia Facebook group

Some legacy stuff (including a iPython notebook that is an introductory-level tutorial on how
to do this stuff) is in the `legacy` subdir.

The current working thing is just the `pull_feed.py` program.

## Quick start:

0. Make sure you are already a member of the [Big Data Malaysia Facebook group](https://www.facebook.com/groups/bigdatamy/).
1. Visit [https://developers.facebook.com/tools/explorer](https://developers.facebook.com/tools/explorer) to obtain your access token.
2. Save the token to `oauth_file` in the same directory as `pull_feed.py`.
3. Run `pull_feed.py`.
4. Once `pull_feed.py` drops you to the pdb prompt, it's a good idea to save the downloaded data so you can load it later.
