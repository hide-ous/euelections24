import json
import logging
import os
import pathlib
import time
from datetime import datetime, timedelta
import optparse

import schedule
from pytangle.api import API, CONFIG_FILE_LOCATIONS

from content_downloader import to_fname, download
from real_time_monitor import TIMESTAMP_FORMAT

logger = logging.getLogger(name='OneTimeScraper')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)
options=None

def main():
    global options, logger
    usage = "example usage: one_time_scraper.py --key APIKEY"
    parser = optparse.OptionParser(usage)
    parser.add_option("-f", "--file", dest="filename", default='pytangle_{}.njson'.format(
        time.strftime('%Y%m%d%H%M%S')),
                      help="store to FILE", metavar="FILE")

    def split_list(option, opt, value, parser, *args, **kwargs):
        setattr(parser.values, option.dest, value.split(','))

    parser.add_option("-l", "--lists", dest="lists", default=None, action='callback',
                      callback=split_list, nargs=1, type='string',
                      help="comma-separated ids of the list to scrape, e.g. -l 123,345")

    parser.add_option("-q", "--quiet",
                      action="store_true", dest="quiet", default=False,
                      help="don't print status messages to stdout")

    parser.add_option("-k", "--key", dest="api_key", default=None,
                      help="API key", metavar="CTAPIKEY")

    parser.add_option("-c", "--config", dest="config_path", default=CONFIG_FILE_LOCATIONS,
                      help="pytangle config file location")
    parser.add_option("-d", "--days", dest="days", default=7,
                      help="how many days in the past to scrape")
    (options, args) = parser.parse_args()

    run_once()
    job = schedule.every(options.days).days.do(run_once)
    while True:
        logger.debug('next run at ' + str(schedule.next_run()))
        schedule.run_pending()
        sleep_time = (schedule.next_run() - datetime.now()).total_seconds()
        logger.debug('sleeping for {} seconds'.format(sleep_time))
        time.sleep(sleep_time)


def run_once():
    global options, logger
    api = API(token=options.api_key)
    scrape_timestamp = time.strftime('%Y%m%d%H%M%S')
    with open(f'log{scrape_timestamp}.njson', 'w+', encoding='utf8') as out_file:

        try:
            for post in api.posts(listIds=options.lists,
                                  includeHistory=True,
                                  sortBy='date', count=-1,
                                  startDate=(datetime.utcnow() - timedelta(days=options.days)).strftime(
                                      TIMESTAMP_FORMAT),
                                  ):

                post['scraped'] = datetime.utcnow().strftime(TIMESTAMP_FORMAT)
                for media in post.get('media', []):
                    url = media['url']
                    fname = to_fname(url)
                    media_fpath = os.path.join(f'media{scrape_timestamp}', fname)
                    pathlib.Path(os.path.dirname(media_fpath)).mkdir(parents=True, exist_ok=True)
                    if not os.path.exists(media_fpath):
                        logger.debug(f'{os.getpid()} Downloading {url} to {media_fpath}')
                        download(url, media_fpath)
                    media['file_path']=media_fpath
                out_file.write(json.dumps(post) + '\n')
        except Exception as e:
            logger.exception(repr(e))


if __name__ == '__main__':
    main()
