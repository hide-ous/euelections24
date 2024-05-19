import collections
import http
import json
import logging
import os
import time
from watchfiles import watch, DefaultFilter, Change
import requests
from urllib.parse import urlparse
import pathlib
from hashlib import sha256
import multiprocessing

DEBOUNCE_QUEUE_LENGTH = 1000
NUM_PROCESSES = 10
CHAR_LIMIT = 100

manager = multiprocessing.Manager()
observed_urls = manager.list()  # List to keep track of the order of URLs
observed_urls_set = manager.dict()  # Dictionary to check for observed URLs

logging.basicConfig()

logger = logging.getLogger(name='content_downloader')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


class NJSONFilter(DefaultFilter):
    allowed_extensions = '.jsonl', '.njson', 'ndjson'

    def __call__(self, change: Change, path: str) -> bool:
        return (
                super().__call__(change, path) and
                path.endswith(self.allowed_extensions)
        )


def to_fname(url, char_limit=CHAR_LIMIT):
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    hash = sha256(url.encode()).hexdigest()[:char_limit]
    return hash + ext


def download(url, fpath, sleep_retry=10, retries=4):
    for i in range(retries):
        try:
            with requests.get(url, stream=True) as r:
                try:
                    r.raise_for_status()
                    pathlib.Path(os.path.dirname(fpath)).mkdir(parents=True, exist_ok=True)

                    with open(fpath, 'wb+') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    break
                except Exception as e:
                    logger.debug(f"could not download {url},", e)
                    break
        except http.client.IncompleteRead:
            logger.debug(f'incomplete read for {url}, retrying after {sleep_retry} seconds (try {i}/{retries})')
            time.sleep(sleep_retry)
        except Exception as e:
            logger.debug(f"could not download {url},", e)


def get_change_objects(fname, last_positions):
    last_position = last_positions.get(fname, 0)
    logger.debug(f"last position: {last_position}")
    with open(fname, encoding='utf-8') as f:
        f.seek(last_position)
        while l := f.readline():
            obj = json.loads(l)
            for media in obj.get('media', []):
                yield media['url']
        last_positions[fname] = f.tell()


def worker_main(queue, observed_urls, observed_urls_set):
    logger.debug(f"{os.getpid()} working")
    while True:
        url = queue.get(block=True)  # block=True means make a blocking call to wait for items in queue
        if url is None:
            logger.debug(f"{os.getpid()} received stop signal")
            break

        mine = False  # the url is new and this worker will handle it
        if url not in observed_urls_set:
            observed_urls_set[url] = True
            observed_urls.append(url)
            mine = True
            if len(observed_urls) > DEBOUNCE_QUEUE_LENGTH:
                oldest_url = observed_urls.pop(0)
                del observed_urls_set[oldest_url]

        if mine:
            fname = to_fname(url)
            media_fpath = os.path.join('media', fname[:2], fname[2:4], fname)
            if not os.path.exists(media_fpath):
                logger.debug(f'{os.getpid()} Downloading {url} to {media_fpath}')
                download(url, media_fpath)


def main(seed_fnames):
    last_positions = dict()
    the_queue = multiprocessing.Queue()

    the_pool = multiprocessing.Pool(NUM_PROCESSES, worker_main, (the_queue, observed_urls, observed_urls_set))
    for fname in seed_fnames:
        if not os.path.exists(fname): continue
        for url in get_change_objects(fname, last_positions):
            if url is not None:
                the_queue.put(url)
    for changes in watch('.', recursive=False, watch_filter=NJSONFilter(), raise_interrupt=False):
        logger.debug(str(changes))
        for change in changes:
            for url in get_change_objects(change[1], last_positions):
                if url is not None:
                    logger.debug(f'adding {url} to the queue')
                    the_queue.put(url)
    for i in range(NUM_PROCESSES):
        the_queue.put(None)
    # prevent adding anything more to the queue and wait for queue to empty
    the_queue.close()
    the_queue.join_thread()
    # prevent adding anything more to the process pool and wait for all processes to finish
    the_pool.close()
    the_pool.join()


if __name__ == '__main__':
    main(seed_fnames=['iglog.njson', 'fblog.njson'])
