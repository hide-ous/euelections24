# euelections24

`real_time_monitor.py`:
* Polls for new content from crowdtangle
* Example usage: `python -u real_time_monitor.py --every 30 --timeunit minutes --key APIKEY --file log.njson` 

`content_downloader.py`:
* Watches json line files in the current folder for changes, expecting one post per line, and downloads the media (images, videos) of the new posts as they are appended to the files.
* It is possible to download media for posts already present in a seed set of files
* Media are stored this way:
  - the file name (minus the file extension) is the first 100 characters of the sha256 of the url
  - the file is stored in `media/[first two characters of the file name]/[next two characters of the file name]/filename.extension`
  - technically it is possible that two distinct urls map to the same file name; it should be highly improbable
* Example usage: `python -u content_downloader.py`
  