""" PULLS GOSPEL MUSIC DATA FROM THE YOUTUBE API V3 USING THE PYTHON REQUEST MODULE """


import os
import logging
import requests
from datetime import datetime
from time import sleep
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

ingestion_date = datetime.now().strftime("%d-%m-%Y")

log_file = LOG_DIR / f"pipeline_run: {ingestion_date}"



logger = logging.getLogger(__name__)


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format=" %(asctime)s - [%(levelname)s] - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

session = requests.Session()

BASE_URL = "https://www.googleapis.com/youtube/v3"

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

with (BASE_DIR / "gospel_musicians.csv").open(
    "r", newline="", encoding="utf-8"
) as gospel_artists:
    reader = csv.reader(gospel_artists)
    SEARCH_TERMS = [row[0].strip() for row in reader if row and row[0].strip()]

MAX_PAGES_PER_TERM = 5
MAX_TIMEOUT_RETRIES = 3


def search_videos(query):
    """ PAGINATES THROUGH YOUTUBE FOR A 'SEARCH TERM' AND RETURNS A LIST OF RAW SEARCH ITEMS"""
    if not YOUTUBE_API_KEY:
        raise EnvironmentError("YOUTUBE_API_KEY is not set")

    search_results = []
    page_count = 0
    next_page_token = None
    timeout_retries = 0
    
    while page_count < MAX_PAGES_PER_TERM:   
        
        params = {
            "part":"snippet",
            "q": f"{query} gospel song",
            "key":YOUTUBE_API_KEY,
            "type":"video",
            "maxResults": 50,
            "videoCategoryId":"10",
        }
        
        if next_page_token:
            params["pageToken"] = next_page_token
            
        try: 
            response = session.get(f"{BASE_URL}/search", params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                raise RuntimeError(
                    "YouTube API rate limit or quota exceeded; stopping ingestion"
                ) from e
            logger.error(
                "HTTP error searching %s page %d: status %s",
                query,
                page_count + 1,
                response.status_code,
            )
            break
        except requests.exceptions.Timeout:
            timeout_retries += 1
            logger.warning(
                f"Timeout at {query} page {page_count + 1} "
                f"({timeout_retries}/{MAX_TIMEOUT_RETRIES})"
            )
            if timeout_retries >= MAX_TIMEOUT_RETRIES:
                break
            sleep(2)
            continue
        except requests.exceptions.RequestException as e:
            logger.error(
                "Request failed at %s page %d: %s",
                query,
                page_count + 1,
                type(e).__name__,
            )
            break
            
        
        data = response.json()
        timeout_retries = 0
        items = data.get("items", [])
        search_results.extend(items)
        
        logger.info(f"Fetched {len(items)} items for {query} page {page_count + 1}. Total results: {len(search_results)} ")
        
        page_count += 1
        
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            logger.info(f"No more pages for {query}")
            break
        
        sleep(0.5)
        
    return search_results


def chunk_list(lst, size =50):
    """Splits a list into chunks of 'size' for batching API calls """  
    for i in  range(0, len(lst),size):
        yield lst[i:i+size]


def get_video_details (video_ids):
    
    """  FETCHES VIDEO DETAILS FOR UP TO 50 IDS.
        RETURNS A LIST OF RAW VIDEO DETAILS.
    """
    if not video_ids:
        return []

    all_videos = []
    
    for batch in chunk_list(video_ids):
        params = {
            "part":"snippet,contentDetails,statistics",
            "id": "," .join(batch),
            "key":YOUTUBE_API_KEY   
        }
                  

        try:
            response = session.get(f"{BASE_URL}/videos", params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Video detail request failed for batch of %d: %s", len(batch), type(e).__name__)
            continue

        data = response.json()
        items = data.get("items", [])
        all_videos.extend(items)
        
        logger.info(f"Results for {len(items)} videos: total = {len(all_videos)}")
        
        sleep(0.5)
        
    return all_videos
    
    
def get_channel_details (channel_ids):
    
    """ FETCHES VIDEO DETAILS FOR UP TO 50 IDS.
        RETURNS A LIST OF RAW VIDEO DETAILS.
    """
    if not channel_ids:
        return []

    all_channels = []
    
    for batch in chunk_list(channel_ids):
        params = {
            "part":"snippet,statistics",
            "id": "," .join(batch),
            "key":YOUTUBE_API_KEY   
        }
                  

        try:
            response = session.get(f"{BASE_URL}/channels", params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Channel detail request failed for batch of %d: %s", len(batch), type(e).__name__)
            continue

        data = response.json()
        items = data.get("items", [])
        all_channels.extend(items)
        
    logger.info(f"Results for {len(all_channels)} channels: total = {len(all_channels)}")
    
    sleep(0.5)
    
    return all_channels
    

def main ():
    if not YOUTUBE_API_KEY:
        raise EnvironmentError("YOUTUBE_API_KEY is not set")

    logger.info(f"Starting Ingestion run for {ingestion_date}")

    all_search_results = []

    for term in SEARCH_TERMS:
        all_search_results.extend(search_videos(term))

    video_ids = list({
        item.get("id", {}).get("videoId")
        for item in all_search_results
        if item.get("id", {}).get("videoId")
    })
    if not video_ids:
        logger.warning("No videos found for any search term")
        yield [], []
        return

    logger.info(f"Discovered {len(video_ids)} unique videos")
    videos = get_video_details(video_ids)

    channel_ids = list({
        video.get("snippet", {}).get("channelId")
        for video in videos
        if video.get("snippet", {}).get("channelId")
    })
    logger.info(f"Discovered {len(channel_ids)} unique channels")
    channels = get_channel_details(channel_ids)
    yield videos, channels

if __name__ == "__main__":
    configure_logging()
    main()