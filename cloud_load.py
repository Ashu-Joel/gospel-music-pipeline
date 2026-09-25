""" LOADS INGESTED DATA FROM `youtube_ingestion.py` INTO MY DATA WAREHOUSE IN MOTHERDUCK USING DLT"""


from youtube_ingestion import configure_logging, main
import dlt
import logging
from datetime import datetime

load_date = datetime.now().strftime("%d-%m-%Y")

logger = logging.getLogger(__name__)

logger.info(f"Starting Pipeline for {load_date}")

def load_data():
    load_info = None
    pipeline = dlt.pipeline(
        pipeline_name= "youtube_gospel_music_loader",
        destination="motherduck",
        dataset_name="raw_data",
    )
    for videos, channels in main():
        if not videos and not channels:
            logger.warning("No videos or channels were returned; skipping dlt load")
            return None

        logger.info(f"Loaded {len(videos)} videos and {len(channels)} channels for {load_date}")
        load_info = pipeline.run([
            dlt.resource(videos, name="raw_videos", write_disposition="merge", primary_key="id"),
            dlt.resource(channels, name="raw_channels", write_disposition="merge", primary_key="id")
        ])

    if load_info is None:
        logger.warning("Ingestion produced no load batch")
        return None

    logger.info(f"dlt load for {load_date} completed: {load_info} ")
    return load_info

if __name__ == '__main__':
    configure_logging()
    load_data()