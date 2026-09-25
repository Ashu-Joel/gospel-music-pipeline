import duckdb
import dlt

from youtube_ingestion import get_video_details, get_channel_details

connection = duckdb.connect("md:my_db")

video_ids = [
    row[0]
    for row in connection.execute(
        "SELECT id FROM raw_data.raw_videos WHERE id IS NOT NULL"
    ).fetchall()
]

videos = get_video_details(video_ids)

channel_ids = list({
    video["snippet"]["channelId"]
    for video in videos
    if video.get("snippet", {}).get("channelId")
})

channels = get_channel_details(channel_ids)

print(f"Updated {len(videos)} videos")
print(f"Updated {len(channels)} channels")



pipeline = dlt.pipeline(
    pipeline_name="warehouse_update",
    destination="motherduck",
    dataset_name="raw_data",
)

pipeline.run([
    dlt.resource(
        videos,
        name="raw_videos",
        write_disposition="merge",
        primary_key="id",
    ),
    dlt.resource(
        channels,
        name="raw_channels",
        write_disposition="merge",
        primary_key="id",
    ),
])