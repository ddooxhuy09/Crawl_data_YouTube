from googleapiclient.discovery import build
import pandas as pd
import requests
import json
from datetime import timedelta, datetime
import boto3
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_video_id(youtube, channel_id):
    videos_response = youtube.search().list(
    part='id',
    channelId=channel_id,
    order='date',
    type='video',
    maxResults=50
    ).execute()

    video_ids = [item['id']['videoId'] for item in videos_response['items']]

    while 'nextPageToken' in videos_response:
        next_page_token = videos_response['nextPageToken']
        videos_response = youtube.search().list(
            part='id',
            channelId=channel_id,
            order='date',
            type='video',
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        video_ids.extend(item['id']['videoId'] for item in videos_response['items'])

    return video_ids

def transform_data(df, api_key, video_ids):
    for i, video_id in enumerate(video_ids):
        url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&key={api_key}&id={video_id}"
        data = json.loads(requests.get(url).text)

        try:
            channel_id = data["items"][0]["snippet"]["channelId"]
        except KeyError:
            channel_id = None

        try:
            published_date = data["items"][0]["snippet"]["publishedAt"]
        except KeyError:
            published_date = None

        try:
            title = data["items"][0]["snippet"]["title"]
        except KeyError:
            title = None

        try:
            description = data["items"][0]["snippet"]["description"]
        except KeyError:
            description = None

        try:
            view = data["items"][0]["statistics"]["viewCount"]
        except KeyError:
            view = None

        try:
            like = data["items"][0]["statistics"]["likeCount"]
        except KeyError:
            like = None

        try:
            comment_count = data["items"][0]["statistics"]["commentCount"]
        except KeyError:
            comment_count = None

        row = [video_id, channel_id, published_date, title, description, view, like, comment_count]

        df.loc[i] = row

    return df

def main():

    api_key = "AIzaSyDcxGAcw8EJPS7FzPYAoo0JUEkm42KxiTQ"
    channel_id = "UCA_23dkEYToAc37hjSsCnXA"

    youtube = build('youtube', 'v3', developerKey=api_key)

    video_ids = get_video_id(youtube, channel_id)

    df = pd.DataFrame(columns = ['video_id', 'channel_id', 'published_date', 'title', 'description', 'views', 'likes', 'comment_count'])

    data = transform_data(df, api_key, video_ids)

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)


    aws_access_key_id = 'AKIAXLOEF7XPQ74PVODY'
    aws_secret_access_key = '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'

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'do_mixi' + dt_string

    bucket_name = 'youtube-do-mixi'
    file_key = f'{dt_string}.csv'

    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_key)

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 11, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='crawl_data_youtube',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    crawl_data = PythonOperator(
        task_id='do_mixi',
        python_callable=main
    )