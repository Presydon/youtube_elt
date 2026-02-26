import os
import requests
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv('YOUTUBE_API_KEY')
CHANNEL_HANDLER = "@MrBeast"
MAX_RESULTS = 50


PLAYLIST_URL = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={API_KEY}'


def get_playlist_id(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        channel_items = data['items'][0]
        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id
    
    except requests.exceptions.RequestException as e:
        raise e



def get_video_ids(playlist_id):
    video_ids = []
    page_token = None

    
    try: 
        while True:
            url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={API_KEY}"
            if page_token:
                url += f'&pageToken={page_token}'

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            page_token = data.get("nextPageToken")

            if not page_token:
                break

    except requests.exceptions.RequestException as e:
        raise e

    return video_ids

if __name__ == "__main__":
    playlist_id = get_playlist_id(PLAYLIST_URL)
    video_ids = get_video_ids(playlist_id)