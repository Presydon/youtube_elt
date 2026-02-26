import os
import requests
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv('YOUTUBE_API_KEY')
CHANNEL_HANDLER = "@MrBeast"

URL = f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLER}&key={API_KEY}'

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

if __name__ == "__main__":
    playlist_id = get_playlist_id(URL)
    print(playlist_id)