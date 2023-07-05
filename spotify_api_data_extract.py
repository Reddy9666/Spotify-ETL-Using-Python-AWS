import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):

#This is data extraction part

    client_id= os.environ.get('client_id')
    client_secret= os.environ.get('client_secret')
    client_credentials_manager = SpotifyClientCredentials(client_id="a4ecae3c968b4626a6daafe27f1500ce", client_secret="c8496732a3a2487db0f27f711350a7e2")
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotipy')
    
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_uri =playlist_link.split("/")[-1]
    spotify_data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')
    
    filename = "spotify_raw"+ str(datetime.now())+".json"
    
    client.put_object(
        Bucket ="spotify-etl-project-reddy",
        Key = "raw_data/to_processed/"+ filename,
        Body= json.dumps(spotify_data)
        )
