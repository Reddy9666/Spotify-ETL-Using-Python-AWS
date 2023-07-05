import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):
    album_data=[]
    for i in data['items']:
        album_id = i['track']['album']['id']
        name = i['track']['album']['name']
        release_date = i['track']['album']['release_date']
        total_tracks = i['track']['album']['total_tracks']
        url = i['track']['album']['external_urls']['spotify']
        album_elements = {"album_id" : album_id,'name':name,'release_date':release_date,
                            'total_tracks':total_tracks,'url':url}
        album_data.append(album_elements)
    return album_data

def artist(data):
    artist_data = []
    for j in data['items']:
        for key,value in j.items():
            if key == "track":
                for k in value["artists"]:
                    artist_dic = {'artist_id': k['id'],'artist_name':k['name'], 'external_url': k['href']}
                    artist_data.append(artist_dic)
    return artist_data

def song(data):
    song_data= []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row ['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_data.append(song_element)
    return song_data


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = "spotify-etl-project-reddy"
    Key = "raw_data/to_processed/"
    spotify_data = []
    spotify_key=[]
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file["Key"]
        if file_key.split(".")[-1]=="json":
            reponse = s3.get_object(Bucket= Bucket,Key= file_key)
            content = reponse['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)
    
    for data in spotify_data:
        album_data = album(data)
        artist_data =artist(data)
        song_data = song(data)
        
        album_df = pd.DataFrame.from_dict(album_data)
        song_df = pd.DataFrame.from_dict(song_data)
        artist_df = pd.DataFrame.from_dict(artist_data)
        album_df = album_df.drop_duplicates(subset = ['album_id'])
        artist_df = artist_df.drop_duplicates(subset = ['artist_id'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        song_key ="transformed_data/song_data/song_transformed_"+str(datetime.now())+".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket= Bucket,Key = song_key,Body = song_content)
        
        
        album_key ="transformed_data/album_data/album_transformed_"+str(datetime.now())+".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer,index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket= Bucket,Key = album_key,Body = album_content)
        
        
        artist_key ="transformed_data/artist_data/artist_transformed_"+str(datetime.now())+".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer,index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket= Bucket,Key = artist_key,Body = artist_content)
        
        
    s3_resource = boto3.resource('s3')
    for key in spotify_key:
        copy_source ={
            'Bucket':Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete() 