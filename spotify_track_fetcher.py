import os
import logging
import boto3
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from pymongo import MongoClient
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Client
sns_client = boto3.client('sns')

# Spotify API and MongoDB settings from environment variables
CLIENT_ID = os.environ['SPOTIFY_CLIENT_ID']
CLIENT_SECRET = os.environ['SPOTIFY_CLIENT_SECRET']
REDIRECT_URI = os.environ['SPOTIFY_REDIRECT_URI']
REFRESH_TOKEN = os.environ.get('SPOTIFY_REFRESH_TOKEN')
MONGO_URI = os.environ['MONGO_URI']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# MongoDB Connection
client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
db = client['spotify']
collection = db['songs']

def send_sns_notification(subject, message):
    """Send an SNS notification on success or failure."""
    try:
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"SNS notification sent: {response['MessageId']}")
    except Exception as e:
        logger.error(f"Error sending SNS notification: {str(e)}")

def get_spotify_client():
    """Authenticate with Spotify using the stored Refresh Token."""
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played"
    )

    if not REFRESH_TOKEN:
        raise Exception("Refresh token not found in environment variables.")

    token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
    access_token = token_info.get('access_token')

    if not access_token:
        raise Exception("Failed to refresh access token. Please re-authenticate.")

    return Spotify(auth=access_token)

def fetch_recent_tracks():
    """Fetch tracks, calculate true play times, and load into MongoDB."""
    logger.info("Fetching recently played tracks...")
    try:
        sp = get_spotify_client()
        recent_tracks = sp.current_user_recently_played(limit=50)
        items = recent_tracks.get('items', [])
        
        if not items:
            logger.info("No tracks found.")
            return

        for item in items:
            played_at_str = item['played_at']
            # Handle timestamps with or without fractional seconds
            try:
                dt = datetime.strptime(played_at_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                dt = datetime.strptime(played_at_str, "%Y-%m-%dT%H:%M:%SZ")
            
            # Force UTC awareness
            item['utc_time'] = dt.replace(tzinfo=timezone.utc)

        for i, item in enumerate(items):
            track = item['track']
            
            duration_seconds = track['duration_ms'] // 1000
            time_taken = duration_seconds
            
            # If there is a track played BEFORE this one (index i + 1), calculate the difference
            if i < len(items) - 1:
                older_item = items[i + 1]
                time_diff = item['utc_time'] - older_item['utc_time']
                diff_seconds = int(time_diff.total_seconds())
                
                # Time taken is the minimum of the track's full length or the time before the next skip
                time_taken = min(duration_seconds, diff_seconds)

            # Extract image and artist IDs
            images = track['album'].get('images', [])
            album_cover = images[1]['url'] if len(images) > 1 else (images[0]['url'] if images else None)
            artist_data = [{'name': a['name'], 'id': a['id']} for a in track['artists']]

            data = {
                'track_id': track['id'],
                'title': track['name'],
                'artists': [a['name'] for a in artist_data],
                'artist_ids': [a['id'] for a in artist_data],
                'album': track['album']['name'],
                'album_id': track['album']['id'],
                'album_cover_url': album_cover,
                'played_at': item['utc_time'],
                'duration': duration_seconds,
                'time_taken': time_taken
            }
            
            logger.info(f"Inserting: {data['title']} | Listened for {time_taken} seconds out of {duration_seconds} seconds")
            
            # Upsert into MongoDB using the absolute UTC datetime as the unique identifier
            collection.update_one({'played_at': data['played_at']}, {'$set': data}, upsert=True)

        logger.info("Recently played tracks successfully stored in MongoDB!")
        send_sns_notification(
            subject="Spotify Data Fetch Successful",
            message="Recently played tracks have been fetched and stored in MongoDB successfully."
        )

    except Exception as e:
        logger.error(f"Error fetching Spotify data: {str(e)}")
        send_sns_notification(
            subject="Spotify Data Fetch Failed",
            message=f"An error occurred: {str(e)}"
        )
        raise

def lambda_handler(event, context):
    try:
        fetch_recent_tracks()
        return {"statusCode": 200, "body": "Spotify recently played tracks fetched and stored."}
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"statusCode": 500, "body": str(e)}