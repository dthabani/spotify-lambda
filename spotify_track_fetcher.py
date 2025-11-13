import os
import logging
import boto3
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth
from pymongo import MongoClient
from datetime import datetime
import pytz

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

client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
db = client['spotify']
collection = db['songs']

def send_sns_notification(subject, message):
    """Send an SNS notification."""
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
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-recently-played"
    )

    if not REFRESH_TOKEN:
        raise Exception("Refresh token not found. Please authenticate locally and store the refresh token.")

    token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
    access_token = token_info.get('access_token')

    if not access_token:
        raise Exception("Failed to refresh access token. Please re-authenticate.")

    return Spotify(auth=access_token)

def fetch_recent_tracks():
    logger.info("Fetching recently played tracks...")
    try:
        sp = get_spotify_client()
        recent_tracks = sp.current_user_recently_played(limit=50)
        gmt_plus_8 = pytz.timezone('Asia/Singapore')

        for item in recent_tracks['items']:
            track = item['track']
            try:
                utc_time = datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
            except ValueError:
                # Handle timestamps without fractional seconds
                utc_time = datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
            local_time = utc_time.astimezone(gmt_plus_8)
            duration_ms = track['duration_ms']
            minutes = duration_ms // 60000
            seconds = (duration_ms % 60000) // 1000
            duration_formatted = f"{minutes:02}:{seconds:02}"

            data = {
                'artist': track['artists'][0]['name'],
                'title': track['name'],
                'played_at': local_time.strftime("%Y-%m-%d %H:%M:%S"),
                'duration': duration_formatted,
                'album': track['album']['name'],
            }
            logger.info(f"Inserting data: {data}")
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