import os
import time
from datetime import datetime

import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from pymongo import MongoClient, UpdateOne

# Environment variables
MONGO_URI = os.environ["MONGO_URI"]
CLIENT_ID = os.environ["SPOTIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SPOTIFY_CLIENT_SECRET"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
DB_NAME = os.environ.get("DB_NAME")

sns_client = boto3.client("sns")

def send_sns(subject, summary):
    """Send a simple plain-text SNS notification with the sync results."""
    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN not set, skipping SNS notification.")
        return

    # If the summary is an error string, just send the string.
    if isinstance(summary, str):
        message = summary
    else:
        message = (
            "Spotify Artist Sync Report\n\n"
            f"Status: Artist profiles check completed\n\n"
            f"Total Unique Artists in History: {summary['total_unique']}\n"
            f"Already in Database: {summary['existing']}\n"
            f"Missing Artists Detected: {summary['missing']}\n"
            f"Successfully Fetched & Saved: {summary['updated']}\n"
            f"Errors/Failed Fetches: {summary['errors']}\n"
            f"Elapsed Time: {summary['elapsed']}\n\n"
        )

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print("SNS text notification sent.")
    except Exception as e:
        print(f"Failed to send SNS notification: {e}")

def get_mongo_client():
    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    db = client[DB_NAME]
    return db, client

def chunker(seq, size):
    """Helper function to split a list into chunks."""
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def sync_artist_profiles(db):
    """Core logic to fetch and update missing artist profiles."""
    start_time = time.time()
    
    tracks_coll = db["songs"]
    artists_coll = db["artists"]

    print("Scanning listening history for unique Artist IDs...")
    raw_artist_ids = tracks_coll.distinct("artist_ids")
    all_artist_ids = [aid for aid in raw_artist_ids if aid]
    
    print("Checking database for existing artists...")
    existing_artists = artists_coll.find({}, {"_id": 1})
    existing_ids = {doc["_id"] for doc in existing_artists}
    
    missing_ids = [aid for aid in all_artist_ids if aid not in existing_ids]
    
    summary = {
        "total_unique": len(all_artist_ids),
        "existing": len(existing_ids),
        "missing": len(missing_ids),
        "updated": 0,
        "errors": 0,
        "elapsed": "00:00"
    }

    print(f"Total unique: {summary['total_unique']} | Existing: {summary['existing']} | Missing: {summary['missing']}")

    if not missing_ids:
        print("All artist profiles are up to date! Exiting early.")
        elapsed = time.time() - start_time
        m, s = divmod(int(elapsed), 60)
        summary["elapsed"] = f"{m:02}:{s:02}"
        return summary

    print("Authenticating with Spotify...")
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET),
        requests_timeout=10,
        retries=10,
        status_retries=10,
        backoff_factor=0.5
    )

    batches = list(chunker(missing_ids, 50))
    total_batches = len(batches)
    print(f"Fetching {len(missing_ids)} artists in {total_batches} batches...")

    for i, batch in enumerate(batches, 1):
        try:
            response = sp.artists(artists=batch)
            operations = []
            
            for artist in response['artists']:
                if not artist:
                    continue
                    
                artist_id = artist['id']
                artist_name = artist['name']
                images = artist.get('images', [])
                image_url = images[1]['url'] if len(images) > 1 else (images[0]['url'] if images else None)
                
                operations.append(UpdateOne(
                    {"_id": artist_id},
                    {"$set": {
                        "name": artist_name,
                        "profile_image_url": image_url
                    }},
                    upsert=True
                ))
            
            if operations:
                artists_coll.bulk_write(operations)
                summary["updated"] += len(operations)
            
            print(f"Batch {i}/{total_batches} Complete: Fetched & Saved {len(operations)} artists.")
            time.sleep(0.1) # Safety sleep
            
        except Exception as e:
            print(f"Error fetching Batch {i}: {str(e)}")
            summary["errors"] += len(batch)

    elapsed = time.time() - start_time
    m, s = divmod(int(elapsed), 60)
    summary["elapsed"] = f"{m:02}:{s:02}"

    return summary

def lambda_handler(event, context):
    """Main entry point for AWS Lambda."""
    print("Starting Artist Profile Sync...")

    db, client = get_mongo_client()

    try:
        summary = sync_artist_profiles(db)

        # Send Success SNS
        send_sns("Spotify Artist Sync Successful", summary)

        return {
            "statusCode": 200,
            "body": {
                "message": "Artist profile sync completed successfully.",
                "summary": summary
            }
        }

    except Exception as e:
        error_message = f"Artist Sync failed: {e}"
        print(error_message)
        # Send Failure SNS
        send_sns("Spotify Artist Sync FAILED", error_message)
        raise

    finally:
        client.close()
        print("MongoDB connection closed.")