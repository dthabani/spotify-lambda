# Calculates and updates the 'time_taken' field for Spotify tracks in a MongoDB collection.

import os
import time
import json
from datetime import datetime

import boto3
from pymongo import MongoClient

# Environment variables
MONGO_URI = os.environ["MONGO_URI"]
DB_NAME = os.environ.get("DB_NAME", "spotify")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "songs")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

sns_client = boto3.client("sns")


def send_sns(subject, summary):
    """Send a simple plain-text SNS notification."""

    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN not set, skipping SNS notification.")
        return

    message = (
        "Spotify Time Taken Report\n\n"
        f"Status: time_taken calculation completed\n\n"
        f"Total Docs: {summary['total_docs']}\n"
        f"Updated: {summary['updated']}\n"
        f"Skipped: {summary['skipped']}\n"
        f"Errors: {summary['errors']}\n"
        f"Elapsed: {summary['elapsed']}\n\n"
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

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME], client


def calculate_time_taken_for_collection(collection):
    start_time = time.time()

    print("Fetching and sorting data...")
    data = list(collection.find().sort("played_at", -1))
    total_docs = len(data)

    updated_count = 0
    skipped_count = 0
    error_count = 0

    if not data:
        return {
            "total_docs": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "elapsed": "00:00"
        }

    print("Processing documents...")

    for i, song in enumerate(data):
        update_fields = {}

        if "time_taken" in song:
            skipped_count += 1
            continue

        try:
            if i < len(data) - 1:
                played_at_current = datetime.strptime(song["played_at"], "%Y-%m-%d %H:%M:%S")
                played_at_next = datetime.strptime(data[i + 1]["played_at"], "%Y-%m-%d %H:%M:%S")
                diff = (played_at_current - played_at_next).total_seconds()

                minutes, seconds = song["duration"].split(":")
                duration_total = int(minutes) * 60 + int(seconds)

                actual = min(diff, duration_total)
                time_taken_str = f"{int(actual // 60)}:{str(int(actual % 60)).zfill(2)}"
                update_fields["time_taken"] = time_taken_str
            else:
                update_fields["time_taken"] = song["duration"]

            collection.update_one({"_id": song["_id"]}, {"$set": update_fields})
            updated_count += 1

        except Exception as e:
            print(f"Error: {e}")
            error_count += 1

    elapsed = time.time() - start_time
    m, s = divmod(int(elapsed), 60)
    elapsed_str = f"{m:02}:{s:02}"

    return {
        "total_docs": total_docs,
        "updated": updated_count,
        "skipped": skipped_count,
        "errors": error_count,
        "elapsed": elapsed_str
    }


def lambda_handler(event, context):
    print("Starting time_taken updater...")

    collection, client = get_mongo_collection()

    try:
        summary = calculate_time_taken_for_collection(collection)

        message = (
            f"time_taken calculation completed\n\n"
            f"Total docs: {summary['total_docs']}\n"
            f"Updated: {summary['updated']}\n"
            f"Skipped: {summary['skipped']}\n"
            f"Errors: {summary['errors']}\n"
            f"Elapsed: {summary['elapsed']}"
        )

        send_sns("Spotify Time Taken Updater", summary)

        return {
            "statusCode": 200,
            "body": {
                "message": "time_taken calculation completed",
                "summary": summary
            }
        }

    except Exception as e:
        error_message = f"Updater failed: {e}"
        send_sns("Spotify Time Taken Updater FAILED", error_message)
        print(error_message)
        raise

    finally:
        client.close()