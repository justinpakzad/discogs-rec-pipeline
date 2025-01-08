import uuid
import json
import random
from faker import Faker
import datetime as dt
from countries_and_cities import location_data
import polars as pl
import boto3
import logging
from io import StringIO

fake = Faker()


def get_release_ids(s3, bucket, file):
    try:
        response = s3.get_object(Bucket=bucket, Key=file)
        file_content = response["Body"].read().decode("utf-8")
        df = pl.read_csv(StringIO(file_content))
        return df

    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing data: {str(e)}"),
        }


def write_to_s3(s3, bucket, file, data):
    try:
        s3.put_object(Bucket=bucket, Key=file, Body=json.dumps(data))
        return {"statusCode": 200, "body": json.dumps("Data sucessfully pushed to s3")}
    except Exception as e:
        logging.error(f"Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing data: {str(e)}"),
        }


def generate_user_profile(location_data):
    username = fake.user_name()
    device_type = random.choice(["laptop", "desktop", "mobile"])
    profile = {
        "username": username,
        "email": f'{username}@{random.choice(["gmail.com","hotmail.com","yahoo.com","outlook.com"])}',
        "user_id": str(uuid.uuid4()),
        "location": (
            random.choice(location_data)
            if random.choices([True, False], weights=[0.9, 0.1])
            else {}
        ),
        "device": {
            "type": random.choice(["laptop", "desktop", "mobile"]),
            "os": (
                random.choice(["Windows", "macOS", "Linux"])
                if device_type in ["laptop", "desktop"]
                else random.choice(["Android", "iOS"])
            ),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        },
        "age": (
            str(random.randint(21, 45))
            if random.choices([True, False], weights=[0.8, 0.2])
            else random.randint(21, 45)
        ),
    }
    return profile


def generate_corrupted_data(data):
    choice = random.choices([True, False], weights=[0.25, 0.75], k=1)[0]
    if choice:
        key = random.choice(list(data.keys()))
        if not isinstance(data.get(key), dict) and key.lower().strip() not in [
            "interaction_timestamp",
            "input_url",
        ]:
            if random.choice([True, False]):
                data[key] = None
            else:
                data[key] = "N/A" if random.random() >= 0.5 else "None"
    return data


def generate_duplicates(data_list):
    num_dups = int(len(data_list) * random.uniform(0.025, 0.1))
    duplicates = random.choices(data_list, k=num_dups)
    data_list.extend(duplicates)
    random.shuffle(data_list)
    return data_list


def generate_user_feedback(release_ids):
    purchased = random.choice(["yes", "no"])
    added_to_wantlist = random.choice(["yes", "no"])
    session_duration = random.randint(10, 600)
    shared_with_friends = random.choice(["yes", "no"])
    user_feedback = random.choice(
        ["like", "dislike", "neutral", "strong like", "strong dislike"]
    )
    date_formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%SZ"]

    feedback = {
        "feedback": user_feedback,
        "input_url": (f"https://www.discogs.com/release/{random.choice(release_ids)}"),
        "recommendation_rank": (
            random.randint(0, 2)
            if user_feedback in ["dislike", "strong dislike"]
            else random.randint(3, 5)
        ),
        "would_recommend_to_friend_rank": (
            random.randint(3, 5)
            if shared_with_friends == "yes"
            else random.randint(0, 2)
        ),
        "session": {
            "session_id": str(uuid.uuid4()),
            "session_duration": session_duration,
            "n_clicks": (
                random.randint(0, 2) if session_duration <= 20 else random.randint(3, 5)
            ),
        },
        "purchased": {
            "status": purchased,
            "count": random.randint(1, 5) if purchased == "yes" else 0,
        },
        "added_to_wantlist": {
            "status": added_to_wantlist,
            "count": random.randint(1, 5) if added_to_wantlist == "yes" else 0,
        },
        "familiarity": random.choices(["new", "known"], weights=[0.7, 0.3], k=1)[0],
        "shared_with_friends": shared_with_friends,
        "recommendation_relevance": random.choices(
            ["to_similar", "to_different", "balanced"], weights=[0.15, 0.15, 0.7], k=1
        )[0],
        "interaction_timestamp": dt.datetime.now().strftime(
            random.choice(date_formats)
        ),
    }
    return feedback


def generate_feedback_and_profiles(n_users, release_ids, location_data):
    users_feedback = []
    for _ in range(n_users):
        user = generate_user_profile(location_data)
        feedback = generate_user_feedback(release_ids)
        if random.choices([True, False], weights=[0.25, 0.75], k=1)[0]:
            user = generate_corrupted_data(user)
            feedback = generate_corrupted_data(feedback)
        users_feedback.append({**user, **feedback})
    users_feedback = generate_duplicates(users_feedback)
    return users_feedback


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket = "discogs-recommender"
    date_folder = dt.datetime.now().strftime("%Y%m%d%H")
    df = get_release_ids(s3=s3, bucket=bucket, file="release_metadata/release_ids.csv")
    release_ids = df["release_id"].to_list()
    users_feedback = generate_feedback_and_profiles(
        n_users=random.randint(50000, 80000),
        release_ids=release_ids,
        location_data=location_data,
    )

    write_to_s3(
        s3=s3,
        bucket=bucket,
        file=f"logs/{date_folder}/feedback_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        data=users_feedback,
    )
