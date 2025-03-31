import sys
from pathlib import Path
import pytest
from datetime import datetime

sys.path.append(
    str(Path(__file__).parents[1] / "src" / "lambdas" / "generate_feedback")
)
from lambda_function import (  # type: ignore
    generate_user_feedback,
    generate_user_profile,
    generate_feedback_and_profiles,
)


@pytest.fixture
def location_data():
    return [
        {"country": "Germany", "city": "Berlin"},
        {"country": "France", "city": "Paris"},
    ]


@pytest.fixture
def release_ids():
    return [5555573, 1045596, 1045593]


@pytest.fixture
def expected_profile_keys():
    return {"username", "user_id", "location", "device", "age", "email"}


@pytest.fixture
def user_profile(location_data):
    """Fixture to generate a single user profile."""
    return generate_user_profile(location_data)


@pytest.fixture
def user_feedback(release_ids):
    """Fixture to generate user feedback."""
    return generate_user_feedback(release_ids)


@pytest.fixture
def expected_feedback_keys():
    return {
        "feedback",
        "input_url",
        "recommendation_rank",
        "would_recommend_to_friend_rank",
        "session",
        "purchased",
        "added_to_wantlist",
        "familiarity",
        "shared_with_friends",
        "recommendation_relevance",
        "interaction_timestamp",
    }


class TestUserProfileGenerator:
    def test_user_profile_keys(self, user_profile, expected_profile_keys):
        assert set(user_profile.keys()) == expected_profile_keys

    def test_user_profile_email(self, user_profile):
        assert user_profile.get("email").split("@")[-1] in [
            "gmail.com",
            "hotmail.com",
            "yahoo.com",
            "outlook.com",
        ]

    def test_user_profile_device(self, user_profile):
        assert isinstance(user_profile.get("device"), dict)
        assert set(user_profile.get("device").keys()) == {"type", "os", "browser"}

    def test_user_profile_numeric(self, user_profile):
        assert len(user_profile.get("user_id")) == 36


class TestUserFeedbackGenerator:
    def test_user_feedback_keys(self, user_feedback, expected_feedback_keys):
        assert set(user_feedback.keys()) == expected_feedback_keys

    def test_user_feedback_text(self, user_feedback):
        assert user_feedback.get("feedback") in [
            "like",
            "dislike",
            "neutral",
            "strong like",
            "strong dislike",
        ]
        assert user_feedback.get("recommendation_relevance") in [
            "to_similar",
            "to_different",
            "balanced",
        ]
        assert user_feedback.get("input_url").startswith("https://www.discogs.com")
        assert user_feedback.get("familiartiy") in ["new", "known"]

    def test_user_feedback_session(self, user_feedback):
        session = user_feedback.get("session")
        assert set(session.keys()) == {"session_id", "session_duration", "n_clicks"}
        assert 10 <= session.get("session_duration") <= 600
        assert (
            0 <= session.get("n_clicks") <= 2
            if session.get("session_duration") <= 20
            else 3 <= session.get("n_clicks") <= 5
        )

    def test_user_feedback_ranks(self, user_feedback):
        assert (
            0 <= user_feedback.get("recommendation_rank") <= 2
            if user_feedback.get("feedback") in ["dislike", "strong dislike"]
            else 3 <= user_feedback.get("recommendation_rank") <= 5
        )
        assert (
            0 <= user_feedback.get("would_recommend_to_friend_rank") <= 2
            if user_feedback.get("shared_with_friends") in ["no"]
            else 3 <= user_feedback.get("would_recommend_to_friend_rank") <= 5
        )

    def test_user_feedback_wantlist(self, user_feedback):
        added_to_wantlist = user_feedback.get("added_to_wantlist")
        assert set(added_to_wantlist.keys()) == {"status", "count"}
        assert added_to_wantlist.get("status") in ["yes", "no"]
        assert (
            added_to_wantlist.get("count") == 0
            if added_to_wantlist.get("status") in ["no"]
            else 1 <= added_to_wantlist.get("count") <= 5
        )

    def test_user_feedback_wants_and_purchases(self, user_feedback):
        purchased = user_feedback.get("purchased")
        assert set(purchased.keys()) == {"status", "count"}
        assert purchased.get("status") in ["yes", "no"]
        assert (
            purchased.get("count") == 0
            if purchased.get("status") in ["no"]
            else 1 <= purchased.get("count") <= 5
        )


class TestFeedbackProfilesGenerator:
    def gerate_feedback_and_profiles(self, release_ids, location_data, n_users=100):
        users_feedback = generate_feedback_and_profiles(
            n_users, release_ids, location_data
        )
        assert len(users_feedback) == n_users
