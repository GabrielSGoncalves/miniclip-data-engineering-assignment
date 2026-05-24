import json
from pathlib import Path

import jsonschema
import pytest

from generators import generate_in_app_purchase, generate_init, generate_match

SCHEMAS_DIR = Path(__file__).parents[2] / "schemas"


def _load_schema(name: str) -> dict:
    return json.loads((SCHEMAS_DIR / f"{name}.json").read_text())


def test_init_validates():
    jsonschema.validate(generate_init("user_1"), _load_schema("init"))


def test_match_validates():
    jsonschema.validate(generate_match("user_1", "user_2"), _load_schema("match"))


def test_match_without_user_b_postmatch_validates():
    event = generate_match("user_1", "user_2")
    event.pop("user-b-postmatch-info", None)
    jsonschema.validate(event, _load_schema("match"))


def test_in_app_purchase_validates():
    jsonschema.validate(generate_in_app_purchase("user_1"), _load_schema("in-app-purchase"))


def test_init_user_id_is_string():
    assert isinstance(generate_init("user_1")["user-id"], str)


def test_match_winner_is_one_of_the_players():
    event = generate_match("alice", "bob")
    assert event["winner"] in ("alice", "bob")


def test_game_tier_minimum():
    # Schema has duplicate "minimum" keys on game-tier; last value (5) wins in most parsers
    for _ in range(50):
        assert generate_match("a", "b")["game-tier"] >= 5
