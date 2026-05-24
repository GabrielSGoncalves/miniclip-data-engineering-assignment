import textwrap
import tempfile
import os

import pytest

from rules.uppercase import UppercaseRule
from rules.map_id import MapIdRule
from transformer import load_rules

INIT_EVENT = {"event-type": "init", "platform": "ios", "country": "PT", "user-id": "user_1"}
MATCH_EVENT = {"event-type": "match", "platform": "android", "user-a": "user_1", "user-b": "user_2"}
PURCHASE_EVENT = {"event-type": "in-app-purchase", "user-id": "user_1", "purchase_value": 4.99}

COUNTRY_MAPPING = {"PT": "Portugal", "US": "United States", "BR": "Brazil"}


# --- UppercaseRule ---

def test_uppercase_transforms_matching_event():
    rule = UppercaseRule(field="platform", event_types=["init"])
    result = rule.apply(INIT_EVENT)
    assert result["platform"] == "IOS"


def test_uppercase_does_not_mutate_original():
    rule = UppercaseRule(field="platform", event_types=["init"])
    original = {**INIT_EVENT}
    rule.apply(original)
    assert original["platform"] == "ios"


def test_uppercase_skips_non_matching_event_type():
    rule = UppercaseRule(field="platform", event_types=["init"])
    assert not rule.applies_to(MATCH_EVENT)


def test_uppercase_noop_when_field_absent():
    rule = UppercaseRule(field="platform", event_types=["init"])
    event = {"event-type": "init", "country": "PT"}
    assert rule.apply(event) == event


def test_uppercase_applies_to_all_when_event_types_empty():
    rule = UppercaseRule(field="platform", event_types=[])
    assert rule.applies_to(INIT_EVENT)
    assert rule.applies_to(MATCH_EVENT)


# --- MapIdRule ---

def test_map_id_replaces_known_value():
    rule = MapIdRule(field="country", event_types=["init"], mapping=COUNTRY_MAPPING)
    result = rule.apply(INIT_EVENT)
    assert result["country"] == "Portugal"


def test_map_id_leaves_unknown_value_unchanged():
    rule = MapIdRule(field="country", event_types=["init"], mapping=COUNTRY_MAPPING)
    event = {**INIT_EVENT, "country": "JP"}
    result = rule.apply(event)
    assert result["country"] == "JP"


def test_map_id_noop_when_field_absent():
    rule = MapIdRule(field="country", event_types=["init"], mapping=COUNTRY_MAPPING)
    event = {"event-type": "init", "platform": "ios"}
    assert rule.apply(event) == event


def test_map_id_handles_integer_key():
    rule = MapIdRule(field="country", event_types=["init"], mapping={"1": "Portugal"})
    event = {"event-type": "init", "country": 1}
    assert rule.apply(event)["country"] == "Portugal"


# --- load_rules ---

def test_load_rules_from_yaml():
    yaml_content = textwrap.dedent("""\
        rules:
          - type: uppercase
            field: platform
            event_types: [init]
          - type: map_id
            field: country
            event_types: [init]
            mapping:
              PT: Portugal
    """)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        path = f.name

    try:
        rules = load_rules(path)
        assert len(rules) == 2
        assert isinstance(rules[0], UppercaseRule)
        assert isinstance(rules[1], MapIdRule)
    finally:
        os.unlink(path)
