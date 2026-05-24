import random
import time
from faker import Faker

fake = Faker()

PLATFORMS = ["ios", "android"]
PRODUCTS = ["coins_100", "coins_500", "coins_1000", "vip_pass_30d"]
COUNTRY_CODES = ["PT", "US", "GB", "DE", "BR", "FR", "ES", "IT", "CA", "AU"]


def generate_init(user_id: str) -> dict:
    return {
        "event-type": "init",
        "time": int(time.time()),
        "user-id": user_id,
        "country": random.choice(COUNTRY_CODES),
        "platform": random.choice(PLATFORMS),
    }


def generate_match(user_a: str, user_b: str) -> dict:
    def postmatch_info() -> dict:
        return {
            "coin-balance-after-match": random.randint(0, 10_000),
            "level-after-match": random.randint(1, 100),
            "device": fake.user_agent()[:200],
            "platform": random.choice(PLATFORMS),
        }

    event = {
        "event-type": "match",
        "time": int(time.time()),
        "user-a": user_a,
        "user-b": user_b,
        "user-a-postmatch-info": postmatch_info(),
        "winner": random.choice([user_a, user_b]),
        "game-tier": random.randint(5, 10),
        "duration": random.randint(30, 300),
    }
    # user-b-postmatch-info is optional — absent ~20% of the time (e.g. bot opponent)
    if random.random() > 0.2:
        event["user-b-postmatch-info"] = postmatch_info()
    return event


def generate_in_app_purchase(user_id: str) -> dict:
    return {
        "event-type": "in-app-purchase",
        "time": int(time.time()),
        "user-id": user_id,
        "product-id": random.choice(PRODUCTS),
        "purchase_value": round(random.uniform(0.99, 99.99), 2),
    }
