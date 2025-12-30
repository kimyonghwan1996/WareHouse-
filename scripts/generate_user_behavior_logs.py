# generate_user_behavior_logs.py
"""
User Behavior Log Generator (JSON Lines)
- Creates realistic clickstream events with funnel behavior.
- Output: JSONL file (one JSON object per line)

Usage:
  python generate_user_behavior_logs.py --days 7 --users 20000 --out logs.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple


# -----------------------------
# Config / Helpers
# -----------------------------

@dataclass(frozen=True)
class UserProfile:
    user_id: str
    device: str
    country: str
    signup_date: datetime


def weighted_choice(items: List[Tuple[str, float]]) -> str:
    """Return one item by weight."""
    total = sum(w for _, w in items)
    r = random.random() * total
    upto = 0.0
    for item, w in items:
        upto += w
        if upto >= r:
            return item
    return items[-1][0]


def hour_multiplier(hour: int) -> float:
    """
    Traffic pattern by hour:
    - 낮(11~14), 저녁(19~23) 피크
    - 새벽(2~6) 저조
    """
    # Two peaks: lunch & evening, plus baseline
    lunch_peak = math.exp(-((hour - 12) ** 2) / 10.0)
    evening_peak = math.exp(-((hour - 21) ** 2) / 12.0)
    dawn_dip = math.exp(-((hour - 4) ** 2) / 6.0)
    baseline = 0.35
    mult = baseline + 1.25 * lunch_peak + 1.45 * evening_peak - 0.25 * dawn_dip
    return max(0.05, mult)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_products(n: int = 500) -> List[Dict]:
    """Generate a simple product catalog."""
    categories = [
        ("electronics", 0.18),
        ("fashion", 0.22),
        ("beauty", 0.12),
        ("home", 0.16),
        ("sports", 0.10),
        ("food", 0.10),
        ("books", 0.12),
    ]
    products = []
    for i in range(1, n + 1):
        cat = weighted_choice(categories)
        base_price = {
            "electronics": 120000,
            "fashion": 45000,
            "beauty": 28000,
            "home": 65000,
            "sports": 52000,
            "food": 18000,
            "books": 15000,
        }[cat]
        price = int(random.gauss(base_price, base_price * 0.35))
        price = max(3000, min(price, 800000))
        products.append(
            {
                "product_id": f"p{i:04d}",
                "category": cat,
                "price": price,
                "brand": f"brand{random.randint(1, 80):02d}",
            }
        )
    return products


def build_users(n: int, start_date: datetime, end_date: datetime) -> List[UserProfile]:
    """Create user profiles with fixed attributes."""
    devices = [("mobile", 0.74), ("web", 0.26)]
    countries = [("KR", 0.55), ("JP", 0.12), ("US", 0.18), ("SG", 0.06), ("DE", 0.05), ("BR", 0.04)]
    users: List[UserProfile] = []
    total_days = (end_date.date() - start_date.date()).days + 1
    for i in range(1, n + 1):
        signup_offset = random.randint(0, max(0, total_days - 1))
        signup_dt = (start_date + timedelta(days=signup_offset)).replace(
            hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59)
        )
        users.append(
            UserProfile(
                user_id=f"u{i:06d}",
                device=weighted_choice(devices),
                country=weighted_choice(countries),
                signup_date=signup_dt,
            )
        )
    return users


# -----------------------------
# Session / Event generation
# -----------------------------

def generate_day_events(
    day_start: datetime,
    users: List[UserProfile],
    products: List[Dict],
    avg_sessions_per_user_per_day: float,
) -> List[Dict]:
    """
    Generate events for a single day.
    - Each user has Poisson-like sessions around avg_sessions_per_user_per_day (approx).
    - Each session produces page_view(s), maybe add_to_cart, maybe purchase.
    """
    events: List[Dict] = []
    day_end = day_start + timedelta(days=1)

    # A rough approximation to Poisson without numpy: use geometric-ish sampling
    def sample_sessions(mean: float) -> int:
        # Many users: 0~2 sessions, few heavy users.
        # Tuned to behave like "light-tailed" Poisson-ish.
        r = random.random()
        if r < 0.55:
            return 0
        if r < 0.85:
            return 1
        if r < 0.95:
            return 2
        if r < 0.985:
            return 3
        return 4 + (1 if random.random() < 0.4 else 0)

    # Funnel probabilities (overall conversion ~2~5%)
    p_add_to_cart = 0.09   # given a session
    p_purchase_given_cart = 0.32

    # Page views per session distribution
    pv_choices = [(1, 0.25), (2, 0.28), (3, 0.20), (4, 0.12), (5, 0.08), (6, 0.07)]
    def sample_pageviews() -> int:
        return int(weighted_choice([(str(k), w) for k, w in pv_choices]))

    # For faster lookup
    product_by_id = {p["product_id"]: p for p in products}

    for u in users:
        # User must exist before generating (optional realism)
        if u.signup_date > day_end:
            continue

        n_sessions = sample_sessions(avg_sessions_per_user_per_day)
        for _ in range(n_sessions):
            session_id = str(uuid.uuid4())
            # Sample a session start time within the day, but weighted by hour multiplier
            # First pick hour by weights, then minute/second uniformly
            hour_weights = [(h, hour_multiplier(h)) for h in range(24)]
            hour = int(weighted_choice([(str(h), w) for h, w in hour_weights]))
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            session_start = day_start.replace(hour=hour, minute=minute, second=second)

            # Session length: 2~12 minutes usually
            session_len_sec = int(max(45, random.gauss(6 * 60, 3 * 60)))
            session_end = min(session_start + timedelta(seconds=session_len_sec), day_end - timedelta(seconds=1))

            # Choose a "focus product" for this session (browsing around it)
            focus = random.choice(products)
            focus_id = focus["product_id"]

            # 1) page_view events
            pv_count = int(weighted_choice([(str(k), w) for k, w in pv_choices]))
            for i in range(pv_count):
                ts = session_start + timedelta(seconds=int(i * (session_len_sec / max(1, pv_count))))
                # Sometimes view a related product in same category
                if random.random() < 0.35:
                    same_cat = [p for p in products if p["category"] == focus["category"]]
                    pid = random.choice(same_cat)["product_id"] if same_cat else focus_id
                else:
                    pid = focus_id

                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_time": iso(ts),
                        "event_type": "page_view",
                        "session_id": session_id,
                        "user_id": u.user_id,
                        "device": u.device,
                        "country": u.country,
                        "product_id": pid,
                        "category": product_by_id[pid]["category"],
                        "price": product_by_id[pid]["price"],
                        "revenue": 0,
                        "referrer": weighted_choice([("search", 0.42), ("ads", 0.18), ("direct", 0.25), ("social", 0.15)]),
                    }
                )

            # 2) maybe add_to_cart
            added_pid = None
            if random.random() < p_add_to_cart:
                ts = session_start + timedelta(seconds=int(session_len_sec * random.uniform(0.35, 0.75)))
                added_pid = focus_id if random.random() < 0.7 else random.choice(products)["product_id"]
                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_time": iso(ts),
                        "event_type": "add_to_cart",
                        "session_id": session_id,
                        "user_id": u.user_id,
                        "device": u.device,
                        "country": u.country,
                        "product_id": added_pid,
                        "category": product_by_id[added_pid]["category"],
                        "price": product_by_id[added_pid]["price"],
                        "revenue": 0,
                        "quantity": 1 if random.random() < 0.85 else 2,
                    }
                )

            # 3) maybe purchase (only if cart happened)
            if added_pid and random.random() < p_purchase_given_cart:
                ts = session_start + timedelta(seconds=int(session_len_sec * random.uniform(0.78, 0.98)))
                qty = 1 if random.random() < 0.88 else 2
                price = product_by_id[added_pid]["price"]
                revenue = price * qty
                events.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_time": iso(ts),
                        "event_type": "purchase",
                        "session_id": session_id,
                        "user_id": u.user_id,
                        "device": u.device,
                        "country": u.country,
                        "product_id": added_pid,
                        "category": product_by_id[added_pid]["category"],
                        "price": price,
                        "quantity": qty,
                        "revenue": revenue,
                        "payment": weighted_choice([("card", 0.68), ("bank_transfer", 0.16), ("wallet", 0.16)]),
                    }
                )

    # Sort by time for nicer downstream loading
    events.sort(key=lambda x: x["event_time"])
    return events


def write_jsonl(path: str, rows: List[Dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# -----------------------------
# CLI
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate realistic user behavior logs (JSONL).")
    parser.add_argument("--days", type=int, default=7, help="How many days of logs to generate.")
    parser.add_argument("--users", type=int, default=20000, help="Number of users to simulate.")
    parser.add_argument("--products", type=int, default=500, help="Number of products in the catalog.")
    parser.add_argument("--avg_sessions", type=float, default=0.9, help="Avg sessions per user per day (rough).")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD). Default: today- days.")
    parser.add_argument("--out", type=str, default="data/raw/user_events.jsonl", help="Output JSONL file.")
    args = parser.parse_args()

    # Use UTC to keep it simple; downstream can convert to local time if desired
    now = datetime.now(timezone.utc)
    if args.start:
        start_day = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_day = (now - timedelta(days=args.days)).replace(hour=0, minute=0, second=0, microsecond=0)

    end_day = start_day + timedelta(days=args.days)

    random.seed(42)

    products = make_products(args.products)
    users = build_users(args.users, start_day - timedelta(days=30), end_day)

    all_events: List[Dict] = []
    day = start_day
    while day < end_day:
        day_events = generate_day_events(day, users, products, args.avg_sessions)
        all_events.extend(day_events)
        day += timedelta(days=1)

    write_jsonl(args.out, all_events)

    # Quick stats
    counts = {"page_view": 0, "add_to_cart": 0, "purchase": 0}
    revenue = 0
    for e in all_events:
        counts[e["event_type"]] += 1
        revenue += int(e.get("revenue", 0) or 0)

    print(f"✅ Wrote {len(all_events):,} events to: {args.out}")
    print("Event counts:", counts)
    print(f"Total revenue: {revenue:,}")

if __name__ == "__main__":
    main()
