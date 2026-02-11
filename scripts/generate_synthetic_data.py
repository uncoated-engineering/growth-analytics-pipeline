#!/usr/bin/env python3
"""
Synthetic Data Generation Script for Growth Analytics Pipeline

Generates realistic SaaS product-led growth data with built-in conversion signals.
Can be run in full or for specific components only.
"""

import argparse
import json
import math
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, TypedDict

# ── Type definitions ───────────────────────────────────────────────────────────


class FeatureRelease(TypedDict):
    id: int
    name: str
    release_date: str
    version: str


class FeatureConfig(TypedDict):
    adoption_rate: float
    conversion_boost: float
    days_saved: int
    events_range: Tuple[int, int]


class CompanySizeProfile(TypedDict):
    conversion_mult: float
    days_mult: float
    mrr_range: Dict[str, Tuple[int, int]]


# ── Configuration ──────────────────────────────────────────────────────────────
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
TOTAL_USERS = 5000
BASE_CONVERSION_RATE = 0.12  # 12% baseline (no feature usage)
AVG_CONVERSION_DAYS_BASE = 30  # Average days to convert without features

# Feature releases (including version upgrades for SCD Type 2 demo)
FEATURES: List[FeatureRelease] = [
    {"id": 1, "name": "real_time_collab", "release_date": "2024-01-15", "version": "v1.0"},
    {"id": 2, "name": "ai_insights", "release_date": "2024-03-01", "version": "v1.0"},
    {"id": 3, "name": "advanced_export", "release_date": "2024-05-01", "version": "v1.0"},
    {"id": 4, "name": "api_access", "release_date": "2024-07-01", "version": "v1.0"},
    {"id": 5, "name": "team_analytics", "release_date": "2024-09-01", "version": "v1.0"},
    {"id": 6, "name": "custom_workflows", "release_date": "2024-11-01", "version": "v1.0"},
    # Version upgrades (SCD Type 2 showcase)
    {"id": 1, "name": "real_time_collab", "release_date": "2024-07-01", "version": "v2.0"},
    {"id": 2, "name": "ai_insights", "release_date": "2024-10-01", "version": "v2.0"},
]

# Per-feature behavior: adoption rates, conversion impact, and usage intensity
FEATURE_CONFIGS: Dict[str, FeatureConfig] = {
    "real_time_collab": {
        "adoption_rate": 0.45,
        "conversion_boost": 0.15,
        "days_saved": 10,
        "events_range": (3, 15),
    },
    "ai_insights": {
        "adoption_rate": 0.30,
        "conversion_boost": 0.10,
        "days_saved": 7,
        "events_range": (2, 10),
    },
    "advanced_export": {
        "adoption_rate": 0.20,
        "conversion_boost": 0.06,
        "days_saved": 3,
        "events_range": (1, 8),
    },
    "api_access": {
        "adoption_rate": 0.12,
        "conversion_boost": 0.04,
        "days_saved": 2,
        "events_range": (1, 5),
    },
    "team_analytics": {
        "adoption_rate": 0.25,
        "conversion_boost": 0.12,
        "days_saved": 8,
        "events_range": (2, 12),
    },
    "custom_workflows": {
        "adoption_rate": 0.08,
        "conversion_boost": 0.03,
        "days_saved": 1,
        "events_range": (1, 6),
    },
}

# Company size profiles: conversion multiplier, time-to-convert multiplier, MRR ranges
COMPANY_SIZE_PROFILES: Dict[str, CompanySizeProfile] = {
    "1-10": {
        "conversion_mult": 0.8,
        "days_mult": 0.7,
        "mrr_range": {"pro": (29, 79), "enterprise": (199, 499)},
    },
    "11-50": {
        "conversion_mult": 1.0,
        "days_mult": 0.9,
        "mrr_range": {"pro": (79, 199), "enterprise": (499, 999)},
    },
    "51-200": {
        "conversion_mult": 1.1,
        "days_mult": 1.0,
        "mrr_range": {"pro": (199, 499), "enterprise": (999, 2499)},
    },
    "201-1000": {
        "conversion_mult": 1.2,
        "days_mult": 1.3,
        "mrr_range": {"pro": (499, 999), "enterprise": (2499, 4999)},
    },
    "1000+": {
        "conversion_mult": 1.3,
        "days_mult": 1.5,
        "mrr_range": {"pro": (999, 1999), "enterprise": (4999, 9999)},
    },
}

# Industry affects feature adoption rates
INDUSTRY_ADOPTION_MULT: Dict[str, float] = {
    "technology": 1.3,
    "finance": 1.0,
    "healthcare": 0.8,
    "retail": 1.1,
    "education": 1.2,
    "manufacturing": 0.7,
}

INDUSTRIES = list(INDUSTRY_ADOPTION_MULT.keys())
COMPANY_SIZES = list(COMPANY_SIZE_PROFILES.keys())
EVENT_TYPES = ["view", "use", "share"]
PLANS = ["pro", "enterprise"]


def setup_directories():
    """Create necessary data directories if they don't exist."""
    base_path = Path(__file__).parent.parent / "data" / "raw"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path


def _feature_release_dates() -> Dict[str, datetime]:
    """Return the earliest release date per feature name (v1.0 release)."""
    dates: Dict[str, datetime] = {}
    for f in FEATURES:
        name = f["name"]
        d = datetime.strptime(f["release_date"], "%Y-%m-%d")
        if name not in dates or d < dates[name]:
            dates[name] = d
    return dates


def _feature_id_map() -> Dict[str, int]:
    """Return feature_name -> feature_id mapping."""
    mapping = {}
    for f in FEATURES:
        if f["name"] not in mapping:
            mapping[f["name"]] = f["id"]
    return mapping


def generate_feature_releases(output_path: Path):
    """Generate feature releases timeline (including version upgrades)."""
    print("Generating feature releases...")

    output_file = output_path / "feature_releases.json"
    with open(output_file, "w") as f:
        json.dump(FEATURES, f, indent=2)

    print(f"  ✓ {len(FEATURES)} feature release entries (6 features, 2 version upgrades)")


def generate_user_signups(output_path: Path):
    """Generate user signups with realistic SaaS growth patterns."""
    print("Generating user signups...")

    feature_dates = list(_feature_release_dates().values())
    total_days = (END_DATE - START_DATE).days

    # Weighted distributions for company size and industry
    size_weights = [0.30, 0.25, 0.20, 0.15, 0.10]
    industry_weights = [0.30, 0.15, 0.15, 0.15, 0.15, 0.10]

    daily_signups = []
    current_date = START_DATE
    users_generated = 0

    while current_date <= END_DATE and users_generated < TOTAL_USERS:
        day_of_year = (current_date - START_DATE).days

        # Exponential growth base (~3x over the year)
        growth_factor = 1.0 + 2.0 * (day_of_year / total_days)

        # Seasonal pattern
        month = current_date.month
        if month in (6, 7, 8):
            seasonal = 0.85
        elif month in (1, 11, 12):
            seasonal = 1.15
        else:
            seasonal = 1.0

        # Marketing bumps after feature releases (3-week decay)
        marketing_bump = 1.0
        for fdate in feature_dates:
            days_since = (current_date - fdate).days
            if 0 <= days_since <= 21:
                marketing_bump += 0.6 * math.exp(-days_since / 7)

        # Weekend dip
        dow_factor = 0.4 if current_date.weekday() >= 5 else 1.0

        base_rate = TOTAL_USERS / total_days
        daily_count = base_rate * growth_factor * seasonal * marketing_bump * dow_factor
        daily_count = int(daily_count * random.uniform(0.7, 1.3))
        daily_count = max(0, min(daily_count, TOTAL_USERS - users_generated))

        if daily_count > 0:
            daily_signups.append((current_date, daily_count))
            users_generated += daily_count

        current_date += timedelta(days=1)

    # Write user records
    output_file = output_path / "user_signups.jsonl"
    user_id = 1

    with open(output_file, "w") as f:
        for signup_date, count in daily_signups:
            for _ in range(count):
                user = {
                    "user_id": user_id,
                    "email": f"user{user_id}@example.com",
                    "signup_date": signup_date.strftime("%Y-%m-%d"),
                    "company_size": random.choices(COMPANY_SIZES, weights=size_weights, k=1)[0],
                    "industry": random.choices(INDUSTRIES, weights=industry_weights, k=1)[0],
                }
                f.write(json.dumps(user) + "\n")
                user_id += 1

    print(f"  ✓ {user_id - 1} user signups")


def load_users(output_path: Path) -> List[Dict[str, Any]]:
    """Load generated users from file."""
    users = []
    user_file = output_path / "user_signups.jsonl"

    if not user_file.exists():
        print("Error: user_signups.jsonl not found. Run signup generation first.")
        sys.exit(1)

    with open(user_file, "r") as f:
        for line in f:
            users.append(json.loads(line))

    return users


def generate_feature_usage_events(output_path: Path) -> Dict[int, Set[str]]:
    """Generate feature usage events with per-feature adoption rates."""
    print("Generating feature usage events...")

    users = load_users(output_path)
    release_dates = _feature_release_dates()
    fid_map = _feature_id_map()

    user_features: Dict[int, Set[str]] = {}
    output_file = output_path / "feature_usage_events.jsonl"
    events_generated = 0

    with open(output_file, "w") as f:
        for user in users:
            uid = user["user_id"]
            signup = datetime.strptime(user["signup_date"], "%Y-%m-%d")
            industry = user["industry"]
            adoption_mult = INDUSTRY_ADOPTION_MULT.get(industry, 1.0)
            user_features[uid] = set()

            for feature_name, config in FEATURE_CONFIGS.items():
                release = release_dates[feature_name]

                # User can discover a feature if it was released before or within
                # 60 days after their signup
                earliest_use = max(signup, release)
                if earliest_use > END_DATE:
                    continue

                # Adoption decision (industry affects adoption likelihood)
                effective_adoption = min(config["adoption_rate"] * adoption_mult, 0.95)
                if random.random() >= effective_adoption:
                    continue

                user_features[uid].add(feature_name)
                fid = fid_map[feature_name]
                num_events = random.randint(*config["events_range"])

                for i in range(num_events):
                    if i == 0:
                        # First event: discovery within first 14 days
                        days_offset = random.randint(0, 14)
                    else:
                        # Subsequent events spread over 90 days
                        days_offset = random.randint(0, 90)

                    event_date = earliest_use + timedelta(
                        days=days_offset,
                        hours=random.randint(8, 22),
                        minutes=random.randint(0, 59),
                    )

                    if event_date > END_DATE:
                        continue

                    event_type = random.choices(EVENT_TYPES, weights=[0.4, 0.45, 0.15], k=1)[0]

                    event = {
                        "timestamp": event_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "user_id": uid,
                        "feature_id": fid,
                        "feature_name": feature_name,
                        "event_type": event_type,
                    }
                    f.write(json.dumps(event) + "\n")
                    events_generated += 1

    print(f"  ✓ {events_generated} feature usage events")

    # Save metadata
    per_feature = {
        name: len([1 for feats in user_features.values() if name in feats])
        for name in FEATURE_CONFIGS
    }
    rtc_users = sorted(uid for uid, feats in user_features.items() if "real_time_collab" in feats)

    metadata = {
        "users_with_real_time_collab": rtc_users,
        "percentage": len(rtc_users) / len(users) if users else 0,
        "total_users": len(users),
        "per_feature_adoption": per_feature,
    }
    metadata_file = output_path / "_feature_users_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"  ✓ Feature adoption: {per_feature}")

    return user_features


def generate_conversions(output_path: Path, user_features: Dict[int, Set[str]] = None):
    """Generate conversion events driven by feature usage patterns."""
    print("Generating conversion events...")

    users = load_users(output_path)

    # Load metadata
    metadata_file = output_path / "_feature_users_metadata.json"
    if not metadata_file.exists():
        print("Error: Feature usage metadata not found. Run feature usage generation first.")
        sys.exit(1)

    with open(metadata_file, "r") as f:
        metadata = json.load(f)
    users_with_rtc = set(metadata["users_with_real_time_collab"])

    # Reconstruct user_features from events if not passed directly
    if user_features is None:
        user_features = {}
        events_file = output_path / "feature_usage_events.jsonl"
        with open(events_file, "r") as f:
            for line in f:
                evt = json.loads(line)
                uid = evt["user_id"]
                if uid not in user_features:
                    user_features[uid] = set()
                user_features[uid].add(evt["feature_name"])

    output_file = output_path / "conversions.jsonl"
    conversions_generated = 0

    with open(output_file, "w") as f:
        for user in users:
            uid = user["user_id"]
            signup = datetime.strptime(user["signup_date"], "%Y-%m-%d")
            company_size = user["company_size"]
            size_profile = COMPANY_SIZE_PROFILES[company_size]

            # Calculate conversion probability from feature usage
            conversion_rate = BASE_CONVERSION_RATE
            days_saved = 0
            features_used = user_features.get(uid, set())

            for feat_name in features_used:
                config = FEATURE_CONFIGS[feat_name]
                conversion_rate += config["conversion_boost"]
                days_saved += config["days_saved"]

            # Apply company size multiplier
            conversion_rate *= size_profile["conversion_mult"]
            conversion_rate = min(conversion_rate, 0.85)

            if random.random() >= conversion_rate:
                continue

            # Days to convert (company size affects sales cycle length)
            avg_days = max(5, AVG_CONVERSION_DAYS_BASE - days_saved)
            avg_days = int(avg_days * size_profile["days_mult"])
            days_to_convert = max(1, int(random.gauss(avg_days, avg_days * 0.3)))
            conversion_date = signup + timedelta(days=days_to_convert)

            if conversion_date > END_DATE:
                continue

            # Determine plan based on company size
            if company_size in ("201-1000", "1000+"):
                plan = "enterprise" if random.random() < 0.65 else "pro"
            elif company_size == "51-200":
                plan = "enterprise" if random.random() < 0.35 else "pro"
            else:
                plan = "pro" if random.random() < 0.85 else "enterprise"

            mrr_low, mrr_high = size_profile["mrr_range"][plan]
            mrr = random.randint(mrr_low, mrr_high)

            conversion = {
                "user_id": uid,
                "conversion_date": conversion_date.strftime("%Y-%m-%d"),
                "plan": plan,
                "mrr": mrr,
                "signup_date": user["signup_date"],
                "days_to_convert": days_to_convert,
                "used_real_time_collab": uid in users_with_rtc,
            }
            f.write(json.dumps(conversion) + "\n")
            conversions_generated += 1

    print(f"  ✓ {conversions_generated} conversions")


def validate_data(output_path: Path):
    """Validate generated data and print statistics."""
    print("\n" + "=" * 60)
    print("VALIDATION REPORT")
    print("=" * 60)

    users = load_users(output_path)
    print(f"\n✓ Total users: {len(users)}")

    # Industry breakdown
    industry_counts: Dict[str, int] = {}
    size_counts: Dict[str, int] = {}
    for u in users:
        industry_counts[u["industry"]] = industry_counts.get(u["industry"], 0) + 1
        size_counts[u["company_size"]] = size_counts.get(u["company_size"], 0) + 1
    print(f"  Industries: {industry_counts}")
    print(f"  Company sizes: {size_counts}")

    # Feature usage events
    events_file = output_path / "feature_usage_events.jsonl"
    if events_file.exists():
        with open(events_file, "r") as f:
            events = [json.loads(line) for line in f]
        print(f"\n✓ Total feature usage events: {len(events)}")

        # Per-feature breakdown
        feature_event_counts: Dict[str, int] = {}
        feature_user_sets: Dict[str, Set[int]] = {}
        for e in events:
            fn = e["feature_name"]
            feature_event_counts[fn] = feature_event_counts.get(fn, 0) + 1
            if fn not in feature_user_sets:
                feature_user_sets[fn] = set()
            feature_user_sets[fn].add(e["user_id"])

        for fn in sorted(feature_event_counts.keys()):
            print(
                f"  {fn}: {feature_event_counts[fn]} events, "
                f"{len(feature_user_sets[fn])} unique users "
                f"({len(feature_user_sets[fn]) / len(users) * 100:.1f}%)"
            )

    # Conversions
    conversions_file = output_path / "conversions.jsonl"
    if conversions_file.exists():
        with open(conversions_file, "r") as f:
            conversions = [json.loads(line) for line in f]
        print(f"\n✓ Total conversions: {len(conversions)}")
        print(f"  Overall conversion rate: {len(conversions) / len(users) * 100:.1f}%")

        # Plan breakdown
        pro = [c for c in conversions if c["plan"] == "pro"]
        ent = [c for c in conversions if c["plan"] == "enterprise"]
        print(f"  Pro: {len(pro)}, Enterprise: {len(ent)}")

        # MRR stats
        mrrs = [c["mrr"] for c in conversions]
        print(f"  MRR range: ${min(mrrs)} - ${max(mrrs)}")
        print(f"  MRR avg: ${sum(mrrs) / len(mrrs):.0f}")
        print(f"  Total MRR: ${sum(mrrs):,}")

        # Conversion rate by feature usage
        with_feature = [c for c in conversions if c["used_real_time_collab"]]
        without_feature = [c for c in conversions if not c["used_real_time_collab"]]

        metadata_file = output_path / "_feature_users_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            users_with_feature = set(metadata["users_with_real_time_collab"])
            users_without_feature = set(u["user_id"] for u in users) - users_with_feature

            conv_rate_with = len(with_feature) / len(users_with_feature) * 100
            conv_rate_without = len(without_feature) / len(users_without_feature) * 100

            print("\n📊 Real-Time Collab Conversion Analysis:")
            print(
                f"   WITH real_time_collab: {conv_rate_with:.1f}% "
                f"({len(with_feature)}/{len(users_with_feature)})"
            )
            print(
                f"   WITHOUT real_time_collab: {conv_rate_without:.1f}% "
                f"({len(without_feature)}/{len(users_without_feature)})"
            )
            print(f"   Lift: {conv_rate_with - conv_rate_without:.1f} percentage points")

            avg_days_with = (
                sum(c["days_to_convert"] for c in with_feature) / len(with_feature)
                if with_feature
                else 0
            )
            avg_days_without = (
                sum(c["days_to_convert"] for c in without_feature) / len(without_feature)
                if without_feature
                else 0
            )

            print("\n⏱️  Time to Conversion:")
            print(f"   WITH real_time_collab: {avg_days_with:.1f} days")
            print(f"   WITHOUT real_time_collab: {avg_days_without:.1f} days")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for Growth Analytics Pipeline"
    )
    parser.add_argument(
        "--generate",
        nargs="+",
        choices=["features", "signups", "usage", "conversions", "all"],
        default=["all"],
        help="Specify which data to generate (default: all)",
    )
    parser.add_argument(
        "--validate", action="store_true", help="Validate and show statistics for generated data"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)"
    )

    args = parser.parse_args()

    random.seed(args.seed)

    output_path = setup_directories()

    print("\n" + "=" * 60)
    print("SYNTHETIC DATA GENERATION")
    print("=" * 60 + "\n")

    generate_items = args.generate
    if "all" in generate_items:
        generate_items = ["features", "signups", "usage", "conversions"]

    user_features = None

    if "features" in generate_items:
        generate_feature_releases(output_path)

    if "signups" in generate_items:
        generate_user_signups(output_path)

    if "usage" in generate_items:
        user_features = generate_feature_usage_events(output_path)

    if "conversions" in generate_items:
        generate_conversions(output_path, user_features)

    if args.validate or "all" in args.generate:
        validate_data(output_path)

    print("\n✅ Data generation complete!\n")


if __name__ == "__main__":
    main()
