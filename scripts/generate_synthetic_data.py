#!/usr/bin/env python3
"""
Synthetic Data Generation Script for Growth Analytics Pipeline

Generates realistic SaaS product-led growth data with built-in conversion signals.
Can be run in full or for specific components only.
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

# Configuration
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
TOTAL_USERS = 1000
BASE_CONVERSION_RATE = 0.18  # 18% baseline
FEATURE_CONVERSION_RATE = 0.22  # 22% with real_time_collab
FEATURE_USAGE_PERCENTAGE = 0.40  # 40% try real_time_collab
AVG_CONVERSION_DAYS_WITH_FEATURE = 18
AVG_CONVERSION_DAYS_WITHOUT = 27
CONVERSION_WINDOW_DAYS = 7  # Early usage window for higher conversion

# Feature definitions
FEATURES = [
    {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
    {"id": 2, "name": "ai_insights", "release_date": "2024-05-01", "version": "v1.0"},
    {"id": 3, "name": "advanced_export", "release_date": "2024-08-01", "version": "v1.0"},
    {"id": 4, "name": "api_access", "release_date": "2024-10-01", "version": "v1.0"},
]

INDUSTRIES = ["technology", "healthcare", "finance", "retail", "education", "manufacturing"]
COMPANY_SIZES = ["1-10", "11-50", "51-200", "201-1000", "1000+"]
EVENT_TYPES = ["view", "use", "share"]
PLANS = ["pro", "enterprise"]


def setup_directories():
    """Create necessary data directories if they don't exist."""
    base_path = Path(__file__).parent.parent / "data" / "raw"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path


def generate_feature_releases(output_path: Path):
    """Generate feature releases timeline."""
    print("Generating feature releases...")

    output_file = output_path / "feature_releases.json"
    with open(output_file, "w") as f:
        json.dump(FEATURES, f, indent=2)

    print(f"✓ Generated {len(FEATURES)} feature releases -> {output_file}")


def generate_user_signups(output_path: Path):
    """Generate user signups with realistic growth patterns."""
    print("Generating user signups...")

    # Parse feature release dates
    feature_dates = [datetime.strptime(str(f["release_date"]), "%Y-%m-%d") for f in FEATURES]

    # Calculate daily signup distribution
    total_days = (END_DATE - START_DATE).days
    daily_signups = []

    current_date = START_DATE
    users_generated = 0
    base_daily_rate = TOTAL_USERS / total_days

    while current_date <= END_DATE and users_generated < TOTAL_USERS:
        # Increase signups after feature releases (marketing bumps)
        multiplier = 1.0
        for feature_date in feature_dates:
            days_since_release = (current_date - feature_date).days
            if 0 <= days_since_release <= 14:  # 2-week marketing bump
                multiplier += 0.5 * (1 - days_since_release / 14)

        # Add some randomness
        daily_count = int(base_daily_rate * multiplier * random.uniform(0.7, 1.3))
        daily_count = max(1, min(daily_count, TOTAL_USERS - users_generated))

        daily_signups.append((current_date, daily_count))
        users_generated += daily_count
        current_date += timedelta(days=1)

    # Generate actual user records
    output_file = output_path / "user_signups.jsonl"
    user_id = 1

    with open(output_file, "w") as f:
        for signup_date, count in daily_signups:
            for _ in range(count):
                user = {
                    "user_id": user_id,
                    "email": f"user{user_id}@example.com",
                    "signup_date": signup_date.strftime("%Y-%m-%d"),
                    "company_size": random.choice(COMPANY_SIZES),
                    "industry": random.choice(INDUSTRIES),
                }
                f.write(json.dumps(user) + "\n")
                user_id += 1

    print(f"✓ Generated {user_id - 1} user signups -> {output_file}")


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


def generate_feature_usage_events(output_path: Path):
    """Generate feature usage events correlated with conversions."""
    print("Generating feature usage events...")

    users = load_users(output_path)

    # Determine which users will use real_time_collab (the key feature)
    users_with_feature = set(
        random.sample([u["user_id"] for u in users], int(len(users) * FEATURE_USAGE_PERCENTAGE))
    )

    output_file = output_path / "feature_usage_events.jsonl"
    events_generated = 0

    with open(output_file, "w") as f:
        for user in users:
            user_id = user["user_id"]
            signup_date = datetime.strptime(user["signup_date"], "%Y-%m-%d")

            # Determine if this user uses real_time_collab
            if user_id in users_with_feature:
                # Generate events for real_time_collab (feature id: 1)
                # More events in first 7 days (critical conversion window)
                event_date = signup_date + timedelta(days=random.randint(0, 6))

                # Generate 2-5 events per user with feature
                num_events = random.randint(2, 5)
                for _ in range(num_events):
                    # Spread events over first 30 days
                    event_date = signup_date + timedelta(
                        days=random.randint(0, 30),
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59),
                    )

                    if event_date <= END_DATE:
                        event = {
                            "timestamp": event_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "user_id": user_id,
                            "feature_id": 1,  # real_time_collab
                            "feature_name": "real_time_collab",
                            "event_type": random.choice(EVENT_TYPES),
                        }
                        f.write(json.dumps(event) + "\n")
                        events_generated += 1

            # All users might also use other features (but less impact on conversion)
            if random.random() < 0.3:  # 30% use other features
                for feature in FEATURES[1:]:  # Skip real_time_collab
                    feature_date = datetime.strptime(str(feature["release_date"]), "%Y-%m-%d")

                    if signup_date < feature_date:
                        # User signed up before feature release
                        event_date = feature_date + timedelta(days=random.randint(0, 60))
                    else:
                        # User signed up after feature release
                        event_date = signup_date + timedelta(days=random.randint(0, 60))

                    if event_date <= END_DATE and random.random() < 0.5:
                        event = {
                            "timestamp": event_date.strftime("%Y-%m-%d %H:%M:%S"),
                            "user_id": user_id,
                            "feature_id": feature["id"],
                            "feature_name": feature["name"],
                            "event_type": random.choice(EVENT_TYPES),
                        }
                        f.write(json.dumps(event) + "\n")
                        events_generated += 1

    print(f"✓ Generated {events_generated} feature usage events -> {output_file}")

    # Save metadata about which users used the key feature
    metadata_file = output_path / "_feature_users_metadata.json"
    with open(metadata_file, "w") as f:
        json.dump(
            {
                "users_with_real_time_collab": sorted(list(users_with_feature)),
                "percentage": FEATURE_USAGE_PERCENTAGE,
                "total_users": len(users),
            },
            f,
            indent=2,
        )


def generate_conversions(output_path: Path):
    """Generate conversion events tied to usage patterns."""
    print("Generating conversion events...")

    users = load_users(output_path)

    # Load metadata about feature usage
    metadata_file = output_path / "_feature_users_metadata.json"
    if not metadata_file.exists():
        print("Error: Feature usage metadata not found. Run feature usage generation first.")
        sys.exit(1)

    with open(metadata_file, "r") as f:
        metadata = json.load(f)
        users_with_feature = set(metadata["users_with_real_time_collab"])

    output_file = output_path / "conversions.jsonl"
    conversions_generated = 0

    with open(output_file, "w") as f:
        for user in users:
            user_id = user["user_id"]
            signup_date = datetime.strptime(user["signup_date"], "%Y-%m-%d")

            # Determine conversion probability and timing
            if user_id in users_with_feature:
                conversion_rate = FEATURE_CONVERSION_RATE
                avg_days = AVG_CONVERSION_DAYS_WITH_FEATURE
            else:
                conversion_rate = BASE_CONVERSION_RATE
                avg_days = AVG_CONVERSION_DAYS_WITHOUT

            # Determine if user converts
            if random.random() < conversion_rate:
                # Calculate conversion date (normal distribution around avg)
                days_to_convert = max(1, int(random.gauss(avg_days, 5)))
                conversion_date = signup_date + timedelta(days=days_to_convert)

                # Only record if within our date range
                if conversion_date <= END_DATE:
                    # Enterprise plans more common for larger companies
                    company_size = user["company_size"]
                    if company_size in ["201-1000", "1000+"]:
                        plan = "enterprise" if random.random() < 0.6 else "pro"
                        mrr = (
                            random.randint(500, 2000)
                            if plan == "enterprise"
                            else random.randint(50, 200)
                        )
                    else:
                        plan = "pro" if random.random() < 0.8 else "enterprise"
                        mrr = (
                            random.randint(50, 200) if plan == "pro" else random.randint(500, 1000)
                        )

                    conversion = {
                        "user_id": user_id,
                        "conversion_date": conversion_date.strftime("%Y-%m-%d"),
                        "plan": plan,
                        "mrr": mrr,
                        "signup_date": user["signup_date"],
                        "days_to_convert": days_to_convert,
                        "used_real_time_collab": user_id in users_with_feature,
                    }
                    f.write(json.dumps(conversion) + "\n")
                    conversions_generated += 1

    print(f"✓ Generated {conversions_generated} conversions -> {output_file}")


def validate_data(output_path: Path):
    """Validate generated data and print statistics."""
    print("\n" + "=" * 60)
    print("VALIDATION REPORT")
    print("=" * 60)

    # Count users
    users = load_users(output_path)
    print(f"\n✓ Total users: {len(users)}")

    # Count feature usage events
    events_file = output_path / "feature_usage_events.jsonl"
    if events_file.exists():
        with open(events_file, "r") as f:
            events = [json.loads(line) for line in f]
        print(f"✓ Total feature usage events: {len(events)}")

        # Count real_time_collab usage
        rtc_events = [e for e in events if e["feature_id"] == 1]
        rtc_users = len(set(e["user_id"] for e in rtc_events))
        print(
            f"✓ Users who used real_time_collab: {rtc_users} ({rtc_users / len(users) * 100:.1f}%)"
        )

    # Count conversions
    conversions_file = output_path / "conversions.jsonl"
    if conversions_file.exists():
        with open(conversions_file, "r") as f:
            conversions = [json.loads(line) for line in f]
        print(f"✓ Total conversions: {len(conversions)}")

        # Calculate conversion rates
        with_feature = [c for c in conversions if c["used_real_time_collab"]]
        without_feature = [c for c in conversions if not c["used_real_time_collab"]]

        # Load metadata
        metadata_file = output_path / "_feature_users_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
                users_with_feature = set(metadata["users_with_real_time_collab"])
                users_without_feature = set(u["user_id"] for u in users) - users_with_feature

            conv_rate_with = len(with_feature) / len(users_with_feature) * 100
            conv_rate_without = len(without_feature) / len(users_without_feature) * 100

            print("\n📊 Conversion Rate Analysis:")
            print(f"""
                - WITH real_time_collab:
                {conv_rate_with:.1f}% ({len(with_feature)}/{len(users_with_feature)})
                """)
            print(f"""
                - WITHOUT real_time_collab: {conv_rate_without:.1f}%
                ({len(without_feature)}/{len(users_without_feature)})
                """)
            print(f"- Lift: {conv_rate_with - conv_rate_without:.1f} percentage points")

            # Average time to convert
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
            print(f"   - WITH real_time_collab: {avg_days_with:.1f} days")
            print(f"   - WITHOUT real_time_collab: {avg_days_without:.1f} days")

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

    # Set random seed
    random.seed(args.seed)

    # Setup directories
    output_path = setup_directories()

    print("\n" + "=" * 60)
    print("SYNTHETIC DATA GENERATION")
    print("=" * 60 + "\n")

    # Determine what to generate
    generate_items = args.generate
    if "all" in generate_items:
        generate_items = ["features", "signups", "usage", "conversions"]

    # Generate data in order (dependencies matter)
    if "features" in generate_items:
        generate_feature_releases(output_path)

    if "signups" in generate_items:
        generate_user_signups(output_path)

    if "usage" in generate_items:
        generate_feature_usage_events(output_path)

    if "conversions" in generate_items:
        generate_conversions(output_path)

    # Validate if requested or if all data was generated
    if args.validate or "all" in args.generate:
        validate_data(output_path)

    print("\n✅ Data generation complete!\n")


if __name__ == "__main__":
    main()
