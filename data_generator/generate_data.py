"""
Data Generator — Telecom Data Ecosystem
Sinh 3 loại dữ liệu giả lập với lỗi có chủ đích để test data quality pipeline.

Usage:
    python data_generator/generate_data.py
    python data_generator/generate_data.py --output-dir /tmp/raw --customers 5000 --days 7
"""
import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker("vi_VN")
random.seed(42)


def generate_customers(n: int = 10_000) -> pd.DataFrame:
    """
    Sinh bảng khách hàng với ~5% null phone_number (lỗi có chủ đích).
    Schema: customer_id, full_name, phone_number, registration_date, plan_type, is_active
    """
    records = []
    for i in range(n):
        records.append({
            "customer_id": f"VT{i:06d}",
            "full_name": fake.name(),
            "phone_number": fake.phone_number() if random.random() > 0.05 else None,
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "plan_type": random.choice(["basic", "standard", "premium"]),
            "is_active": random.choice([True, True, True, False]),
        })
    df = pd.DataFrame(records)
    null_pct = df["phone_number"].isna().mean() * 100
    print(f"  Customers: {len(df):,} rows | phone_number null: {null_pct:.1f}%")
    return df


def generate_cdr(customers_df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
    """
    Sinh Call Detail Records với lỗi có chủ đích:
    - ~3%  duration âm (lỗi hệ thống)
    - ~2%  ngày giờ sai format (dd/MM/yyyy thay vì ISO)
    - ~1%  duplicate records (lỗi gửi lại từ switch)
    """
    active_customers = customers_df[customers_df["is_active"]]["customer_id"].tolist()
    records = []

    for day_offset in range(days):
        call_date = datetime.now() - timedelta(days=day_offset)
        daily_calls = random.randint(500, 2000)

        for _ in range(daily_calls):
            caller = random.choice(active_customers)
            callee = random.choice(active_customers)
            duration = random.randint(10, 3600)

            if random.random() < 0.03:
                duration = -duration

            if random.random() < 0.02:
                call_time = call_date.strftime("%d/%m/%Y %H:%M:%S")
            else:
                call_time = call_date.isoformat()

            records.append({
                "record_id": fake.uuid4(),
                "caller_id": caller,
                "callee_id": callee,
                "call_timestamp": call_time,
                "duration_seconds": duration,
                "call_type": random.choice(["voice", "video", "data"]),
                "tower_id": f"TOWER_{random.randint(1, 500):04d}",
            })

    df = pd.DataFrame(records)

    duplicate_count = int(len(df) * 0.01)
    duplicates = df.sample(duplicate_count)
    df = pd.concat([df, duplicates]).reset_index(drop=True)
    df = df.sample(frac=1).reset_index(drop=True)

    neg_dur_pct = (df["duration_seconds"] < 0).mean() * 100
    print(
        f"  CDR: {len(df):,} rows | negative duration: {neg_dur_pct:.1f}% "
        f"| duplicates injected: {duplicate_count:,}"
    )
    return df


def generate_tower_status(n_towers: int = 500, days: int = 30) -> pd.DataFrame:
    """
    Sinh trạng thái trạm phát sóng theo ngày.
    Schema: tower_id, report_date, status, signal_strength_dbm, traffic_load_percent
    """
    records = []
    for tower_num in range(1, n_towers + 1):
        for day_offset in range(days):
            records.append({
                "tower_id": f"TOWER_{tower_num:04d}",
                "report_date": (datetime.now() - timedelta(days=day_offset)).date(),
                "status": random.choice(["active", "active", "active", "maintenance", "offline"]),
                "signal_strength_dbm": round(random.uniform(-120, -50), 2),
                "traffic_load_percent": round(random.uniform(0, 100), 2),
            })
    df = pd.DataFrame(records)
    print(f"  Tower status: {len(df):,} rows ({n_towers} towers × {days} days)")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic telecom data")
    parser.add_argument("--output-dir", default="/tmp/raw", help="Output directory (default: /tmp/raw)")
    parser.add_argument("--customers", type=int, default=10_000, help="Number of customers (default: 10000)")
    parser.add_argument("--days", type=int, default=30, help="History days for CDR (default: 30)")
    parser.add_argument("--towers", type=int, default=500, help="Number of towers (default: 500)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print("Generating synthetic telecom data...")
    print("=" * 50)

    print("\n[1/3] Customers:")
    customers = generate_customers(args.customers)
    customers.to_csv(output_dir / "customers.csv", index=False)
    print(f"  Saved → {output_dir / 'customers.csv'}")

    print("\n[2/3] CDR (Call Detail Records):")
    cdr = generate_cdr(customers, days=args.days)
    cdr.to_csv(output_dir / "cdr.csv", index=False)
    print(f"  Saved → {output_dir / 'cdr.csv'}")

    print("\n[3/3] Tower Status:")
    towers = generate_tower_status(n_towers=args.towers, days=args.days)
    towers.to_csv(output_dir / "tower_status.csv", index=False)
    print(f"  Saved → {output_dir / 'tower_status.csv'}")

    print("\n" + "=" * 50)
    print(f"  Customers : {len(customers):>10,} rows")
    print(f"  CDR       : {len(cdr):>10,} rows")
    print(f"  Towers    : {len(towers):>10,} rows")
    print("=" * 50)


if __name__ == "__main__":
    main()
