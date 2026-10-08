import datetime
import os
import random
import pyarrow as pa
import pyarrow.orc as orc

ORGANISATIONS = [
    "org_alpha",
    "org_beta",
    "org_gamma"
]

DATES = [
    "2026-03-01",
    "2026-03-02",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
    "2026-03-07"
]

# 10 Financial tables conforming to senior data architecture spec
TABLE_DEFINITIONS = {
    "accounts": {
        "fields": [
            ("account_id", pa.string()),
            ("account_name", pa.string()),
            ("status", pa.string()),
            ("currency", pa.string()),
            ("advisor_id", pa.string()),
            ("opened_at", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "contacts": {
        "fields": [
            ("contact_id", pa.string()),
            ("account_id", pa.string()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("email", pa.string()),
            ("phone", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "portfolios": {
        "fields": [
            ("portfolio_id", pa.string()),
            ("account_id", pa.string()),
            ("portfolio_name", pa.string()),
            ("strategy", pa.string()),
            ("risk_profile", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "securities": {
        "fields": [
            ("security_id", pa.string()),
            ("ticker", pa.string()),
            ("security_name", pa.string()),
            ("asset_class", pa.string()),
            ("currency", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "holdings": {
        "fields": [
            ("holding_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("security_id", pa.string()),
            ("quantity", pa.float64()),
            ("market_value", pa.float64()),
            ("cost_basis", pa.float64()),
            ("as_of_date", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "trades": {
        "fields": [
            ("trade_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("security_id", pa.string()),
            ("side", pa.string()),
            ("quantity", pa.float64()),
            ("price", pa.float64()),
            ("trade_value", pa.float64()),
            ("commission", pa.float64()),
            ("trade_timestamp", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "transactions": {
        "fields": [
            ("transaction_id", pa.string()),
            ("account_id", pa.string()),
            ("transaction_type", pa.string()),
            ("amount", pa.float64()),
            ("currency", pa.string()),
            ("transaction_timestamp", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "advisors": {
        "fields": [
            ("advisor_id", pa.string()),
            ("advisor_name", pa.string()),
            ("email", pa.string()),
            ("region", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "fee_schedules": {
        "fields": [
            ("fee_schedule_id", pa.string()),
            ("account_id", pa.string()),
            ("fee_type", pa.string()),
            ("rate", pa.float64()),
            ("updated_at", pa.string())
        ]
    },
    "statements": {
        "fields": [
            ("statement_id", pa.string()),
            ("account_id", pa.string()),
            ("statement_date", pa.string()),
            ("total_value", pa.float64()),
            ("currency", pa.string()),
            ("updated_at", pa.string())
        ]
    }
}

COLLISION_ACCOUNT_ID = "ACC-COLLISION-999"

def generate_table_data(table_name, org_id, date_str, seed=42):
    rng = random.Random(f"{table_name}-{org_id}-{date_str}-{seed}")
    rows = []

    if table_name == "advisors":
        for i in range(1, 4):
            rows.append({
                "advisor_id": f"ADV-{org_id[-3:].upper()}-{i}",
                "advisor_name": rng.choice(["Franklin Sterling", "Grace Hopper", "Marcus Vance"]),
                "email": f"advisor{i}@{org_id}.com",
                "region": rng.choice(["NORTH_AMERICA", "EMEA", "APAC"]),
                "updated_at": f"{date_str}T07:00:00Z"
            })

    elif table_name == "accounts":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "account_id": acc_id,
                "account_name": f"Account {i} for {org_id.upper()}",
                "status": "ACTIVE",
                "currency": "USD",
                "advisor_id": f"ADV-{org_id[-3:].upper()}-{(i % 3) + 1}",
                "opened_at": f"2026-01-0{i}T00:00:00Z",
                "updated_at": f"{date_str}T08:00:00Z"
            })

        # MANDATORY MULTI-TENANT COLLISION:
        # Same account_id ACC-COLLISION-999 under BOTH org_alpha and org_beta
        if org_id in ["org_alpha", "org_beta"]:
            rows.append({
                "account_id": COLLISION_ACCOUNT_ID,
                "account_name": f"Collision Account Owned By {org_id.upper()}",
                "status": "ACTIVE",
                "currency": "USD",
                "advisor_id": f"ADV-{org_id[-3:].upper()}-1",
                "opened_at": "2026-02-01T10:00:00Z",
                "updated_at": f"{date_str}T10:30:00Z"
            })

        # Intentional duplicate record for deduplication test (same PK, later updated_at timestamp)
        if rows:
            dup = dict(rows[0])
            dup["updated_at"] = f"{date_str}T14:45:00Z"
            dup["account_name"] = rows[0]["account_name"] + " (Latest Snapshot)"
            rows.append(dup)

    elif table_name == "contacts":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "contact_id": f"CON-{org_id[-3:].upper()}-{500 + i}",
                "account_id": acc_id,
                "first_name": rng.choice(["Alice", "Bob", "Charlie", "Diana", "Evan"]),
                "last_name": rng.choice(["Smith", "Johnson", "Williams", "Brown", "Jones"]),
                "email": f"contact{i}@{org_id}.example.com",
                "phone": f"+1-555-01{i:02d}",
                "updated_at": f"{date_str}T09:00:00Z"
            })

    elif table_name == "portfolios":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "portfolio_id": f"PORT-{org_id[-3:].upper()}-{200 + i}",
                "account_id": acc_id,
                "portfolio_name": f"Growth Portfolio {i}",
                "strategy": rng.choice(["EQUITY_GROWTH", "FIXED_INCOME_YIELD", "BALANCED"]),
                "risk_profile": rng.choice(["CONSERVATIVE", "MODERATE", "AGGRESSIVE"]),
                "updated_at": f"{date_str}T08:15:00Z"
            })
        if org_id in ["org_alpha", "org_beta"]:
            rows.append({
                "portfolio_id": f"PORT-COLLISION-{org_id[-3:].upper()}",
                "account_id": COLLISION_ACCOUNT_ID,
                "portfolio_name": f"Flagship Strategy {org_id.upper()}",
                "strategy": "EQUITY_GROWTH",
                "risk_profile": "AGGRESSIVE",
                "updated_at": f"{date_str}T08:15:00Z"
            })

    elif table_name == "securities":
        tickers = [
            ("AAPL", "Apple Inc", "EQUITY", "USD"),
            ("MSFT", "Microsoft Corp", "EQUITY", "USD"),
            ("BND", "Total Bond Market ETF", "FIXED_INCOME", "USD"),
            ("NVDA", "Nvidia Corp", "EQUITY", "USD"),
            ("GLD", "SPDR Gold Shares", "COMMODITY", "USD")
        ]
        for idx, (sym, name, a_class, curr) in enumerate(tickers, start=1):
            rows.append({
                "security_id": f"SEC-{1000 + idx}",
                "ticker": sym,
                "security_name": name,
                "asset_class": a_class,
                "currency": curr,
                "updated_at": f"{date_str}T00:00:00Z"
            })

    elif table_name == "holdings":
        for i in range(1, 6):
            port_id = f"PORT-{org_id[-3:].upper()}-{200 + i}"
            sec_id = f"SEC-{1000 + ((i % 5) + 1)}"
            qty = round(10.0 * i + rng.uniform(0.5, 5.0), 4)
            price = 150.0 + rng.uniform(-10.0, 30.0)
            cost = round(140.0 * qty, 2)
            mval = round(qty * price, 2)
            rows.append({
                "holding_id": f"HLD-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i}",
                "portfolio_id": port_id,
                "security_id": sec_id,
                "quantity": float(qty),
                "market_value": float(mval),
                "cost_basis": float(cost),
                "as_of_date": date_str,
                "updated_at": f"{date_str}T16:00:00Z"
            })

    elif table_name == "trades":
        for i in range(1, 5):
            port_id = f"PORT-{org_id[-3:].upper()}-{200 + i}"
            sec_id = f"SEC-{1000 + i}"
            qty = round(5.0 * i, 2)
            px = round(120.0 + i * 15.0, 2)
            val = round(qty * px, 2)
            rows.append({
                "trade_id": f"TRD-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i:03d}",
                "portfolio_id": port_id,
                "security_id": sec_id,
                "side": rng.choice(["BUY", "SELL"]),
                "quantity": float(qty),
                "price": float(px),
                "trade_value": float(val),
                "commission": 4.95,
                "trade_timestamp": f"{date_str}T14:{i*10:02d}:00Z",
                "updated_at": f"{date_str}T14:{i*10:02d}:00Z"
            })

    elif table_name == "transactions":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            amount = round(500.0 * i + rng.uniform(10.0, 50.0), 2)
            rows.append({
                "transaction_id": f"TXN-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i:04d}",
                "account_id": acc_id,
                "transaction_type": rng.choice(["DEPOSIT", "DIVIDEND", "FEE", "WITHDRAWAL"]),
                "amount": float(amount),
                "currency": "USD",
                "transaction_timestamp": f"{date_str}T18:00:00Z",
                "updated_at": f"{date_str}T18:00:00Z"
            })

    elif table_name == "fee_schedules":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "fee_schedule_id": f"FEE-{org_id[-3:].upper()}-{i}",
                "account_id": acc_id,
                "fee_type": rng.choice(["AUM_TIER", "FIXED_ANNUAL", "TRANSACTION_BASIS"]),
                "rate": round(rng.uniform(0.0025, 0.0075), 4),
                "updated_at": f"{date_str}T06:00:00Z"
            })

    elif table_name == "statements":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "statement_id": f"STMT-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i}",
                "account_id": acc_id,
                "statement_date": date_str,
                "total_value": float(50000.0 + i * 10000.0),
                "currency": "USD",
                "updated_at": f"{date_str}T23:59:59Z"
            })

    return rows

def generate_and_save_dataset(base_output_dir):
    """
    Generates synthetic ORC files with partitioning:
    <base_output_dir>/<table_name>/organisation_id=<org>/processing_date=<date>/data.orc
    Total target: 10 tables x 3 orgs x 7 dates = 210 logical ORC files.
    """
    os.makedirs(base_output_dir, exist_ok=True)
    summary = {}

    for table_name, schema_info in TABLE_DEFINITIONS.items():
        summary[table_name] = {"files": 0, "rows": 0}
        fields = schema_info["fields"]
        pa_schema = pa.schema(fields)

        for org_id in ORGANISATIONS:
            for date_str in DATES:
                part_dir = os.path.join(
                    base_output_dir,
                    table_name,
                    f"organisation_id={org_id}",
                    f"processing_date={date_str}"
                )
                os.makedirs(part_dir, exist_ok=True)
                file_path = os.path.join(part_dir, "data.orc")

                data_rows = generate_table_data(table_name, org_id, date_str, seed=42)

                arrays = []
                for field_name, field_type in fields:
                    col_vals = [row[field_name] for row in data_rows]
                    arrays.append(pa.array(col_vals, type=field_type))

                table = pa.Table.from_arrays(arrays, schema=pa_schema)
                orc.write_table(table, file_path)
                summary[table_name]["files"] += 1
                summary[table_name]["rows"] += len(data_rows)

    return summary

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.abspath(os.path.join(current_dir, "..", "data", "raw_lake"))
    print(f"Generating synthetic ORC data into: {out_dir}")
    stats = generate_and_save_dataset(out_dir)
    print("Generation complete! Summary of generated ORC files:")
    total_files = 0
    total_rows = 0
    for t, stat in stats.items():
        total_files += stat["files"]
        total_rows += stat["rows"]
        print(f"  - {t:15s}: {stat['files']:3d} ORC files | {stat['rows']:4d} rows")
    print(f"\nTotal: {total_files} ORC files across 10 tables, 3 orgs, and 7 dates ({total_rows} total rows).")
