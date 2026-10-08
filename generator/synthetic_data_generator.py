import datetime
import decimal
import json
import os
import random
import uuid
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

TABLE_DEFINITIONS = {
    "accounts": {
        "fields": [
            ("account_id", pa.string()),
            ("account_number", pa.string()),
            ("account_name", pa.string()),
            ("account_type", pa.string()),
            ("currency", pa.string()),
            ("status", pa.string()),
            ("created_at", pa.string()),
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
            ("role", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "portfolios": {
        "fields": [
            ("portfolio_id", pa.string()),
            ("account_id", pa.string()),
            ("portfolio_name", pa.string()),
            ("benchmark_code", pa.string()),
            ("risk_tolerance", pa.string()),
            ("is_active", pa.int8()),
            ("updated_at", pa.string())
        ]
    },
    "securities": {
        "fields": [
            ("security_id", pa.string()),
            ("symbol", pa.string()),
            ("security_name", pa.string()),
            ("asset_class", pa.string()),
            ("sector", pa.string()),
            ("exchange", pa.string()),
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
            ("unrealized_gain_loss", pa.float64()),
            ("as_of_date", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "trades": {
        "fields": [
            ("trade_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("security_id", pa.string()),
            ("trade_type", pa.string()),
            ("trade_status", pa.string()),
            ("units", pa.float64()),
            ("execution_price", pa.float64()),
            ("gross_amount", pa.float64()),
            ("commission_fee", pa.float64()),
            ("executed_at", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "transactions": {
        "fields": [
            ("transaction_id", pa.string()),
            ("account_id", pa.string()),
            ("portfolio_id", pa.string()),
            ("transaction_type", pa.string()),
            ("amount", pa.float64()),
            ("currency", pa.string()),
            ("description", pa.string()),
            ("effective_date", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "advisors": {
        "fields": [
            ("advisor_id", pa.string()),
            ("advisor_code", pa.string()),
            ("full_name", pa.string()),
            ("branch_code", pa.string()),
            ("license_status", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "fee_schedules": {
        "fields": [
            ("fee_schedule_id", pa.string()),
            ("account_id", pa.string()),
            ("tier_name", pa.string()),
            ("rate_bps", pa.float64()),
            ("min_annual_fee", pa.float64()),
            ("billing_frequency", pa.string()),
            ("effective_from", pa.string()),
            ("updated_at", pa.string())
        ]
    },
    "statements": {
        "fields": [
            ("statement_id", pa.string()),
            ("account_id", pa.string()),
            ("statement_period", pa.string()),
            ("starting_balance", pa.float64()),
            ("ending_balance", pa.float64()),
            ("total_fees", pa.float64()),
            ("statement_url", pa.string()),
            ("generated_at", pa.string())
        ]
    }
}

COLLISION_ACCOUNT_ID = "ACC-COLLISION-999"

def generate_table_data(table_name, org_id, date_str, seed):
    rng = random.Random(f"{table_name}-{org_id}-{date_str}-{seed}")
    
    # We will generate rows with some duplicate updates to exercise deduplication
    rows = []
    
    if table_name == "accounts":
        # Base accounts
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "account_id": acc_id,
                "account_number": f"NUM-{1000 + i}",
                "account_name": f"Account {i} for {org_id.upper()}",
                "account_type": rng.choice(["BROKERAGE", "IRA", "TRUST"]),
                "currency": "USD",
                "status": "ACTIVE",
                "created_at": f"2026-01-0{i}T00:00:00Z",
                "updated_at": f"{date_str}T08:00:00Z"
            })
        
        # DELIBERATE MULTI-TENANT COLLISION:
        # Same account_id ACC-COLLISION-999 in org_alpha AND org_beta!
        if org_id in ["org_alpha", "org_beta"]:
            rows.append({
                "account_id": COLLISION_ACCOUNT_ID,
                "account_number": f"COLLISION-NUM-{org_id.upper()}",
                "account_name": f"Collision Account Owned By {org_id.upper()}",
                "account_type": "PRIVATE_WEALTH",
                "currency": "USD",
                "status": "ACTIVE",
                "created_at": "2026-02-01T10:00:00Z",
                "updated_at": f"{date_str}T10:30:00Z"
            })
            
        # Add a duplicate record for deduplication testing (same PK, later updated_at timestamp)
        if rows:
            dup = dict(rows[0])
            dup["updated_at"] = f"{date_str}T14:45:00Z"
            dup["status"] = "UPDATED_STATUS"
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
                "role": rng.choice(["PRIMARY_OWNER", "BENEFICIARY", "TRUSTEE"]),
                "updated_at": f"{date_str}T09:00:00Z"
            })

    elif table_name == "portfolios":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "portfolio_id": f"PORT-{org_id[-3:].upper()}-{200 + i}",
                "account_id": acc_id,
                "portfolio_name": f"Growth Portfolio {i}",
                "benchmark_code": rng.choice(["SP500", "MSCI_WORLD", "AGG"]),
                "risk_tolerance": rng.choice(["CONSERVATIVE", "MODERATE", "AGGRESSIVE"]),
                "is_active": 1,
                "updated_at": f"{date_str}T08:15:00Z"
            })
        if org_id in ["org_alpha", "org_beta"]:
            rows.append({
                "portfolio_id": f"PORT-COLLISION-{org_id[-3:].upper()}",
                "account_id": COLLISION_ACCOUNT_ID,
                "portfolio_name": f"Flagship Strategy {org_id.upper()}",
                "benchmark_code": "SP500",
                "risk_tolerance": "AGGRESSIVE",
                "is_active": 1,
                "updated_at": f"{date_str}T08:15:00Z"
            })

    elif table_name == "securities":
        tickers = [("AAPL", "Apple Inc", "EQUITY", "TECH"),
                   ("MSFT", "Microsoft Corp", "EQUITY", "TECH"),
                   ("BND", "Total Bond ETF", "FIXED_INCOME", "GOVT_BOND"),
                   ("NVDA", "Nvidia Corp", "EQUITY", "SEMICONDUCTORS"),
                   ("GLD", "SPDR Gold Trust", "COMMODITY", "PRECIOUS_METALS")]
        for idx, (sym, name, a_class, sec) in enumerate(tickers, start=1):
            rows.append({
                "security_id": f"SEC-{1000 + idx}",
                "symbol": sym,
                "security_name": name,
                "asset_class": a_class,
                "sector": sec,
                "exchange": "NASDAQ" if sym in ["AAPL", "MSFT", "NVDA"] else "NYSE",
                "updated_at": f"{date_str}T00:00:00Z"
            })

    elif table_name == "holdings":
        for i in range(1, 6):
            port_id = f"PORT-{org_id[-3:].upper()}-{200 + i}"
            sec_id = f"SEC-{1000 + ((i % 5) + 1)}"
            qty = round(10.0 * i + rng.uniform(0.5, 5.0), 4)
            price = 150.0 + rng.uniform(-10.0, 30.0)
            cost = 140.0 * qty
            mval = round(qty * price, 2)
            rows.append({
                "holding_id": f"HLD-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i}",
                "portfolio_id": port_id,
                "security_id": sec_id,
                "quantity": float(qty),
                "market_value": float(mval),
                "cost_basis": float(round(cost, 2)),
                "unrealized_gain_loss": float(round(mval - cost, 2)),
                "as_of_date": date_str,
                "updated_at": f"{date_str}T16:00:00Z"
            })

    elif table_name == "trades":
        for i in range(1, 5):
            port_id = f"PORT-{org_id[-3:].upper()}-{200 + i}"
            sec_id = f"SEC-{1000 + i}"
            units = round(5.0 * i, 2)
            px = round(120.0 + i * 15.0, 2)
            gross = round(units * px, 2)
            rows.append({
                "trade_id": f"TRD-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i:03d}",
                "portfolio_id": port_id,
                "security_id": sec_id,
                "trade_type": rng.choice(["BUY", "SELL"]),
                "trade_status": "FILLED",
                "units": float(units),
                "execution_price": float(px),
                "gross_amount": float(gross),
                "commission_fee": 4.95,
                "executed_at": f"{date_str}T14:{i*10:02d}:00Z",
                "updated_at": f"{date_str}T14:{i*10:02d}:00Z"
            })

    elif table_name == "transactions":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            port_id = f"PORT-{org_id[-3:].upper()}-{200 + i}"
            amount = round(500.0 * i + rng.uniform(10.0, 50.0), 2)
            rows.append({
                "transaction_id": f"TXN-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i:04d}",
                "account_id": acc_id,
                "portfolio_id": port_id,
                "transaction_type": rng.choice(["DEPOSIT", "DIVIDEND", "FEE", "WITHDRAWAL"]),
                "amount": float(amount),
                "currency": "USD",
                "description": f"Standard cash movement {i}",
                "effective_date": date_str,
                "updated_at": f"{date_str}T18:00:00Z"
            })

    elif table_name == "advisors":
        for i in range(1, 4):
            rows.append({
                "advisor_id": f"ADV-{org_id[-3:].upper()}-{i}",
                "advisor_code": f"CODE-{org_id[-3:].upper()}-{i:02d}",
                "full_name": rng.choice(["Franklin Sterling", "Grace Hopper", "Marcus Vance"]),
                "branch_code": f"BR-{100 + i}",
                "license_status": "ACTIVE",
                "updated_at": f"{date_str}T07:30:00Z"
            })

    elif table_name == "fee_schedules":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "fee_schedule_id": f"FEE-{org_id[-3:].upper()}-{i}",
                "account_id": acc_id,
                "tier_name": rng.choice(["TIER_A", "TIER_B", "CUSTOM"]),
                "rate_bps": round(rng.uniform(25.0, 75.0), 2),
                "min_annual_fee": 150.0,
                "billing_frequency": "QUARTERLY",
                "effective_from": "2026-01-01",
                "updated_at": f"{date_str}T06:00:00Z"
            })

    elif table_name == "statements":
        for i in range(1, 6):
            acc_id = f"ACC-{org_id[-3:].upper()}-{100 + i}"
            rows.append({
                "statement_id": f"STMT-{org_id[-3:].upper()}-{date_str.replace('-','')}-{i}",
                "account_id": acc_id,
                "statement_period": date_str[:7],
                "starting_balance": float(50000.0 + i * 10000.0),
                "ending_balance": float(51200.0 + i * 10500.0),
                "total_fees": float(45.50 * i),
                "statement_url": f"https://vault.{org_id}.com/statements/{acc_id}/{date_str}.pdf",
                "generated_at": f"{date_str}T23:59:59Z"
            })
            
    return rows

def generate_and_save_dataset(base_output_dir):
    """
    Generates synthetic ORC files structured as:
    <base_output_dir>/<table_name>/organisation_id=<org>/processing_date=<date>/data.orc
    """
    os.makedirs(base_output_dir, exist_ok=True)
    summary = {}

    for table_name, schema_info in TABLE_DEFINITIONS.items():
        summary[table_name] = 0
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
                
                # Build pyarrow arrays
                arrays = []
                for field_name, field_type in fields:
                    col_vals = [row[field_name] for row in data_rows]
                    arrays.append(pa.array(col_vals, type=field_type))
                
                table = pa.Table.from_arrays(arrays, schema=pa_schema)
                orc.write_table(table, file_path)
                summary[table_name] += len(data_rows)

    return summary

if __name__ == "__main__":
    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw_lake"))
    print(f"Generating synthetic ORC data into: {out_dir}")
    stats = generate_and_save_dataset(out_dir)
    print("Generation complete! Summary rows generated:")
    for t, count in stats.items():
        print(f"  - {t:15s}: {count:4d} rows across 3 orgs x 7 dates")
