# Schema Dictionary & Data Models

This document details the schema models across the Medallion stages for the **DATA-1** Financial Platform.

---

## 1. Raw Lake & Bronze Tables (10 Tables)

### 1. `accounts`
| Column | Type | Description |
|---|---|---|
| `organisation_id` | String | Organization tenant ID (`org_alpha`, `org_beta`, `org_gamma`) |
| `processing_date` | Date | Snapshot extraction date (`2026-03-01` to `2026-03-07`) |
| `account_id` | String | Primary account key (e.g., `ACC-COLLISION-999`, `ACC-alpha-0001`) |
| `account_name` | String | Legal title of the brokerage or custody account |
| `status` | String | Status (`OPEN`, `ACTIVE`, `PENDING_KYC`, `CLOSED`) |
| `currency` | String | Base operating currency (`USD`, `EUR`, `GBP`) |
| `advisor_id` | String | Foreign key to assigned wealth manager |
| `opened_at` | DateTime | Timestamp when account was opened |
| `updated_at` | DateTime | Last audit modification timestamp |

### 2. `contacts`
| Column | Type | Description |
|---|---|---|
| `contact_id` | String | Primary contact identifier |
| `account_id` | String | Foreign key to `accounts` |
| `first_name` | String | Given name |
| `last_name` | String | Surname |
| `email` | String | Contact email address |
| `phone` | String | International telephone format |
| `updated_at` | DateTime | Audit timestamp |

### 3. `portfolios`
| Column | Type | Description |
|---|---|---|
| `portfolio_id` | String | Primary portfolio key |
| `account_id` | String | Foreign key to `accounts` |
| `portfolio_name` | String | Portfolio mandate name |
| `strategy` | String | Investment strategy (`Growth`, `Balanced`, `Value`, `Income`) |
| `risk_profile` | String | Risk tolerance (`Conservative`, `Moderate`, `Aggressive`) |
| `updated_at` | DateTime | Audit timestamp |

### 4. `securities`
| Column | Type | Description |
|---|---|---|
| `security_id` | String | Security identifier (ISIN / CUSIP equivalent) |
| `ticker` | String | Market exchange ticker symbol (`AAPL`, `MSFT`, `BND`, etc.) |
| `security_name` | String | Full legal instrument name |
| `asset_class` | String | Asset class (`Equity`, `Fixed Income`, `Commodity`, `Cash`) |
| `currency` | String | Trading currency |
| `updated_at` | DateTime | Audit timestamp |

### 5. `holdings`
| Column | Type | Description |
|---|---|---|
| `holding_id` | String | Primary holding position key |
| `portfolio_id` | String | Foreign key to `portfolios` |
| `security_id` | String | Foreign key to `securities` |
| `quantity` | Float64 | Total shares/contracts held |
| `market_value` | Float64 | Current mark-to-market valuation |
| `cost_basis` | Float64 | Original acquisition book cost |
| `as_of_date` | Date | Effective valuation date |
| `updated_at` | DateTime | Audit timestamp |

### 6. `trades`
| Column | Type | Description |
|---|---|---|
| `trade_id` | String | Primary execution ticket identifier |
| `portfolio_id` | String | Foreign key to `portfolios` |
| `security_id` | String | Foreign key to `securities` |
| `side` | String | Order side (`BUY`, `SELL`) |
| `quantity` | Float64 | Executed quantity |
| `price` | Float64 | Execution unit fill price |
| `trade_value` | Float64 | Gross consideration amount |
| `commission` | Float64 | Broker transaction fee |
| `executed_at` | DateTime | Execution timestamp |
| `updated_at` | DateTime | Audit timestamp |

### 7. `transactions`
| Column | Type | Description |
|---|---|---|
| `transaction_id` | String | Primary ledger entry key |
| `account_id` | String | Foreign key to `accounts` |
| `transaction_type` | String | Cash movement type (`DEPOSIT`, `WITHDRAWAL`, `DIVIDEND`, `FEE`) |
| `amount` | Float64 | Cash transaction value |
| `currency` | String | Currency |
| `description` | String | Ledger narration |
| `transaction_date`| Date | Settlement date |
| `updated_at` | DateTime | Audit timestamp |

### 8. `advisors`
| Column | Type | Description |
|---|---|---|
| `advisor_id` | String | Primary advisor key |
| `first_name` | String | Given name |
| `last_name` | String | Surname |
| `email` | String | Business email |
| `branch` | String | Branch office location |
| `aum_target` | Float64 | Annual AUM quota in USD |
| `updated_at` | DateTime | Audit timestamp |

### 9. `fee_schedules`
| Column | Type | Description |
|---|---|---|
| `fee_schedule_id`| String | Primary fee plan key |
| `account_id` | String | Foreign key to `accounts` |
| `fee_type` | String | Fee schedule type (`MANAGEMENT_FEE`, `PERFORMANCE_FEE`, `CUSTODY_FEE`) |
| `rate_bps` | Float64 | Fee rate in basis points (1 bps = 0.01%) |
| `billing_frequency`| String | Billing cycle (`MONTHLY`, `QUARTERLY`, `ANNUALLY`) |
| `effective_date` | Date | Contractual start date |
| `updated_at` | DateTime | Audit timestamp |

### 10. `statements`
| Column | Type | Description |
|---|---|---|
| `statement_id` | String | Primary statement document key |
| `account_id` | String | Foreign key to `accounts` |
| `period_start` | Date | Accounting period start |
| `period_end` | Date | Accounting period end |
| `opening_balance`| Float64 | Starting cash + securities value |
| `closing_balance`| Float64 | Ending valuation balance |
| `currency` | String | Reporting currency |
| `updated_at` | DateTime | Audit timestamp |

---

## 2. Silver Enriched Relation
- **Name:** `silver_portfolio_holdings_enriched`
- **Grain:** One row per holding position per organization per processing date.
- **Join Keys:**
  - `holdings.portfolio_id = portfolios.portfolio_id AND holdings.organisation_id = portfolios.organisation_id`
  - `portfolios.account_id = accounts.account_id AND portfolios.organisation_id = accounts.organisation_id`
  - `holdings.security_id = securities.security_id AND holdings.organisation_id = securities.organisation_id`

---

## 3. Gold Aggregated Metric Models
1. **`gold_daily_organisation_aum`**:
   - `organisation_id`, `processing_date` $\rightarrow$ `total_market_value`, `total_cost_basis`, `position_count`.
2. **`gold_organisation_asset_allocation`**:
   - `organisation_id`, `processing_date`, `asset_class` $\rightarrow$ `total_allocation_value`, `asset_count`.
3. **`gold_organisation_trading_volume`**:
   - `organisation_id`, `processing_date`, `side` $\rightarrow$ `total_gross_volume`, `total_commissions`, `total_quantity`, `trade_count`.
