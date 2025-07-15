# Dune Analytics - Morpho Blue Vault Metrics

## 📊 Metrics Overview

| Metric | Description |
|--------|-------------|
| `borrow_amount_usd` | Cumulative borrowing activity in USD |
| `supply_amount_usd` | Cumulative lending/supply activity in USD |
| `tvl_amount_usd` | Total Value Locked in the protocol |
| `pure_tvl_amount_usd` | Net available liquidity after accounting for loans |
| `ratio` | Efficiency ratio (pure TVL / supply) |
| `day` | Time dimension for trend analysis |

## 🔗 Data Sources & Links

### **Google Sheets**
- **Vault Data & Links**: [Morpho Blue Vaults Dashboard](https://docs.google.com/spreadsheets/d/1eZQtVwVJBTJvEFV48Y59ZhKa22Andd6qWVtLXUKZ-sM/edit?gid=0#gid=0)

### **Dune Queries**
All vault data is sourced from Dune Analytics queries. Individual query links are available in the Google Sheet above.

## 📈 Metric Calculations

### **borrow_amount_usd**
Cumulative borrowed amount in USD, calculated using window functions to track total borrowing over time. Includes:
- ✅ Positive amounts from borrow events
- ❌ Negative amounts from repayments and liquidations  
- ➕ Interest accrual

### **supply_amount_usd**
Cumulative supplied amount in USD, incorporating:
- ✅ Supply events (positive)
- ❌ Withdrawals (negative)
- ❌ Bad debt from liquidations (negative)
- ➕ Earned interest (positive)

### **tvl_amount_usd**
Total Value Locked representing the sum of supplied assets and collateral deposits, providing a comprehensive view of vault asset holdings.

### **pure_tvl_amount_usd**
Pure TVL calculation that subtracts borrowed amounts from total value locked, showing the net asset position of the vault.

### **ratio**
Pure TVL to Supply ratio, indicating the efficiency of capital utilization and the proportion of assets actively generating yield versus borrowed funds.

## 🧮 Calculation Methodology

### **Supply Interactions**
```
Supply Events: + amounts from supply operations
Withdraw Events: - amounts from withdrawal operations  
Liquidation Bad Debt: - amounts from bad debt socialization
Interest Accrual: + amounts from interest earned
```

### **Borrow Interactions**
```
Borrow Events: + amounts from borrowing operations
Repay Events: - amounts from repayment operations
Liquidation Repaid: - amounts from liquidation repayments
Interest Accrual: + amounts from interest charged
```

### **Collateral Interactions**
```
Supply Collateral: + amounts from collateral deposits
Withdraw Collateral: - amounts from collateral withdrawals
Liquidation Seized: - amounts from seized collateral
```

## 📊 Final Output Metrics

### **Cumulative Calculations**
All metrics use window functions for cumulative tracking:

| Metric | Formula |
|--------|---------|
| `borrow_amount_usd` | `SUM(borrow_amount) OVER (ORDER BY day)` |
| `supply_amount_usd` | `SUM(supply_amount) OVER (ORDER BY day)` |
| `tvl_amount_usd` | `SUM(supply_amount) + SUM(collateral_amount) OVER (ORDER BY day)` |
| `pure_tvl_amount_usd` | `SUM(supply_amount) + SUM(collateral_amount) - SUM(borrow_amount) OVER (ORDER BY day)` |
| `ratio` | `pure_tvl_amount / supply_amount` |

## 🗂️ Dune Analytics Data Structure

### **Main Categories**
- **Raw Data**: `blocks`, `transactions`, `traces`, `logs`
- **Decoded Data**: Event tables (`evt_`), call tables (`call_`), project-specific tables
- **Curated Data**: `prices`, `dex.trades`, `nft.trades`, `tokens`, `labels`
- **Community Data**: `flashbots`, `reservoir`, `farcaster`, `lens`
- **User Generated**: `dataset_`, `query_`, `result_` tables

### **Price Data Structure**
- `prices.day`: Daily token prices with timestamp, price, volume, source
- `prices.hour`: Hourly price data
- `prices.minute`: Minute-by-minute prices
- `prices.latest`: Most recent prices