"""
IBKR Trade Report — PnL Graphical Report
Reads the multi-account CSV and produces:
  1. PnL by asset class (bar chart)
  2. Monthly PnL waterfall
  3. Cumulative PnL by asset class (line chart)
  4. Summary panel with buy/sell cross-check

If the CSV contains trades from multiple years, the user is prompted to pick one.
"""

import csv
import glob
import os
import sys
from collections import defaultdict, deque
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
import numpy as np

# ── Locate CSV ──────────────────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
csvfiles = sorted(glob.glob(os.path.join(script_dir, "*.csv")))
if not csvfiles:
    raise FileNotFoundError("No CSV file found next to this script.")

if len(csvfiles) == 1:
    CSV_PATH = csvfiles[0]
else:
    print("Multiple CSV files found:\n")
    for i, path in enumerate(csvfiles, 1):
        print(f"  {i}. {os.path.basename(path)}")
    while True:
        choice = input(f"\nWhich file? [1-{len(csvfiles)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(csvfiles):
            CSV_PATH = csvfiles[int(choice) - 1]
            break
        print(f"  Enter a number between 1 and {len(csvfiles)}")

print(f"Reading: {os.path.basename(CSV_PATH)}")

# ── First pass: discover years present in the data ──────────────────────────
SKIP_ACCOUNT = {"ClientAccountID", ""}
SKIP_TXN     = {"TransactionType", "TradeCancel", ""}

years_found = set()
with open(CSV_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get("ClientAccountID", "") in SKIP_ACCOUNT:
            continue
        td = row.get("TradeDate", "")
        if td in ("TradeDate", "") or len(td) < 4:
            continue
        years_found.add(td[:4])

years_sorted = sorted(years_found)

if not years_sorted:
    print("No valid trade dates found in the CSV.")
    sys.exit(1)

if len(years_sorted) == 1:
    chosen_year = years_sorted[0]
    print(f"Single year in data: {chosen_year}")
else:
    print(f"\nMultiple years found in the data: {', '.join(years_sorted)}")
    # Check for command-line argument first
    if len(sys.argv) > 1 and sys.argv[1] in years_sorted:
        chosen_year = sys.argv[1]
    else:
        while True:
            answer = input(f"Which year to report on? [{'/'.join(years_sorted)}]: ").strip()
            if answer in years_sorted:
                chosen_year = answer
                break
            print(f"  Please enter one of: {', '.join(years_sorted)}")

print(f"Generating report for: {chosen_year}\n")

# ── Second pass: parse data for chosen year ─────────────────────────────────
monthly_pnl    = defaultdict(float)
asset_pnl      = defaultdict(float)
asset_monthly  = defaultdict(lambda: defaultdict(float))

total_sells      = 0.0
total_buys       = 0.0
adj_sells        = 0.0
adj_buys         = 0.0
total_fifo       = 0.0
total_commission = 0.0
entry_commission = 0.0
exit_commission  = 0.0
asset_commission = defaultdict(float)
asset_entry_comm = defaultdict(float)
asset_exit_comm  = defaultdict(float)

# ── FIFO pool for matching opening trades to closing trades ─────────────────
# Key: (symbol, "LONG" | "SHORT")
# Value: deque of [remaining_qty, remaining_proceeds]  (both positive)
open_pool = defaultdict(deque)

# Adj. Buys breakdown: actual close costs + entry costs (actual vs implied)
adj_buys_close_actual  = 0.0   # BUY to close short: actual buyback cost
adj_buys_entry_actual  = 0.0   # SELL to close long: entry cost matched from data
adj_buys_entry_implied = 0.0   # SELL to close long: entry cost implied (prior year)

# Adj. Sells breakdown: actual close proceeds + entry credits (actual vs implied)
adj_sells_close_actual  = 0.0  # SELL to close long: actual sale proceeds
adj_sells_entry_actual  = 0.0  # BUY to close short: entry credit matched from data
adj_sells_entry_implied = 0.0  # BUY to close short: entry credit implied (prior year)

actual_entry_closes  = 0
implied_entry_closes = 0

def consume_pool(pool, needed_qty):
    """Consume up to needed_qty from the FIFO pool.
    Returns (consumed_qty, consumed_proceeds)."""
    consumed_qty = 0.0
    consumed_proceeds = 0.0
    while needed_qty > 1e-9 and pool:
        lot = pool[0]
        take = min(needed_qty, lot[0])
        if take >= lot[0] - 1e-9:          # consume entire lot
            consumed_qty += lot[0]
            consumed_proceeds += lot[1]
            pool.popleft()
        else:                               # partial consumption
            frac = take / lot[0]
            partial = lot[1] * frac
            consumed_qty += take
            consumed_proceeds += partial
            lot[0] -= take
            lot[1] -= partial
        needed_qty -= take
    return consumed_qty, consumed_proceeds

with open(CSV_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        acct = row.get("ClientAccountID", "")
        if acct in SKIP_ACCOUNT:
            continue
        txn = row.get("TransactionType", "")
        if txn in SKIP_TXN:
            continue

        trade_date = row.get("TradeDate", "")
        if trade_date in ("TradeDate", ""):
            continue

        # ── Parse common fields (needed for ALL years for pool management) ──
        symbol = row.get("Symbol", "")
        bs     = row.get("Buy/Sell", "")
        oci    = row.get("Open/CloseIndicator", "")
        try:
            proceeds = float(row["Proceeds"])
        except (ValueError, KeyError):
            proceeds = 0.0
        try:
            qty = abs(float(row["Quantity"]))
        except (ValueError, KeyError):
            qty = 0.0
        try:
            fifo = float(row["FifoPnlRealized"])
        except (ValueError, KeyError):
            fifo = 0.0

        # ── FIFO pool: register opening trades from ALL years ─────────────
        # This way, positions opened before the chosen year have actual entry
        # prices available when they close in the chosen year.
        if txn == "ExchTrade" and "O" in oci and qty > 0:
            if bs == "BUY":
                open_pool[(symbol, "LONG")].append([qty, abs(proceeds)])
            elif bs == "SELL":
                open_pool[(symbol, "SHORT")].append([qty, proceeds])

        # ── For non-chosen years: consume pool on closes to keep it accurate,
        #    but don't accumulate into any report accumulators ──────────────
        is_chosen_year = trade_date[:4] == chosen_year

        if not is_chosen_year:
            is_close = "C" in oci and qty > 0 and txn in ("ExchTrade", "BookTrade")
            if is_close:
                if bs == "SELL":
                    consume_pool(open_pool[(symbol, "LONG")], qty)
                elif bs == "BUY":
                    consume_pool(open_pool[(symbol, "SHORT")], qty)
            continue

        # ══════════════════════════════════════════════════════════════════════
        # From here on: chosen year only
        # ══════════════════════════════════════════════════════════════════════
        month = trade_date[:7]
        asset = row.get("AssetClass", "") or "UNKNOWN"

        # ── FifoPnlRealized ─────────────────────────────────────────────────
        monthly_pnl[month]          += fifo
        asset_pnl[asset]            += fifo
        asset_monthly[asset][month] += fifo
        total_fifo                  += fifo

        # ── Commission ──────────────────────────────────────────────────────
        try:
            comm = float(row["IBCommission"])
        except (ValueError, KeyError):
            comm = 0.0
        total_commission        += comm
        asset_commission[asset] += comm

        if "O" in oci:
            entry_commission        += comm
            asset_entry_comm[asset] += comm
        if "C" in oci:
            exit_commission         += comm
            asset_exit_comm[asset]  += comm

        # ── RAW: actual cash flows for ExchTrades in the chosen year ────────
        if txn == "ExchTrade":
            if bs == "SELL":
                total_sells += proceeds
            elif bs == "BUY":
                total_buys += abs(proceeds)

        # ── ADJUSTED check: use actual entry when available, implied otherwise
        # For closing trades (ExchTrade and BookTrade):
        #   1. Determine close value:
        #      - ExchTrade: |Proceeds| (actual cash)
        #      - BookTrade: ClosePrice * Multiplier * |Qty| (settlement value)
        #   2. Consume matching opening lots from the FIFO pool (may include
        #      lots from prior years if available in the data)
        #   3. For any unmatched portion, derive implied entry from FifoPnlRealized
        is_close = "C" in oci and qty > 0 and txn in ("ExchTrade", "BookTrade")
        if is_close:
            if txn == "ExchTrade":
                close_value = abs(proceeds)
            else:
                try:
                    close_price = float(row.get("ClosePrice") or 0)
                    multiplier  = float(row.get("Multiplier") or 1)
                except (ValueError, TypeError):
                    close_price = 0.0
                    multiplier  = 1.0
                close_value = close_price * multiplier * qty

            if bs == "SELL":                        # closing a LONG position
                total_entry = close_value - fifo
                pool_key = (symbol, "LONG")
                matched_qty, matched_cost = consume_pool(open_pool[pool_key], qty)
                implied_cost = total_entry - matched_cost

                adj_sells += close_value
                adj_buys  += matched_cost + implied_cost

                adj_sells_close_actual += close_value
                adj_buys_entry_actual  += matched_cost
                adj_buys_entry_implied += implied_cost

                if matched_qty > 1e-9:
                    actual_entry_closes += 1
                if qty - matched_qty > 1e-9:
                    implied_entry_closes += 1

            elif bs == "BUY":                       # closing a SHORT position
                total_entry = close_value + fifo
                pool_key = (symbol, "SHORT")
                matched_qty, matched_credit = consume_pool(open_pool[pool_key], qty)
                implied_credit = total_entry - matched_credit

                adj_buys  += close_value
                adj_sells += matched_credit + implied_credit

                adj_buys_close_actual   += close_value
                adj_sells_entry_actual  += matched_credit
                adj_sells_entry_implied += implied_credit

                if matched_qty > 1e-9:
                    actual_entry_closes += 1
                if qty - matched_qty > 1e-9:
                    implied_entry_closes += 1

gross_pnl_check = total_sells - total_buys
adj_pnl_check   = adj_sells - adj_buys

# ── Sort months ──────────────────────────────────────────────────────────────
months = sorted(monthly_pnl.keys())
pnl_values = [monthly_pnl[m] for m in months]
cumulative  = np.cumsum(pnl_values)

assets = [a for a in sorted(asset_pnl.keys()) if a not in ("CASH", "UNKNOWN")]

# ── Colour palette ───────────────────────────────────────────────────────────
ASSET_COLORS = {
    "STK": "#2196F3",
    "FUT": "#F44336",
    "OPT": "#4CAF50",
    "CASH": "#9E9E9E",
}
pos_color = "#4CAF50"
neg_color = "#F44336"

# ── Layout ───────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(18, 13), facecolor="#1a1a2e")
fig.suptitle(f"IBKR Trade Report  -  PnL Dashboard  -  {chosen_year}",
             fontsize=18, fontweight="bold", color="white", y=0.98)

gs = gridspec.GridSpec(
    3, 2,
    figure=fig,
    hspace=0.48, wspace=0.3,
    left=0.07, right=0.97,
    top=0.93, bottom=0.07
)

ax_bar  = fig.add_subplot(gs[0, 0])
ax_wf   = fig.add_subplot(gs[1, :])
ax_cum  = fig.add_subplot(gs[0, 1])
ax_sum  = fig.add_subplot(gs[2, :])

PANEL_BG = "#16213e"
TICK_COL  = "#c0c0c0"
GRID_COL  = "#2a2a4a"

for ax in (ax_bar, ax_wf, ax_cum, ax_sum):
    ax.set_facecolor(PANEL_BG)
    ax.tick_params(colors=TICK_COL, labelsize=9)
    for spine in ax.spines.values():
        spine.set_edgecolor("#3a3a5a")

def money(v):
    return f"${v:+,.0f}" if v != 0 else "$0"

def fmt_k(x, _):
    return f"${x/1000:+.1f}k" if abs(x) >= 1000 else f"${x:+.0f}"

# ── 1. PnL by Asset Class ────────────────────────────────────────────────────
ax_bar.set_title("Realized PnL by Asset Class", color="white", fontsize=11, pad=8)
bar_vals = [asset_pnl[a] for a in assets]
bars = ax_bar.bar(assets, bar_vals, width=0.5, zorder=3)

for bar, val in zip(bars, bar_vals):
    bar.set_color(pos_color if val >= 0 else neg_color)
    ax_bar.text(
        bar.get_x() + bar.get_width() / 2,
        val + (max(bar_vals)*0.02 if val >= 0 else min(bar_vals)*0.02),
        money(val),
        ha="center", va="bottom" if val >= 0 else "top",
        color="white", fontsize=9, fontweight="bold"
    )

total_bar_val = sum(bar_vals)
ax_bar.axhline(0, color="#555577", linewidth=0.8)
ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
ax_bar.set_ylabel("USD", color=TICK_COL, fontsize=9)
ax_bar.set_xticks(range(len(assets)))
ax_bar.set_xticklabels(assets, color="white", fontsize=10)
ax_bar.grid(axis="y", color=GRID_COL, linewidth=0.7, zorder=0)
ax_bar.tick_params(axis="x", colors="white")

ax_bar.text(
    0.97, 0.05,
    f"Total: {money(total_bar_val)}",
    transform=ax_bar.transAxes,
    ha="right", va="bottom",
    color="white", fontsize=10, fontweight="bold",
    bbox=dict(facecolor="#0f3460", edgecolor="#4a90d9", boxstyle="round,pad=0.3")
)

# ── 2. Cumulative total PnL ──────────────────────────────────────────────────
ax_cum.set_title("Cumulative PnL (all assets)", color="white", fontsize=11, pad=8)
x_pos = np.arange(len(months))
ax_cum.plot(x_pos, cumulative, color="#00bcd4", linewidth=2, zorder=3)
ax_cum.fill_between(x_pos, cumulative, alpha=0.15, color="#00bcd4", zorder=2)
ax_cum.axhline(0, color="#555577", linewidth=0.8)
ax_cum.set_xticks(x_pos)
ax_cum.set_xticklabels([m[5:] for m in months], rotation=45, ha="right",
                        color=TICK_COL, fontsize=8)
ax_cum.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
ax_cum.set_ylabel("USD", color=TICK_COL, fontsize=9)
ax_cum.grid(color=GRID_COL, linewidth=0.7, zorder=0)

ax_cum.annotate(
    money(cumulative[-1]),
    xy=(x_pos[-1], cumulative[-1]),
    xytext=(-45, 10), textcoords="offset points",
    color="white", fontsize=9, fontweight="bold",
    arrowprops=dict(arrowstyle="->", color="#aaaacc", lw=1)
)

# ── 3. Monthly Waterfall ─────────────────────────────────────────────────────
ax_wf.set_title("Monthly Realized PnL  (waterfall)", color="white", fontsize=11, pad=8)
x_pos_wf = np.arange(len(months))
bar_colors_wf = [pos_color if v >= 0 else neg_color for v in pnl_values]
bars_wf = ax_wf.bar(x_pos_wf, pnl_values, color=bar_colors_wf, width=0.6, zorder=3)

for bar, val in zip(bars_wf, pnl_values):
    ax_wf.text(
        bar.get_x() + bar.get_width() / 2,
        val,
        money(val),
        ha="center", va="bottom" if val >= 0 else "top",
        color="white", fontsize=8,
    )

ax_wf.axhline(0, color="#555577", linewidth=0.8)
ax_wf.set_xticks(x_pos_wf)
ax_wf.set_xticklabels(months, rotation=30, ha="right", color=TICK_COL, fontsize=9)
ax_wf.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
ax_wf.set_ylabel("USD", color=TICK_COL, fontsize=9)
ax_wf.grid(axis="y", color=GRID_COL, linewidth=0.7, zorder=0)

for asset in assets:
    vals = [asset_monthly[asset].get(m, 0.0) for m in months]
    cumv = np.cumsum(vals)
    color = ASSET_COLORS.get(asset, "#9C27B0")
    ax_wf.plot(x_pos_wf, cumv, color=color, linewidth=1.5,
               linestyle="--", alpha=0.7, zorder=4, label=asset)

ax_wf.legend(
    loc="upper left", facecolor=PANEL_BG, edgecolor="#3a3a5a",
    labelcolor="white", fontsize=8
)

# ── 4. Summary / Cross-check panel ──────────────────────────────────────────
ax_sum.axis("off")
ax_sum.set_title("PnL Summary & Buy/Sell Cross-Check", color="white", fontsize=11, pad=8)

raw_diff = gross_pnl_check - total_fifo
adj_diff = adj_pnl_check - total_fifo

def match_label(d):
    return "MATCH" if abs(d) < 0.10 else f"off by ${d:+,.2f}"

net_pnl_after_comm = total_fifo + total_commission

col_labels = ["Metric", "Value", "Note"]
rows_data = [
    ["Realized PnL  (FifoPnlRealized)",
     f"${total_fifo:+,.2f}",
     "IBKR FIFO on closing trades (before commission)"],
    ["   STK  pnl / comm (entry/exit)",
     f"${asset_pnl.get('STK',0):+,.2f}  /  ${asset_commission.get('STK',0):,.2f}"
     f"  ({asset_entry_comm.get('STK',0):,.2f} / {asset_exit_comm.get('STK',0):,.2f})", ""],
    ["   OPT  pnl / comm (entry/exit)",
     f"${asset_pnl.get('OPT',0):+,.2f}  /  ${asset_commission.get('OPT',0):,.2f}"
     f"  ({asset_entry_comm.get('OPT',0):,.2f} / {asset_exit_comm.get('OPT',0):,.2f})", ""],
    ["   FUT  pnl / comm (entry/exit)",
     f"${asset_pnl.get('FUT',0):+,.2f}  /  ${asset_commission.get('FUT',0):,.2f}"
     f"  ({asset_entry_comm.get('FUT',0):,.2f} / {asset_exit_comm.get('FUT',0):,.2f})", ""],
    ["Total Commission  (entry / exit)",
     f"${total_commission:,.2f}  (${entry_commission:,.2f} / ${exit_commission:,.2f})",
     "Negative = cost"],
    ["Net PnL after commission",
     f"${net_pnl_after_comm:+,.2f}",
     "FifoPnlRealized + IBCommission"],
    ["", "", ""],
    ["RAW check  (actual Proceeds in the data, ExchTrade only)",
     "", ""],
    ["  Total Sells",
     f"${total_sells:,.2f}", ""],
    ["  Total Buys",
     f"${total_buys:,.2f}", ""],
    ["  Sells - Buys",
     f"${gross_pnl_check:+,.2f}",
     f"off by ${raw_diff:+,.2f} vs FifoPnl  (open positions at period boundaries)"],
    ["", "", ""],
    ["ADJUSTED check  (actual entry when in data, implied otherwise)",
     "", f"{actual_entry_closes} closes matched, {implied_entry_closes} implied"],
    ["  Adj. Sells",
     f"${adj_sells:,.2f}",
     f"close proceeds: {adj_sells_close_actual:,.0f}"
     f"  +  entry from data: {adj_sells_entry_actual:,.0f}"
     f"  +  implied: {adj_sells_entry_implied:,.0f}"],
    ["  Adj. Buys",
     f"${adj_buys:,.2f}",
     f"close costs: {adj_buys_close_actual:,.0f}"
     f"  +  entry from data: {adj_buys_entry_actual:,.0f}"
     f"  +  implied: {adj_buys_entry_implied:,.0f}"],
    ["  Adj. Sells - Buys",
     f"${adj_pnl_check:+,.2f}",
     f"{match_label(adj_diff)} vs FifoPnl (excl. BookTrade PnL: ${total_fifo - adj_pnl_check:+,.2f})"],
]

# Draw as a styled table
col_x   = [0.01, 0.38, 0.58]
row_h   = 0.055
y_start = 0.97

for i, label in enumerate(col_labels):
    ax_sum.text(col_x[i], y_start, label,
                transform=ax_sum.transAxes,
                color="#aaddff", fontsize=9, fontweight="bold",
                va="top", family="monospace")

ax_sum.plot([0.01, 0.99], [y_start - 0.03, y_start - 0.03],
            color="#3a3a5a", linewidth=0.8, transform=ax_sum.transAxes)

separator_rows = {6, 11}
for ri, row in enumerate(rows_data):
    y = y_start - 0.06 - ri * row_h
    if ri in separator_rows:
        ax_sum.plot([0.01, 0.99], [y + row_h * 0.5, y + row_h * 0.5],
                    color="#2a2a4a", linewidth=0.5, transform=ax_sum.transAxes)
        continue
    is_total = any(w in row[0] for w in ("Realized", "Total", "Sells", "Buys", "Net PnL"))
    txt_color = "white" if is_total else "#c0c0c0"

    # Value colour by sign
    if row[1].startswith("$+") or (row[1].startswith("$") and "-" not in row[1] and row[1] != "$0.00"):
        val_sign_color = pos_color
    elif "-" in row[1]:
        val_sign_color = neg_color
    else:
        val_sign_color = "white"

    ax_sum.text(col_x[0], y, row[0],
                transform=ax_sum.transAxes,
                color=txt_color, fontsize=9, va="top", family="monospace")
    ax_sum.text(col_x[1], y, row[1],
                transform=ax_sum.transAxes,
                color=val_sign_color, fontsize=9, va="top",
                fontweight="bold" if is_total else "normal",
                family="monospace")
    ax_sum.text(col_x[2], y, row[2],
                transform=ax_sum.transAxes,
                color="#888888", fontsize=8, va="top", family="monospace")

# ── Save & show ───────────────────────────────────────────────────────────────
out_path = os.path.join(script_dir, f"pnl_report_{chosen_year}.png")
fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\nReport saved: {out_path}")
print(f"\nFIFO Realized PnL      : ${total_fifo:+,.2f}")
print(f"Total Commission       : ${total_commission:,.2f}  (entry: ${entry_commission:,.2f}  exit: ${exit_commission:,.2f})")
print(f"Net PnL after comm     : ${net_pnl_after_comm:+,.2f}")
print(f"\nRAW  (ExchTrade actual proceeds in the data)")
print(f"  Total Sells        : ${total_sells:,.2f}")
print(f"  Total Buys         : ${total_buys:,.2f}")
print(f"  Sells - Buys       : ${gross_pnl_check:+,.2f}  (off by ${raw_diff:+,.2f} vs FIFO)")
print(f"\nADJUSTED  (actual entry when in data, implied otherwise)")
print(f"  Adj. Sells         : ${adj_sells:,.2f}")
print(f"    close proceeds   : ${adj_sells_close_actual:,.2f}")
print(f"    entry from data  : ${adj_sells_entry_actual:,.2f}")
print(f"    entry implied    : ${adj_sells_entry_implied:,.2f}")
print(f"  Adj. Buys          : ${adj_buys:,.2f}")
print(f"    close costs      : ${adj_buys_close_actual:,.2f}")
print(f"    entry from data  : ${adj_buys_entry_actual:,.2f}")
print(f"    entry implied    : ${adj_buys_entry_implied:,.2f}")
print(f"  Sells - Buys       : ${adj_pnl_check:+,.2f}  ({match_label(adj_diff)} vs FIFO)")
print(f"  Matched closes: {actual_entry_closes}  |  Implied closes: {implied_entry_closes}")

plt.close(fig)
