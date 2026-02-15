#!/usr/bin/env python3
"""PIR Capture Efficiency Analysis — Plots 2 & 3"""

import csv
import json
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from scipy.stats import gaussian_kde

OUT = Path(__file__).parent / "out"

# ── Load data ─────────────────────────────────────────────────────────────────

with open(OUT / "summary.csv") as f:
    summary = {r["tx_hash"]: r for r in csv.DictReader(f)}

proto_tx_hashes = set(summary.keys())

proto_swaps: dict[str, list[dict]] = defaultdict(list)
with open(OUT / "tx_pool_swaps.jsonl") as f:
    for line in f:
        d = json.loads(line)
        proto_swaps[d["tx_hash"]].append(d)

market_swaps: dict[str, list[dict]] = defaultdict(list)
with open(OUT / "next_blocks_pool_swaps.jsonl") as f:
    for line in f:
        d = json.loads(line)
        if d["swap_tx_hash"] not in proto_tx_hashes:
            market_swaps[d["origin_tx_hash"]].append(d)

# ── Compute efficiency per event ──────────────────────────────────────────────

results = []

for tx_hash, row in summary.items():
    internal_profit = int(row["profit_raw"])
    swaps = proto_swaps.get(tx_hash, [])
    net_word0 = sum(int(s["decoded"]["word0"]) for s in swaps)

    if net_word0 == 0 or not swaps:
        results.append(dict(ip=internal_profit, lv=0.0, eff=1.0))
        continue

    # Use the protocol's directional (gross) volume as the denominator
    # i.e. total volume in the protocol's net direction
    dir_vol = sum(
        abs(int(s["decoded"]["word0"])) for s in swaps
        if (int(s["decoded"]["word0"]) > 0) == (net_word0 > 0)
    )
    if dir_vol == 0:
        dir_vol = abs(net_word0)
    ratio = abs(internal_profit) / dir_vol

    leaked = 0.0
    for ms in market_swaps.get(tx_hash, []):
        mw0 = int(ms["decoded"]["word0"])
        if (net_word0 > 0) != (mw0 > 0):          # opposite signs → counter
            leaked += abs(mw0) * ratio

    eff = internal_profit / (internal_profit + leaked) if (internal_profit + leaked) > 0 else 1.0
    results.append(dict(ip=internal_profit, lv=leaked, eff=eff))

effs = np.array([r["eff"] for r in results])
ips  = np.array([r["ip"] for r in results], dtype=float)
lvs  = np.array([r["lv"] for r in results])

total_ip = ips.sum()
total_lv = lvs.sum()
w_eff    = total_ip / (total_ip + total_lv)

print(f"Events: {len(results)}  |  100%-eff: {(effs >= 1-1e-9).sum()}")
print(f"Median eff: {np.median(effs):.1%}  |  Weighted eff: {w_eff:.1%}")

# ── Styling ───────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "font.family": "sans-serif", "font.size": 12,
    "axes.titlesize": 14, "axes.labelsize": 12,
})
C_BLUE, C_AMBER, C_RED, C_GREEN = "#2563EB", "#F59E0B", "#EF4444", "#10B981"

# ── Plot 2 — Efficiency Frequency Histogram ──────────────────────────────────

fig, ax = plt.subplots(figsize=(8, 5))
bins = np.linspace(0, 1.0, 21)
counts, edges, patches = ax.hist(
    effs, bins=bins, color=C_BLUE, edgecolor="white", lw=.8, alpha=.85, zorder=3)

for p, le in zip(patches, edges[:-1]):
    if le >= 0.95:
        p.set_facecolor(C_GREEN)

sub = effs[effs < 1 - 1e-9]
if len(sub) > 2:
    kde = gaussian_kde(sub, bw_method=0.15)
    xs = np.linspace(0, .99, 200)
    ax.plot(xs, kde(xs) * len(sub) * .05, color=C_AMBER, lw=2, zorder=4,
            label="KDE (events < 100%)")
    ax.legend(frameon=False)

ax.annotate(f"Median Efficiency: {np.median(effs):.0%}",
            xy=(.975, counts[-1]), xytext=(.55, counts[-1] * .85),
            fontsize=13, fontweight="bold", color=C_GREEN,
            arrowprops=dict(arrowstyle="->", color=C_GREEN, lw=1.5), zorder=5)

ax.set_xlabel("Capture Efficiency")
ax.set_ylabel("Count of Events")
ax.set_title("PIR Capture Efficiency Distribution")
ax.xaxis.set_major_formatter(mticker.PercentFormatter(1.0))
ax.set_xlim(-.02, 1.05)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="y", alpha=.3)
fig.tight_layout()
fig.savefig(OUT / "fig2_efficiency_frequency_histogram.png", dpi=200)
print(f"Saved fig2")

# ── Plot 3 — Weighted Value Capture ──────────────────────────────────────────

fig, ax = plt.subplots(figsize=(6, 5))
pct_c = total_ip / (total_ip + total_lv) * 100
pct_l = 100 - pct_c

ax.bar(["PIR Value Capture"], [pct_c], color=C_GREEN, edgecolor="white",
       lw=1.2, label=f"Internalized ({pct_c:.1f}%)", width=.45, zorder=3)
ax.bar(["PIR Value Capture"], [pct_l], bottom=[pct_c], color=C_RED,
       edgecolor="white", lw=1.2, label=f"Leaked ({pct_l:.1f}%)", width=.45, zorder=3)

ax.text(0, pct_c / 2, f"{pct_c:.1f}%\nInternalized",
        ha="center", va="center", fontsize=14, fontweight="bold", color="white", zorder=4)
if pct_l > 5:
    ax.text(0, pct_c + pct_l / 2, f"{pct_l:.1f}%\nLeaked",
            ha="center", va="center", fontsize=12, fontweight="bold", color="white", zorder=4)
else:
    ax.text(0, pct_c + pct_l + 2, f"{pct_l:.1f}% Leaked",
            ha="center", va="bottom", fontsize=11, fontweight="bold", color=C_RED, zorder=4)

ax.set_ylabel("Share of Total Rebalancing Value (%)")
ax.set_title("Economic Efficiency of PIR")
ax.set_ylim(0, 115)
ax.yaxis.set_major_formatter(mticker.PercentFormatter())
ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1), frameon=False, fontsize=10)

ax.text(0, 104, f"Volume-Weighted\nEfficiency: {w_eff:.1%}",
        ha="center", va="bottom", fontsize=13, fontweight="bold", color=C_BLUE)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="y", alpha=.3)
fig.tight_layout()
fig.savefig(OUT / "fig3_weighted_value_capture.png", dpi=200)
print(f"Saved fig3")
