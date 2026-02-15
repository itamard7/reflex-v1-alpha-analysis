# Reflex V1 Alpha — Backrun Extraction & Analysis

Extract and analyze backrun transactions from the Reflex V1 Alpha contract on HyperEVM.

## Overview

This tool processes a list of transaction hashes to extract:

- **Backrun logs** — profit, pool, and token data from the Reflex contract (`0x74c518...`)
- **Next-block pool swaps** — swap activity on the same pool in the 3 blocks following each backrun

## Requirements

- Python 3.10+
- Access to a HyperEVM RPC endpoint

## Usage

```bash
export RPC_URL="https://your-hyperevm-rpc-endpoint"
python extract_backruns.py
```

Transaction hashes are read from `tx_hashes.txt` (one per line).

## Output

Results are written to `out/`:

| File | Description |
|------|-------------|
| `backruns.jsonl` | Extracted backrun events with profit, pool, block, and timing data |
| `next_blocks_pool_swaps.jsonl` | Swap events on the same pool in the next 3 blocks after each backrun |
