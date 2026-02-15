#!/usr/bin/env python3
"""
Extract swap events by topic0, identify swapper from receipt/tx, then scan
the same block (after tx) and next 3 blocks for follow-up swaps on the same pool.

Usage:
    python3 extract_swaps.py --rpc https://rpc.hyperliquid.xyz/evm \
        --topic0 0x121cb44ee54098b1a04743c487e7460d8dd429b27f88b1f4d4767396e1a59f79 \
        --out out/swap_followups.jsonl
"""

import argparse
import json
import logging
import os
import time
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_RETRIES = 5
BACKOFF_BASE = 1.5

INPUT_FILE = Path(__file__).parent / "swap_txs.txt"

# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------
_rpc_id = 0


def rpc_call(rpc_url: str, method: str, params: list, retries: int = MAX_RETRIES):
    global _rpc_id
    _rpc_id += 1
    payload = {"jsonrpc": "2.0", "id": _rpc_id, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                rpc_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if "error" in body:
                raise RuntimeError(f"RPC error: {body['error']}")
            return body.get("result")
        except Exception as e:
            if attempt < retries - 1:
                wait = BACKOFF_BASE ** (attempt + 1)
                log.warning("RPC %s attempt %d failed (%s), retry in %.1fs",
                            method, attempt + 1, e, wait)
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def hex_to_int(h: str) -> int:
    return int(h, 16)


def addr_lower(a: str) -> str:
    return a.lower() if a else ""


def get_tx_from(rpc_url: str, tx_hash: str) -> str:
    """Get the 'from' address of a transaction."""
    tx_obj = rpc_call(rpc_url, "eth_getTransactionByHash", [tx_hash])
    if tx_obj:
        return addr_lower(tx_obj.get("from", ""))
    return ""


def parse_log_index(val) -> int:
    return hex_to_int(val) if isinstance(val, str) else (val or 0)


def parse_block_number(val) -> int:
    return hex_to_int(val) if isinstance(val, str) else (val or 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_tx(rpc_url: str, topic0: str, tx_hash: str, block_number: int,
               block_time: str, expected_count: int, out_f):
    """Process one transaction: find swap logs, get swapper, scan next 3 blocks."""

    # Step 1: get receipt
    receipt = rpc_call(rpc_url, "eth_getTransactionReceipt", [tx_hash])
    if receipt is None:
        log.error("No receipt for %s", tx_hash)
        return 0

    tx_index = hex_to_int(receipt["transactionIndex"]) if isinstance(
        receipt.get("transactionIndex"), str) else receipt.get("transactionIndex", 0)

    # Original swapper = receipt.from
    swapper = addr_lower(receipt.get("from", ""))

    # Filter swap logs by topic0
    swap_logs = [
        l for l in receipt.get("logs", [])
        if l.get("topics") and l["topics"][0].lower() == topic0.lower()
    ]

    if not swap_logs:
        log.warning("No swap logs in %s (expected %d)", tx_hash, expected_count)
        return 0

    count = 0
    for sl in swap_logs:
        pool = addr_lower(sl["address"])
        sl_index = parse_log_index(sl.get("logIndex"))

        # Collect follow-up swaps: same block (after tx) + next 3 blocks
        followup_swaps = []

        # Same block, after our tx
        same_block_hex = receipt["blockNumber"]
        same_block_logs = rpc_call(rpc_url, "eth_getLogs", [{
            "fromBlock": same_block_hex,
            "toBlock": same_block_hex,
            "address": pool,
            "topics": [[topic0]],
        }])
        if same_block_logs:
            for fl in same_block_logs:
                fl_tx = addr_lower(fl.get("transactionHash", ""))
                if fl_tx == tx_hash.lower():
                    continue
                fl_tx_index = hex_to_int(fl["transactionIndex"]) if isinstance(
                    fl.get("transactionIndex"), str) else fl.get("transactionIndex", 0)
                if fl_tx_index <= tx_index:
                    continue

                fu_swapper = get_tx_from(rpc_url, fl.get("transactionHash"))

                followup_swaps.append({
                    "tx_hash": fl.get("transactionHash", "").lower(),
                    "block_number": parse_block_number(fl.get("blockNumber")),
                    "block_offset": 0,
                    "log_index": parse_log_index(fl.get("logIndex")),
                    "swapper": fu_swapper,
                    "raw_topics": fl.get("topics", []),
                    "raw_data": fl.get("data", "0x"),
                })

        # Next 3 blocks
        for offset in (1, 2, 3):
            nb = block_number + offset
            nb_hex = hex(nb)
            fut_logs = rpc_call(rpc_url, "eth_getLogs", [{
                "fromBlock": nb_hex,
                "toBlock": nb_hex,
                "address": pool,
                "topics": [[topic0]],
            }])
            if not fut_logs:
                continue
            for fl in fut_logs:
                fu_swapper = get_tx_from(rpc_url, fl.get("transactionHash"))

                followup_swaps.append({
                    "tx_hash": fl.get("transactionHash", "").lower(),
                    "block_number": parse_block_number(fl.get("blockNumber")),
                    "block_offset": offset,
                    "log_index": parse_log_index(fl.get("logIndex")),
                    "swapper": fu_swapper,
                    "raw_topics": fl.get("topics", []),
                    "raw_data": fl.get("data", "0x"),
                })

        record = {
            "original_tx": tx_hash.lower(),
            "block_number": block_number,
            "block_time": block_time,
            "pool": pool,
            "swap_log_index": sl_index,
            "swapper": swapper,
            "raw_topics": sl.get("topics", []),
            "raw_data": sl.get("data", "0x"),
            "n_followup_swaps": len(followup_swaps),
            "followup_swaps": followup_swaps,
        }
        out_f.write(json.dumps(record) + "\n")
        out_f.flush()
        count += 1

    return count


def load_input(path: Path) -> list[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 4:
                continue
            rows.append({
                "block_time": parts[0],
                "tx_hash": parts[1].lower(),
                "block_number": int(parts[2]),
                "expected_count": int(parts[3]),
            })
    return rows


def main():
    parser = argparse.ArgumentParser(description="Extract swap events and follow-up swaps")
    parser.add_argument("--rpc", required=True, help="RPC URL")
    parser.add_argument("--topic0", required=True, help="Swap event topic0")
    parser.add_argument("--out", default="out/swap_followups.jsonl", help="Output JSONL path")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    rows = load_input(INPUT_FILE)
    log.info("Loaded %d transactions", len(rows))

    # Resume support
    done_txs = set()
    out_path = Path(args.out)
    if out_path.exists():
        with open(out_path) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done_txs.add(rec["original_tx"])
                except Exception:
                    pass
        if done_txs:
            log.info("Resuming: %d txs already processed", len(done_txs))

    mode = "a" if done_txs else "w"
    out_f = open(args.out, mode)

    total_swaps = 0
    errors = 0

    for i, row in enumerate(rows):
        if row["tx_hash"] in done_txs:
            continue
        try:
            n = process_tx(
                args.rpc, args.topic0,
                row["tx_hash"], row["block_number"],
                row["block_time"], row["expected_count"],
                out_f,
            )
            total_swaps += n
        except Exception as e:
            log.error("Failed %s: %s", row["tx_hash"], e)
            errors += 1

        if (i + 1) % 5 == 0 or (i + 1) == len(rows):
            log.info("Processed %d/%d txs, swap records: %d, errors: %d",
                     i + 1, len(rows), total_swaps, errors)

    out_f.close()
    log.info("Done. Output: %s (%d swap records)", args.out, total_swaps)


if __name__ == "__main__":
    main()
