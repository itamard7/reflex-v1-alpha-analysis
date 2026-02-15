#!/usr/bin/env python3
"""
Extract backrun logs, same-tx swaps, same-block-after-tx swaps, and next-2-block swaps
from HyperEVM transactions.
"""

import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

import urllib.request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
RPC_URL = os.environ.get("RPC_URL")
if not RPC_URL:
    sys.exit("ERROR: RPC_URL environment variable not set")

TARGET_CONTRACT = "0x74c51815f070803d53bb6879df6fc1648d741212"  # lowercase
BACKRUN_TOPIC0 = "0x4866868bf8ccc56c236dc0ed3f2d82a498301866dc9edbf731a9b2b4c8716fd7"

OUT_DIR = Path(__file__).parent / "out"
OUT_DIR.mkdir(exist_ok=True)

TX_HASHES_FILE = Path(__file__).parent / "tx_hashes.txt"

MAX_RETRIES = 5
BACKOFF_BASE = 1.5  # seconds

# ---------------------------------------------------------------------------
# Swap topic0 constants
# ---------------------------------------------------------------------------
# keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
V3_SWAP_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# Custom swap topic for this tx set
CUSTOM_SWAP_TOPIC0 = "0x121cb44ee54098b1a04743c487e7460d8dd429b27f88b1f4d4767396e1a59f79"

SWAP_TOPICS = {V3_SWAP_TOPIC0, CUSTOM_SWAP_TOPIC0}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------
_rpc_id = 0


def rpc_call(method: str, params: list, retries: int = MAX_RETRIES):
    global _rpc_id
    _rpc_id += 1
    payload = {"jsonrpc": "2.0", "id": _rpc_id, "method": method, "params": params}
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                RPC_URL,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if "error" in body:
                raise RuntimeError(f"RPC error: {body['error']}")
            return body.get("result")
        except Exception as e:
            if attempt < retries - 1:
                wait = BACKOFF_BASE ** (attempt + 1)
                log.warning("RPC attempt %d failed (%s), retrying in %.1fs", attempt + 1, e, wait)
                time.sleep(wait)
            else:
                raise


def get_receipt(tx_hash: str):
    return rpc_call("eth_getTransactionReceipt", [tx_hash])


def get_tx(tx_hash: str):
    return rpc_call("eth_getTransactionByHash", [tx_hash])


def get_block(block_num_hex: str):
    return rpc_call("eth_getBlockByNumber", [block_num_hex, False])


def get_logs(from_block_hex: str, to_block_hex: str, address: str, topics: list):
    filt = {
        "fromBlock": from_block_hex,
        "toBlock": to_block_hex,
        "address": address,
        "topics": topics,
    }
    return rpc_call("eth_getLogs", [filt])


# ---------------------------------------------------------------------------
# Decoding helpers
# ---------------------------------------------------------------------------
def hex_to_int(h: str) -> int:
    return int(h, 16)


def hex_to_bytes(h: str) -> bytes:
    if h.startswith("0x") or h.startswith("0X"):
        h = h[2:]
    return bytes.fromhex(h)


def uint256_from_bytes(b: bytes) -> int:
    return int.from_bytes(b, "big")


def int256_from_bytes(b: bytes) -> int:
    v = int.from_bytes(b, "big")
    if v >= (1 << 255):
        v -= 1 << 256
    return v


def addr_from_topic(topic: str) -> str:
    """Last 20 bytes of a bytes32 topic -> 0x-prefixed lowercase address."""
    return "0x" + topic[-40:].lower()


def decode_v3_swap(log_entry: dict) -> dict | None:
    try:
        data = hex_to_bytes(log_entry["data"])
        if len(data) < 160:
            return None
        return {
            "type": "v3",
            "sender": addr_from_topic(log_entry["topics"][1]),
            "recipient": addr_from_topic(log_entry["topics"][2]),
            "amount0": str(int256_from_bytes(data[0:32])),
            "amount1": str(int256_from_bytes(data[32:64])),
            "sqrtPriceX96": str(uint256_from_bytes(data[64:96])),
            "liquidity": str(uint256_from_bytes(data[96:128])),
            "tick": str(int256_from_bytes(data[128:160])),
        }
    except Exception as e:
        return {"decode_error": str(e)}


def decode_custom_swap(log_entry: dict) -> dict | None:
    """Best-effort decode for custom swap topic. Store raw if structure unknown."""
    try:
        data = hex_to_bytes(log_entry["data"])
        topics = log_entry.get("topics", [])
        result = {"type": "custom", "topic0": CUSTOM_SWAP_TOPIC0}
        # Try to extract indexed addresses from topics if present
        if len(topics) >= 2:
            result["topic1_addr"] = addr_from_topic(topics[1])
        if len(topics) >= 3:
            result["topic2_addr"] = addr_from_topic(topics[2])
        # Store raw data words
        n_words = len(data) // 32
        for i in range(min(n_words, 8)):
            word = data[i * 32 : (i + 1) * 32]
            result[f"word{i}"] = str(int256_from_bytes(word))
        return result
    except Exception as e:
        return {"decode_error": str(e)}


def decode_swap(log_entry: dict) -> dict | None:
    t0 = log_entry["topics"][0].lower()
    if t0 == V3_SWAP_TOPIC0:
        return decode_v3_swap(log_entry)
    if t0 == CUSTOM_SWAP_TOPIC0:
        return decode_custom_swap(log_entry)
    return None


def raw_log(log_entry: dict) -> dict:
    return {
        "address": log_entry.get("address", "").lower(),
        "topics": log_entry.get("topics", []),
        "data": log_entry.get("data", "0x"),
        "logIndex": log_entry.get("logIndex"),
        "transactionHash": log_entry.get("transactionHash"),
        "blockNumber": log_entry.get("blockNumber"),
    }


# ---------------------------------------------------------------------------
# Backrun log parsing
# ---------------------------------------------------------------------------
def parse_backrun_log(log_entry: dict) -> dict:
    """Extract pool_address, profit_raw, profit_token from a backrun log."""
    topic1 = log_entry["topics"][1]
    pool_address = "0x" + topic1[-40:].lower()

    data = hex_to_bytes(log_entry["data"])
    profit_raw = uint256_from_bytes(data[96:128])
    profit_token = "0x" + data[140:160].hex().lower()

    return {
        "pool": pool_address,
        "profit_raw": str(profit_raw),
        "profit_token": profit_token,
    }


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------
def process_tx(tx_hash: str, writers: dict, block_cache: dict, errors_f):
    """Process a single transaction."""
    receipt = get_receipt(tx_hash)
    if receipt is None:
        err = {"tx_hash": tx_hash, "reason": "receipt_null"}
        errors_f.write(json.dumps(err) + "\n")
        return 0

    # Fetch the transaction object for to address and input
    tx_obj = get_tx(tx_hash)
    tx_to = (tx_obj.get("to") or "").lower() if tx_obj else ""
    tx_input_raw = tx_obj.get("input", "0x") if tx_obj else "0x"
    # First 10 chars = "0x" + 4-byte selector (8 hex chars)
    tx_input_selector = tx_input_raw[:10].lower() if len(tx_input_raw) >= 10 else tx_input_raw.lower()

    block_number = hex_to_int(receipt["blockNumber"])
    block_hex = receipt["blockNumber"]
    tx_index = hex_to_int(receipt["transactionIndex"]) if isinstance(receipt.get("transactionIndex"), str) else receipt.get("transactionIndex", 0)

    # Get block timestamp for this block (cached)
    if block_hex not in block_cache:
        blk = get_block(block_hex)
        block_cache[block_hex] = blk
    block_time = ""
    if block_cache[block_hex]:
        ts = hex_to_int(block_cache[block_hex]["timestamp"])
        block_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

    logs = receipt.get("logs", [])

    # Find backrun logs
    backrun_logs = [
        l for l in logs
        if l.get("address", "").lower() == TARGET_CONTRACT
        and len(l.get("topics", [])) >= 2
        and l["topics"][0].lower() == BACKRUN_TOPIC0
    ]

    if not backrun_logs:
        err = {"tx_hash": tx_hash, "reason": "no_backrun_log"}
        errors_f.write(json.dumps(err) + "\n")
        return 0

    backrun_count = 0

    for bl in backrun_logs:
        parsed = parse_backrun_log(bl)
        pool = parsed["pool"]
        bl_index = bl.get("logIndex")
        if isinstance(bl_index, str):
            bl_index = hex_to_int(bl_index)

        # Write backrun record
        backrun_rec = {
            "tx_hash": tx_hash,
            "block_number": block_number,
            "block_time": block_time,
            "backrun_log_index": bl_index,
            "pool": pool,
            "profit_raw": parsed["profit_raw"],
            "profit_token": parsed["profit_token"],
            "tx_to": tx_to,
            "tx_input_selector": tx_input_selector,
            "raw_backrun_log": raw_log(bl),
        }
        writers["backruns"].write(json.dumps(backrun_rec) + "\n")
        backrun_count += 1

        # --- Same-tx swaps on this pool ---
        n_tx_swaps = 0
        for sl in logs:
            if sl.get("address", "").lower() != pool:
                continue
            if not sl.get("topics"):
                continue
            if sl["topics"][0].lower() not in SWAP_TOPICS:
                continue
            decoded = decode_swap(sl)
            sl_index = sl.get("logIndex")
            if isinstance(sl_index, str):
                sl_index = hex_to_int(sl_index)
            swap_rec = {
                "tx_hash": tx_hash,
                "block_number": block_number,
                "pool": pool,
                "swap_tx_hash": tx_hash,
                "swap_log_index": sl_index,
                "swap_topic0": sl["topics"][0].lower(),
                "decoded": decoded,
                "raw_log": raw_log(sl),
            }
            writers["tx_swaps"].write(json.dumps(swap_rec) + "\n")
            n_tx_swaps += 1

        # --- Same-block-after-tx swaps on this pool ---
        # Use eth_getLogs on the same block, then filter to only txs AFTER ours
        n_same_block_after = 0
        same_block_swap_logs = get_logs(block_hex, block_hex, pool, [[V3_SWAP_TOPIC0, CUSTOM_SWAP_TOPIC0]])
        if same_block_swap_logs:
            for sl in same_block_swap_logs:
                # Skip logs from our own transaction
                sl_tx_hash = sl.get("transactionHash", "").lower()
                if sl_tx_hash == tx_hash:
                    continue
                # Only include logs from transactions AFTER ours in the block
                sl_tx_index = hex_to_int(sl["transactionIndex"]) if isinstance(sl.get("transactionIndex"), str) else sl.get("transactionIndex", 0)
                if sl_tx_index <= tx_index:
                    continue
                decoded = decode_swap(sl)
                sl_index = sl.get("logIndex")
                if isinstance(sl_index, str):
                    sl_index = hex_to_int(sl_index)
                sb_rec = {
                    "origin_tx_hash": tx_hash,
                    "origin_block_number": block_number,
                    "pool": pool,
                    "observed_block_number": block_number,
                    "observed_block_time": block_time,
                    "swap_tx_hash": sl_tx_hash,
                    "swap_tx_index": sl_tx_index,
                    "swap_log_index": sl_index,
                    "swap_topic0": sl["topics"][0].lower(),
                    "decoded": decoded,
                    "raw_log": raw_log(sl),
                }
                writers["same_block_swaps"].write(json.dumps(sb_rec) + "\n")
                n_same_block_after += 1

        # --- Next 3 blocks swaps (block+1, block+2, block+3) ---
        n_next_swaps = 0
        for offset in (1, 2, 3):
            nb = block_number + offset
            nb_hex = hex(nb)

            # Fetch block for timestamp (cached)
            if nb_hex not in block_cache:
                blk = get_block(nb_hex)
                block_cache[nb_hex] = blk
            nb_time = ""
            if block_cache[nb_hex]:
                ts = hex_to_int(block_cache[nb_hex]["timestamp"])
                nb_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

            # eth_getLogs for swaps on pool in this block
            swap_logs = get_logs(nb_hex, nb_hex, pool, [[V3_SWAP_TOPIC0, CUSTOM_SWAP_TOPIC0]])
            if not swap_logs:
                continue

            for sl in swap_logs:
                decoded = decode_swap(sl)
                sl_index = sl.get("logIndex")
                if isinstance(sl_index, str):
                    sl_index = hex_to_int(sl_index)
                ob_num = hex_to_int(sl["blockNumber"]) if isinstance(sl.get("blockNumber"), str) else sl.get("blockNumber")
                nb_rec = {
                    "origin_tx_hash": tx_hash,
                    "origin_block_number": block_number,
                    "pool": pool,
                    "observed_block_number": ob_num,
                    "observed_block_time": nb_time,
                    "swap_tx_hash": sl.get("transactionHash", "").lower(),
                    "swap_log_index": sl_index,
                    "swap_topic0": sl["topics"][0].lower(),
                    "decoded": decoded,
                    "raw_log": raw_log(sl),
                }
                writers["next_swaps"].write(json.dumps(nb_rec) + "\n")
                n_next_swaps += 1

        # Summary row
        writers["summary"].writerow([
            tx_hash, block_number, pool,
            parsed["profit_raw"], parsed["profit_token"],
            tx_to, tx_input_selector,
            n_tx_swaps, n_same_block_after, n_next_swaps,
        ])

    return backrun_count


def main():
    # Read tx hashes
    tx_hashes = []
    with open(TX_HASHES_FILE) as f:
        for line in f:
            h = line.strip()
            if h:
                tx_hashes.append(h.lower())

    log.info("Loaded %d transaction hashes", len(tx_hashes))
    log.info("V3 Swap topic0: %s", V3_SWAP_TOPIC0)

    # Open output files
    backruns_f = open(OUT_DIR / "backruns.jsonl", "w")
    tx_swaps_f = open(OUT_DIR / "tx_pool_swaps.jsonl", "w")
    same_block_f = open(OUT_DIR / "same_block_pool_swaps.jsonl", "w")
    next_swaps_f = open(OUT_DIR / "next_blocks_pool_swaps.jsonl", "w")
    errors_f = open(OUT_DIR / "errors.jsonl", "w")
    summary_f = open(OUT_DIR / "summary.csv", "w", newline="")
    summary_w = csv.writer(summary_f)
    summary_w.writerow([
        "tx_hash", "block_number", "pool",
        "profit_raw", "profit_token",
        "tx_to", "tx_input_selector",
        "n_swaps_in_tx", "n_swaps_same_block_after", "n_swaps_next_3_blocks",
    ])

    writers = {
        "backruns": backruns_f,
        "tx_swaps": tx_swaps_f,
        "same_block_swaps": same_block_f,
        "next_swaps": next_swaps_f,
        "summary": summary_w,
    }

    block_cache: dict = {}
    total_backruns = 0
    errors = 0

    for i, tx_hash in enumerate(tx_hashes):
        try:
            n = process_tx(tx_hash, writers, block_cache, errors_f)
            total_backruns += n
            if n == 0:
                errors += 1
        except Exception as e:
            log.error("Failed tx %s: %s", tx_hash, e)
            err = {"tx_hash": tx_hash, "reason": str(e)}
            errors_f.write(json.dumps(err) + "\n")
            errors += 1

        if (i + 1) % 10 == 0 or (i + 1) == len(tx_hashes):
            log.info("Processed %d/%d txs, backruns found: %d, errors: %d",
                     i + 1, len(tx_hashes), total_backruns, errors)

    # Flush and close
    for f in (backruns_f, tx_swaps_f, same_block_f, next_swaps_f, errors_f, summary_f):
        f.close()

    log.info("Done. Outputs in %s", OUT_DIR)
    log.info("  backruns.jsonl: %d entries", total_backruns)


if __name__ == "__main__":
    main()
