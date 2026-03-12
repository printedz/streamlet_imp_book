# Streamlet Consensus Protocol

A Python implementation of the **Streamlet** blockchain consensus protocol, based on Chapter 7 of *Foundations of Distributed Consensus and Blockchains* by Elaine Shi (2020).

Streamlet was introduced by Chan and Shi as a unified protocol for pedagogy and implementation. It is arguably the simplest known partially synchronous consensus protocol, tolerating up to `f < n/3` Byzantine (corrupt) nodes.

## Protocol Overview

Streamlet operates in synchronized **epochs**, each with a rotating leader chosen via a hash function. The protocol follows a propose-vote paradigm with a finalization rule:

1. **Propose.** Each epoch's leader proposes a new block extending the longest notarized chain it has seen.
2. **Vote.** Every honest node votes for the first valid proposal it receives, provided the proposal extends one of the longest notarized chains the node knows of. A node votes at most once per epoch.
3. **Notarize.** A block becomes *notarized* when it receives votes from at least `2n/3` distinct nodes.
4. **Finalize.** If a notarized chain contains three adjacent blocks with *consecutive* epoch numbers `e, e+1, e+2`, then the prefix up to and including the block at epoch `e+1` is considered *finalized*. Finalized blocks can never be reverted.

### Key Properties (proven in Chapter 7)

- **Consistency (Theorem 7):** All honest nodes' finalized logs are prefixes of each other, regardless of network conditions or adversarial behavior. This holds even during network partitions.
- **Liveness (Theorem 8):** During periods of synchrony (when messages are delivered within one epoch), if 5 consecutive epochs have honest leaders, every honest node's log grows by at least one block.
- **Unique notarization per epoch (Lemma 7):** At most one block can be notarized for any given epoch, because two competing notarizations would require an honest node to have voted twice.

## Module Structure

```
market_sim/blockchain/consensus/
├── __init__.py                  # Public API exports
├── streamlet.py                 # Core protocol implementation
├── trade_ledger.py              # Integration with market_sim trades
├── visualize_streamlet.py       # HTML visualization generator
└── README.md                    # This file

market_sim/tests/
└── test_streamlet.py            # 32 tests covering all protocol properties
```

### `streamlet.py` — Core Protocol

Contains the full Streamlet implementation:

- **`Block`** — Immutable block dataclass with SHA-256 hash chaining, epoch number, and transaction payload. Includes a `genesis()` factory for the genesis block.
- **`BlockchainState`** — A node's local view of the blockchain: known blocks, votes, notarizations, and the finalized log. Implements chain traversal, longest-notarized-chain lookup, and the finalization rule.
- **`StreamletNode`** — A single protocol participant. Honest nodes follow the protocol exactly (propose as leader, vote once per epoch on valid proposals). Corrupt nodes can be configured to skip proposals/votes.
- **`NetworkSimulator`** — Simulates message delivery with configurable delays and network partitions. Supports both synchronous and asynchronous delivery models.
- **`StreamletProtocol`** — Top-level engine that orchestrates multi-node execution across epochs, coordinating proposals, votes, notarizations, and finalizations while tracking metrics.

### `trade_ledger.py` — Market Integration

Bridges the Streamlet protocol with the `market_sim` framework:

- **`TradeTx`** — A trade transaction suitable for blockchain inclusion.
- **`StreamletTradeLedger`** — Wraps `StreamletProtocol` with a trade-focused API: submit trades, run consensus to finalize them, and query the finalized trade history.

### `visualize_streamlet.py` — Visualization

Generates a self-contained HTML dashboard (using Chart.js) showing:

- Protocol summary statistics (proposed, notarized, finalized blocks)
- Cumulative notarization and finalization progress over epochs
- Leader distribution per epoch (honest vs corrupt)
- Block timeline with color-coded status
- Per-node finalized log comparison

## Usage

### Running the Protocol

```python
from blockchain.consensus.streamlet import StreamletProtocol

# 10 nodes, 3 corrupt (< 10/3), 30 epochs
protocol = StreamletProtocol(n_nodes=10, n_corrupt=3)
results = protocol.run(n_epochs=30)

print(f"Consistency: {results['consistency_check']}")
print(f"Finalized: {results['metrics']['blocks_finalized']} blocks")
```

### Using the Trade Ledger

```python
from blockchain.consensus.trade_ledger import StreamletTradeLedger, TradeTx

ledger = StreamletTradeLedger(n_nodes=7, n_corrupt=2)

ledger.submit_trade(TradeTx(
    trade_id="t1", symbol="AAPL", price=150.0,
    quantity=10.0, buyer_id="alice", seller_id="bob",
    timestamp=1.0,
))

results = ledger.run_consensus(n_epochs=20)
print(ledger.get_finalized_trades())
```

### Generating the Visualization

```bash
cd market_sim
python blockchain/consensus/visualize_streamlet.py --nodes 10 --corrupt 3 --epochs 30
# Outputs streamlet_visualization.html
```

## Running Tests

```bash
cd market_sim
python -m pytest tests/test_streamlet.py -v
```

The test suite covers:

- Block hashing, determinism, and chain reconstruction
- Vote counting and the 2n/3 notarization threshold
- Finalization with consecutive vs non-consecutive epoch numbers
- Leader election determinism and uniform distribution
- Honest node behavior (propose as leader, vote once, reject invalid proposals)
- Corrupt node behavior (skip proposals and votes)
- Network message delivery and partition simulation
- Full protocol consistency across multiple independent runs (Theorem 7)
- Liveness with all-honest nodes (Theorem 8)
- Unique notarization per epoch (Lemma 7)
- Trade ledger integration

## References

- Chan, T.-H. H., & Shi, E. (2020). *Streamlet: Textbook Streamlined Blockchains.* AFT '20.
- Shi, E. (2020). *Foundations of Distributed Consensus and Blockchains.* Chapter 7.
- Dwork, C., Lynch, N., & Stockmeyer, L. (1988). *Consensus in the Presence of Partial Synchrony.* JACM.
