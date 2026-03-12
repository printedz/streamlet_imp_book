"""
Streamlet Trade Ledger

Integrates the Streamlet consensus protocol with the market simulation
framework to create a distributed, fault-tolerant trade ledger.

Each trade from the market simulation is submitted as a transaction to
the Streamlet blockchain, where it must be agreed upon by a
supermajority of nodes before being considered final.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from .streamlet import Block, StreamletProtocol

logger = logging.getLogger(__name__)


@dataclass
class TradeTx:
    """A trade transaction to be recorded on the blockchain."""
    trade_id: str
    symbol: str
    price: float
    quantity: float
    buyer_id: str
    seller_id: str
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "timestamp": self.timestamp,
        }


class StreamletTradeLedger:
    """
    A distributed trade ledger backed by Streamlet consensus.

    This wraps StreamletProtocol to provide a trade-focused API:
    - Submit trades as transactions
    - Run consensus to finalize trades
    - Query finalized trade history

    The ledger groups trades into epochs (batches) and runs the
    Streamlet protocol to reach agreement on their ordering.
    """

    def __init__(
        self,
        n_nodes: int = 7,
        n_corrupt: int = 2,
        epoch_duration: float = 1.0,
    ):
        self.protocol = StreamletProtocol(
            n_nodes=n_nodes,
            n_corrupt=n_corrupt,
            epoch_duration=epoch_duration,
        )
        self.pending_trades: List[TradeTx] = []
        self.finalized_trades: List[TradeTx] = []
        self.epoch_trade_batches: List[List[TradeTx]] = []

    def submit_trade(self, trade: TradeTx) -> None:
        """Submit a trade for inclusion in the next epoch."""
        self.pending_trades.append(trade)

    def submit_trade_from_market(
        self,
        trade_id: str,
        symbol: str,
        price: Decimal,
        quantity: Decimal,
        buyer_id: str,
        seller_id: str,
        timestamp: float,
    ) -> None:
        """
        Submit a trade directly from market_sim's Trade model.
        Converts Decimal values to float for JSON serialization
        in the blockchain.
        """
        tx = TradeTx(
            trade_id=trade_id,
            symbol=symbol,
            price=float(price),
            quantity=float(quantity),
            buyer_id=buyer_id,
            seller_id=seller_id,
            timestamp=timestamp,
        )
        self.submit_trade(tx)

    def run_consensus(self, n_epochs: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the Streamlet consensus protocol to finalize pending trades.

        Trades are batched into epochs. If n_epochs is not specified,
        the number of epochs is determined by the number of pending
        trades (one batch per epoch, or at least 5 epochs for liveness).
        """
        if not self.pending_trades and n_epochs is None:
            return {"status": "no_pending_trades"}

        # Batch pending trades into epochs
        batch_size = max(1, len(self.pending_trades) // max(1, n_epochs or 5))
        batches: List[List[Any]] = []
        for i in range(0, len(self.pending_trades), batch_size):
            batch = self.pending_trades[i : i + batch_size]
            batches.append([tx.to_dict() for tx in batch])
            self.epoch_trade_batches.append(batch)

        if n_epochs is None:
            # Run at least 5 extra epochs for liveness
            n_epochs = len(batches) + 5
        else:
            # Pad with empty batches if needed
            while len(batches) < n_epochs:
                batches.append([])

        # Run protocol
        results = self.protocol.run(n_epochs=n_epochs, transactions=batches)

        # Extract finalized trades from node 0's log
        node0 = self.protocol.nodes[0]
        for block in node0.get_finalized_log():
            if block.epoch == 0:
                continue  # skip genesis
            for tx_data in block.transactions:
                if isinstance(tx_data, dict) and "trade_id" in tx_data:
                    finalized_tx = TradeTx(**tx_data)
                    self.finalized_trades.append(finalized_tx)

        self.pending_trades = []

        results["finalized_trade_count"] = len(self.finalized_trades)
        return results

    def get_finalized_trades(self) -> List[TradeTx]:
        """Return all finalized trades."""
        return list(self.finalized_trades)

    def get_ledger_summary(self) -> Dict[str, Any]:
        """Get a summary of the ledger state."""
        return {
            "pending_trades": len(self.pending_trades),
            "finalized_trades": len(self.finalized_trades),
            "total_batches": len(self.epoch_trade_batches),
            "total_value": sum(
                t.price * t.quantity for t in self.finalized_trades
            ),
        }
