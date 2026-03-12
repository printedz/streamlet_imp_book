"""
Tests for the Streamlet consensus protocol implementation.

These tests verify the core properties of the Streamlet protocol as
described in Chapter 7 of "Foundations of Distributed Consensus and
Blockchains" by Elaine Shi:

1. Block and chain data structures
2. Leader election via hash function
3. Voting and notarization (2n/3 threshold)
4. Finalization rule (three consecutive epoch blocks)
5. Consistency property (Theorem 7)
6. Liveness under synchrony (Theorem 8)
7. Trade ledger integration
"""

import sys
import os
import pytest

# Add the market_sim directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from blockchain.consensus.streamlet import (
    Block,
    BlockchainState,
    NetworkSimulator,
    NodeType,
    StreamletNode,
    StreamletProtocol,
    Vote,
)
from blockchain.consensus.trade_ledger import StreamletTradeLedger, TradeTx


# -----------------------------------------------------------------------
# Block Tests
# -----------------------------------------------------------------------


class TestBlock:
    """Tests for the Block data structure."""

    def test_genesis_block(self):
        """Genesis block should have epoch=0 and no parent."""
        genesis = Block.genesis()
        assert genesis.epoch == 0
        assert genesis.parent_hash is None
        assert genesis.transactions == ()
        assert genesis.block_hash != ""

    def test_block_hash_deterministic(self):
        """Same block contents should produce the same hash."""
        b1 = Block(parent_hash="abc", epoch=1, transactions=("tx1",))
        b2 = Block(parent_hash="abc", epoch=1, transactions=("tx1",))
        assert b1.block_hash == b2.block_hash

    def test_block_hash_changes_with_content(self):
        """Different contents should produce different hashes."""
        b1 = Block(parent_hash="abc", epoch=1, transactions=("tx1",))
        b2 = Block(parent_hash="abc", epoch=2, transactions=("tx1",))
        b3 = Block(parent_hash="abc", epoch=1, transactions=("tx2",))
        assert b1.block_hash != b2.block_hash
        assert b1.block_hash != b3.block_hash

    def test_block_repr(self):
        """Block repr should include epoch and partial hash."""
        b = Block(parent_hash="abc", epoch=5, transactions=())
        r = repr(b)
        assert "epoch=5" in r
        assert "hash=" in r

    def test_genesis_is_unique(self):
        """All genesis blocks should be identical."""
        g1 = Block.genesis()
        g2 = Block.genesis()
        assert g1.block_hash == g2.block_hash


# -----------------------------------------------------------------------
# BlockchainState Tests
# -----------------------------------------------------------------------


class TestBlockchainState:
    """Tests for the BlockchainState data structure."""

    def setup_method(self):
        self.state = BlockchainState()
        self.genesis = Block.genesis()
        self.state.add_block(self.genesis)
        self.state.notarized.add(self.genesis.block_hash)

    def test_add_block(self):
        """Blocks should be stored and retrievable."""
        b = Block(
            parent_hash=self.genesis.block_hash, epoch=1, transactions=()
        )
        self.state.add_block(b)
        assert b.block_hash in self.state.blocks
        assert self.state.parent_map[b.block_hash] == self.genesis.block_hash

    def test_vote_and_notarization(self):
        """Block becomes notarized when it receives >= 2n/3 votes."""
        n_nodes = 6  # threshold = ceil(4) = 4
        b = Block(
            parent_hash=self.genesis.block_hash, epoch=1, transactions=()
        )
        self.state.add_block(b)

        # First 3 votes: not yet notarized
        for i in range(3):
            result = self.state.add_vote(
                Vote(node_id=i, block_hash=b.block_hash, epoch=1), n_nodes
            )
            assert result is False

        # 4th vote: notarized
        result = self.state.add_vote(
            Vote(node_id=3, block_hash=b.block_hash, epoch=1), n_nodes
        )
        assert result is True
        assert self.state.is_notarized(b.block_hash)

    def test_chain_reconstruction(self):
        """Chain should be reconstructable from tip to genesis."""
        b1 = Block(
            parent_hash=self.genesis.block_hash, epoch=1, transactions=()
        )
        self.state.add_block(b1)
        b2 = Block(parent_hash=b1.block_hash, epoch=2, transactions=())
        self.state.add_block(b2)

        chain = self.state.get_chain(b2.block_hash)
        assert len(chain) == 3
        assert chain[0].epoch == 0  # genesis
        assert chain[1].epoch == 1
        assert chain[2].epoch == 2

    def test_longest_notarized_chain(self):
        """Should find the longest notarized chain tip."""
        n_nodes = 4

        b1 = Block(
            parent_hash=self.genesis.block_hash, epoch=1, transactions=()
        )
        self.state.add_block(b1)
        self.state.notarized.add(b1.block_hash)

        b2 = Block(parent_hash=b1.block_hash, epoch=2, transactions=())
        self.state.add_block(b2)
        self.state.notarized.add(b2.block_hash)

        # Competing shorter chain
        b3 = Block(
            parent_hash=self.genesis.block_hash, epoch=3, transactions=()
        )
        self.state.add_block(b3)
        self.state.notarized.add(b3.block_hash)

        tip = self.state.get_longest_notarized_chain_tip()
        assert tip == b2.block_hash  # longer chain

    def test_finalization_three_consecutive_epochs(self):
        """
        Finalization rule: three adjacent blocks with consecutive epochs
        finalize the prefix up to the second block.
        """
        b1 = Block(
            parent_hash=self.genesis.block_hash,
            epoch=1,
            transactions=("tx_a",),
        )
        b2 = Block(
            parent_hash=b1.block_hash, epoch=2, transactions=("tx_b",)
        )
        b3 = Block(
            parent_hash=b2.block_hash, epoch=3, transactions=("tx_c",)
        )

        for b in [b1, b2, b3]:
            self.state.add_block(b)
            self.state.notarized.add(b.block_hash)
            self.state.epoch_notarized[b.epoch] = b.block_hash

        newly_finalized = self.state.check_finalization()
        finalized_epochs = [b.epoch for b in self.state.finalized_log]

        # Epochs 1 and 2 should be finalized (prefix up to the second
        # of the triple 1-2-3)
        assert 1 in finalized_epochs
        assert 2 in finalized_epochs

    def test_no_finalization_without_consecutive(self):
        """Non-consecutive epochs should NOT trigger finalization."""
        b1 = Block(
            parent_hash=self.genesis.block_hash, epoch=1, transactions=()
        )
        b2 = Block(
            parent_hash=b1.block_hash,
            epoch=3,  # gap!
            transactions=(),
        )
        b3 = Block(parent_hash=b2.block_hash, epoch=5, transactions=())

        for b in [b1, b2, b3]:
            self.state.add_block(b)
            self.state.notarized.add(b.block_hash)

        newly_finalized = self.state.check_finalization()
        assert len(newly_finalized) == 0


# -----------------------------------------------------------------------
# StreamletNode Tests
# -----------------------------------------------------------------------


class TestStreamletNode:
    """Tests for individual Streamlet node behavior."""

    def test_leader_election_deterministic(self):
        """Leader election should be deterministic for a given epoch."""
        node1 = StreamletNode(0, 10)
        node2 = StreamletNode(5, 10)
        assert node1.get_leader(1) == node2.get_leader(1)
        assert node1.get_leader(42) == node2.get_leader(42)

    def test_leader_distribution(self):
        """Leaders should be roughly uniformly distributed across nodes."""
        node = StreamletNode(0, 10)
        leaders = [node.get_leader(e) for e in range(1, 1001)]
        counts = {i: leaders.count(i) for i in range(10)}
        # Each node should be leader ~100 times out of 1000
        for count in counts.values():
            assert 50 < count < 150, f"Uneven distribution: {counts}"

    def test_honest_node_proposes_as_leader(self):
        """An honest leader should propose a block."""
        n = 4
        node = StreamletNode(0, n, NodeType.HONEST)
        # Find an epoch where node 0 is leader
        for epoch in range(1, 100):
            if node.get_leader(epoch) == 0:
                block = node.propose(epoch)
                assert block is not None
                assert block.epoch == epoch
                return
        pytest.fail("Node 0 was never leader in 100 epochs")

    def test_honest_node_does_not_propose_when_not_leader(self):
        """A non-leader should not propose."""
        n = 4
        node = StreamletNode(0, n, NodeType.HONEST)
        for epoch in range(1, 100):
            if node.get_leader(epoch) != 0:
                block = node.propose(epoch)
                assert block is None
                return

    def test_corrupt_node_does_not_propose(self):
        """A corrupt node skips proposals."""
        n = 4
        node = StreamletNode(0, n, NodeType.CORRUPT)
        for epoch in range(1, 100):
            if node.get_leader(epoch) == 0:
                block = node.propose(epoch)
                assert block is None
                return

    def test_honest_node_votes_once_per_epoch(self):
        """An honest node should vote at most once per epoch."""
        n = 4
        node = StreamletNode(1, n, NodeType.HONEST)
        genesis = Block.genesis()

        # Find the actual leader for epoch 1
        actual_leader = node.get_leader(1)

        block = Block(
            parent_hash=genesis.block_hash, epoch=1, transactions=()
        )

        vote1 = node.receive_proposal(block, leader_id=actual_leader, epoch=1)
        vote2 = node.receive_proposal(block, leader_id=actual_leader, epoch=1)

        assert vote1 is not None
        assert vote2 is None  # should not vote again

    def test_corrupt_node_does_not_vote(self):
        """A corrupt node does not vote."""
        n = 4
        node = StreamletNode(1, n, NodeType.CORRUPT)
        genesis = Block.genesis()
        block = Block(
            parent_hash=genesis.block_hash, epoch=1, transactions=()
        )
        vote = node.receive_proposal(block, leader_id=0, epoch=1)
        assert vote is None


# -----------------------------------------------------------------------
# Network Simulator Tests
# -----------------------------------------------------------------------


class TestNetworkSimulator:
    """Tests for the network simulator."""

    def test_message_delivery(self):
        """Messages should be delivered after their delay."""
        net = NetworkSimulator(n_nodes=3, base_delay=0.1, max_delay=0.2)
        net.broadcast(sender=0, msg_type="test", payload="hello")
        # Too early
        delivered = net.deliver(0.05)
        total = sum(len(msgs) for msgs in delivered.values())
        assert total == 0
        # After max delay
        delivered = net.deliver(0.3)
        total = sum(len(msgs) for msgs in delivered.values())
        assert total > 0

    def test_partition_delays_messages(self):
        """Messages to/from partitioned nodes should not be delivered."""
        net = NetworkSimulator(
            n_nodes=3,
            base_delay=0.1,
            max_delay=0.2,
            partition_nodes={2},
        )
        net.broadcast(sender=2, msg_type="test", payload="from_partition")
        delivered = net.deliver(100.0)  # far future
        # Messages from node 2 should have infinite delay
        for msg_list in delivered.values():
            for msg in msg_list:
                # they shouldn't be delivered since deliver_time = inf
                pass
        # Actually check that node 2's messages are NOT delivered
        net2 = NetworkSimulator(
            n_nodes=3,
            base_delay=0.1,
            max_delay=0.2,
            partition_nodes={2},
        )
        net2.broadcast(sender=2, msg_type="test", payload="hello")
        delivered = net2.deliver(1.0)
        # All messages from partitioned node have inf deliver time
        assert len(net2.message_queue) == 3  # still pending


# -----------------------------------------------------------------------
# Full Protocol Tests
# -----------------------------------------------------------------------


class TestStreamletProtocol:
    """Integration tests for the full Streamlet protocol."""

    def test_invalid_corruption_ratio(self):
        """Should reject f >= n/3."""
        with pytest.raises(ValueError):
            StreamletProtocol(n_nodes=6, n_corrupt=2)  # 2 >= 6/3

    def test_basic_execution(self):
        """Basic execution should complete without errors."""
        protocol = StreamletProtocol(n_nodes=7, n_corrupt=2)
        results = protocol.run(n_epochs=20)
        assert results["n_nodes"] == 7
        assert results["metrics"]["epochs_run"] == 20
        assert results["metrics"]["blocks_proposed"] > 0

    def test_consistency_property(self):
        """
        Theorem 7: All honest nodes' finalized logs must be
        prefixes of each other.
        """
        protocol = StreamletProtocol(n_nodes=10, n_corrupt=3)
        results = protocol.run(n_epochs=30)
        assert results["consistency_check"] is True

    def test_consistency_many_runs(self):
        """Consistency should hold across many independent runs."""
        for _ in range(5):
            protocol = StreamletProtocol(n_nodes=7, n_corrupt=2)
            results = protocol.run(n_epochs=20)
            assert results["consistency_check"] is True

    def test_liveness_with_enough_epochs(self):
        """
        Theorem 8: With enough epochs, some blocks should be finalized
        during periods of synchrony.
        """
        protocol = StreamletProtocol(
            n_nodes=7,
            n_corrupt=0,  # all honest for guaranteed liveness
            network_delay=0.01,
            max_network_delay=0.05,
        )
        results = protocol.run(n_epochs=30)
        assert results["metrics"]["blocks_finalized"] > 0

    def test_transactions_included(self):
        """Transactions should be included in proposed blocks."""
        protocol = StreamletProtocol(n_nodes=7, n_corrupt=2)
        txs = [[f"tx_{e}_{i}" for i in range(3)] for e in range(20)]
        results = protocol.run(n_epochs=20, transactions=txs)
        assert results["metrics"]["blocks_proposed"] > 0

    def test_no_corrupt_all_honest(self):
        """With all honest nodes, protocol should work efficiently."""
        protocol = StreamletProtocol(n_nodes=5, n_corrupt=0)
        results = protocol.run(n_epochs=20)
        assert results["consistency_check"] is True
        assert results["metrics"]["blocks_proposed"] == 20  # every epoch

    def test_metrics_tracked(self):
        """Protocol should track all relevant metrics."""
        protocol = StreamletProtocol(n_nodes=7, n_corrupt=2)
        results = protocol.run(n_epochs=10)
        m = results["metrics"]
        assert len(m["epoch_leaders"]) == 10
        assert len(m["leader_is_honest"]) == 10
        assert m["votes_cast"] > 0

    def test_unique_notarization_per_epoch(self):
        """
        Lemma 7: At most one block should be notarized per epoch
        in honest view.
        """
        protocol = StreamletProtocol(n_nodes=7, n_corrupt=2)
        protocol.run(n_epochs=20)

        # Check node 0's state
        node0 = protocol.nodes[0]
        epoch_blocks = {}
        for bh in node0.state.notarized:
            block = node0.state.blocks.get(bh)
            if block and block.epoch > 0:
                assert block.epoch not in epoch_blocks, (
                    f"Two notarized blocks in epoch {block.epoch}"
                )
                epoch_blocks[block.epoch] = bh


# -----------------------------------------------------------------------
# Trade Ledger Tests
# -----------------------------------------------------------------------


class TestStreamletTradeLedger:
    """Tests for the trade ledger integration."""

    def test_submit_and_finalize_trades(self):
        """Trades should be submittable and eventually finalized."""
        ledger = StreamletTradeLedger(n_nodes=7, n_corrupt=2)

        for i in range(5):
            ledger.submit_trade(
                TradeTx(
                    trade_id=f"t_{i}",
                    symbol="AAPL",
                    price=150.0 + i,
                    quantity=10.0,
                    buyer_id="buyer_1",
                    seller_id="seller_1",
                    timestamp=float(i),
                )
            )

        results = ledger.run_consensus(n_epochs=20)
        assert results["consistency_check"] is True

    def test_ledger_summary(self):
        """Ledger summary should report correct counts."""
        ledger = StreamletTradeLedger(n_nodes=5, n_corrupt=1)
        ledger.submit_trade(
            TradeTx(
                trade_id="t_0",
                symbol="GOOG",
                price=2800.0,
                quantity=1.0,
                buyer_id="b",
                seller_id="s",
                timestamp=0.0,
            )
        )
        summary = ledger.get_ledger_summary()
        assert summary["pending_trades"] == 1

    def test_empty_consensus(self):
        """Running consensus with no trades should be safe."""
        ledger = StreamletTradeLedger(n_nodes=5, n_corrupt=1)
        results = ledger.run_consensus()
        assert results["status"] == "no_pending_trades"


# -----------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
