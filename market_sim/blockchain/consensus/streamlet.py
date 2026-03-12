"""
Streamlet Consensus Protocol Implementation

Based on Chapter 7 of "Foundations of Distributed Consensus and Blockchains"
by Elaine Shi. The Streamlet protocol (Chan and Shi, 2020) is a simple
blockchain protocol tolerating f < n/3 Byzantine faults.

Protocol overview:
- Epochs with rotating leaders (chosen via hash function)
- Propose-vote paradigm: leader proposes, nodes vote
- Notarization: a block is notarized when it receives 2n/3 votes
- Finalization: three consecutive notarized blocks finalize the prefix
  up to (and including) the second block

Properties:
- Consistency: holds regardless of network conditions
- Liveness: guaranteed during periods of synchrony with honest leaders

Reference:
    Chan, T.-H. H., & Shi, E. (2020). Streamlet: Textbook Streamlined
    Blockchains. In Proceedings of the 2nd ACM Conference on Advances
    in Financial Technologies (AFT '20).
"""

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Block:
    """
    A block in the Streamlet blockchain.

    Each block is a tuple (parent_hash, epoch, transactions) as described
    in Section 7.1.2 of the book. The genesis block has
    parent_hash=None, epoch=0, and an empty payload.
    """
    parent_hash: Optional[str]
    epoch: int
    transactions: Tuple[Any, ...]
    block_hash: str = field(default="", compare=False)

    def __post_init__(self):
        if not self.block_hash:
            object.__setattr__(self, "block_hash", self.compute_hash())

    def compute_hash(self) -> str:
        """Compute SHA-256 hash of the block contents."""
        data = json.dumps(
            {
                "parent_hash": self.parent_hash,
                "epoch": self.epoch,
                "transactions": list(self.transactions),
            },
            sort_keys=True,
        )
        return hashlib.sha256(data.encode()).hexdigest()

    @staticmethod
    def genesis() -> "Block":
        """Create the genesis block (Section 7.1.2)."""
        return Block(parent_hash=None, epoch=0, transactions=())

    def __repr__(self) -> str:
        short_hash = self.block_hash[:8] if self.block_hash else "?"
        return f"Block(epoch={self.epoch}, hash={short_hash})"


class NodeType(Enum):
    HONEST = "honest"
    CORRUPT = "corrupt"


@dataclass
class Vote:
    """A vote (signature) on a block by a node."""
    node_id: int
    block_hash: str
    epoch: int


@dataclass
class Notarization:
    """
    A notarization certificate: a collection of >= 2n/3 votes on the
    same block (Section 7.1.3).
    """
    block_hash: str
    votes: List[Vote]

    @property
    def voter_count(self) -> int:
        return len(self.votes)


@dataclass
class BlockchainState:
    """
    Represents a single node's local view of the blockchain,
    including known blocks, notarizations, and the finalized log.
    """
    blocks: Dict[str, Block] = field(default_factory=dict)
    # block_hash -> set of voter node_ids
    votes: Dict[str, Set[int]] = field(default_factory=dict)
    notarized: Set[str] = field(default_factory=set)
    finalized_log: List[Block] = field(default_factory=list)
    # block_hash -> parent_hash (for chain traversal)
    parent_map: Dict[str, Optional[str]] = field(default_factory=dict)
    # epoch -> block_hash of notarized block in that epoch
    epoch_notarized: Dict[int, str] = field(default_factory=dict)

    def add_block(self, block: Block) -> None:
        """Record a block in this node's local state."""
        self.blocks[block.block_hash] = block
        self.parent_map[block.block_hash] = block.parent_hash

    def add_vote(self, vote: Vote, n_nodes: int) -> bool:
        """
        Record a vote. Returns True if the block becomes notarized
        (i.e. receives >= 2n/3 distinct votes).
        """
        bh = vote.block_hash
        if bh not in self.votes:
            self.votes[bh] = set()
        self.votes[bh].add(vote.node_id)

        threshold = (2 * n_nodes + 2) // 3  # ceiling of 2n/3
        if len(self.votes[bh]) >= threshold and bh not in self.notarized:
            self.notarized.add(bh)
            if bh in self.blocks:
                self.epoch_notarized[self.blocks[bh].epoch] = bh
            return True
        return False

    def is_notarized(self, block_hash: str) -> bool:
        return block_hash in self.notarized

    def get_chain(self, tip_hash: str) -> List[Block]:
        """
        Reconstruct the chain from genesis to the block with tip_hash.
        """
        chain = []
        current = tip_hash
        while current is not None and current in self.blocks:
            chain.append(self.blocks[current])
            current = self.parent_map.get(current)
        chain.reverse()
        return chain

    def get_chain_length(self, tip_hash: str) -> int:
        """Return the length of the chain ending at tip_hash."""
        length = 0
        current = tip_hash
        while current is not None and current in self.blocks:
            length += 1
            current = self.parent_map.get(current)
        return length

    def get_longest_notarized_chain_tip(self) -> str:
        """
        Find the tip of the longest notarized chain.
        Ties are broken by highest epoch number.
        """
        best_tip = None
        best_length = 0
        best_epoch = -1

        for bh in self.notarized:
            chain_len = self.get_chain_length(bh)
            block = self.blocks.get(bh)
            epoch = block.epoch if block else -1
            if (chain_len > best_length) or (
                chain_len == best_length and epoch > best_epoch
            ):
                best_length = chain_len
                best_tip = bh
                best_epoch = epoch

        return best_tip

    def check_finalization(self) -> List[Block]:
        """
        Check the finalization rule (Section 7.1.4):
        If three adjacent blocks in a notarized chain have consecutive
        epoch numbers, finalize the prefix up to the second block.

        Returns newly finalized blocks (if any).
        """
        newly_finalized = []
        already_finalized_hashes = {b.block_hash for b in self.finalized_log}

        for tip_hash in self.notarized:
            chain = self.get_chain(tip_hash)
            notarized_chain = [
                b for b in chain
                if b.epoch == 0 or self.is_notarized(b.block_hash)
            ]

            # Look for three consecutive epoch-numbered blocks
            for i in range(len(notarized_chain) - 2):
                b0 = notarized_chain[i]
                b1 = notarized_chain[i + 1]
                b2 = notarized_chain[i + 2]

                if (
                    b1.epoch == b0.epoch + 1
                    and b2.epoch == b1.epoch + 1
                    and b0.epoch >= 1  # skip genesis-rooted triples
                ):
                    # Finalize prefix up to and including b1
                    prefix = notarized_chain[: i + 2]
                    for block in prefix:
                        if block.block_hash not in already_finalized_hashes:
                            newly_finalized.append(block)
                            already_finalized_hashes.add(block.block_hash)

        # Maintain order: add newly finalized in chain order
        for block in newly_finalized:
            if block not in self.finalized_log:
                self.finalized_log.append(block)

        return newly_finalized


# ---------------------------------------------------------------------------
# Network Simulation
# ---------------------------------------------------------------------------

@dataclass
class Message:
    """A message sent between nodes in the network."""
    sender: int
    msg_type: str  # "proposal", "vote", "echo"
    payload: Any
    send_time: float
    deliver_time: float


class NetworkSimulator:
    """
    Simulates network communication between nodes with configurable
    delays and partitions. Supports both synchronous (Delta-bounded)
    and asynchronous (unbounded) delivery.
    """

    def __init__(
        self,
        n_nodes: int,
        base_delay: float = 0.1,
        max_delay: float = 0.5,
        partition_nodes: Optional[Set[int]] = None,
    ):
        self.n_nodes = n_nodes
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.partition_nodes = partition_nodes or set()
        self.message_queue: List[Message] = []
        self.delivered_messages: List[Message] = []
        self.current_time: float = 0.0

    def set_partition(self, nodes: Set[int]) -> None:
        """Partition the given set of nodes from the rest."""
        self.partition_nodes = nodes

    def clear_partition(self) -> None:
        """Remove all network partitions."""
        self.partition_nodes = set()

    def send(
        self,
        sender: int,
        recipients: List[int],
        msg_type: str,
        payload: Any,
    ) -> None:
        """Queue a message for delivery to recipients."""
        import random

        for recipient in recipients:
            # Messages to/from partitioned nodes are delayed indefinitely
            if sender in self.partition_nodes or recipient in self.partition_nodes:
                deliver_time = float("inf")
            else:
                delay = random.uniform(self.base_delay, self.max_delay)
                deliver_time = self.current_time + delay

            msg = Message(
                sender=sender,
                msg_type=msg_type,
                payload=payload,
                send_time=self.current_time,
                deliver_time=deliver_time,
            )
            self.message_queue.append(msg)

    def broadcast(self, sender: int, msg_type: str, payload: Any) -> None:
        """Broadcast a message to all nodes."""
        all_nodes = list(range(self.n_nodes))
        self.send(sender, all_nodes, msg_type, payload)

    def deliver(self, current_time: float) -> Dict[int, List[Message]]:
        """
        Deliver all messages whose delivery time has arrived.
        Returns a dict mapping recipient -> list of messages.
        """
        self.current_time = current_time
        deliverable = []
        remaining = []

        for msg in self.message_queue:
            if msg.deliver_time <= current_time:
                deliverable.append(msg)
            else:
                remaining.append(msg)

        self.message_queue = remaining
        self.delivered_messages.extend(deliverable)

        # Group by implicit recipient (broadcast → all non-sender nodes)
        per_node: Dict[int, List[Message]] = {i: [] for i in range(self.n_nodes)}
        for msg in deliverable:
            for node_id in range(self.n_nodes):
                per_node[node_id].append(msg)

        return per_node


# ---------------------------------------------------------------------------
# Streamlet Node
# ---------------------------------------------------------------------------

class StreamletNode:
    """
    A single participant in the Streamlet protocol.

    Honest nodes follow the protocol exactly:
    - Propose blocks extending the longest notarized chain
    - Vote for valid proposals from the epoch leader
    - Finalize when three consecutive-epoch blocks are notarized

    Corrupt nodes can behave arbitrarily (controlled by subclass/config).
    """

    def __init__(
        self,
        node_id: int,
        n_nodes: int,
        node_type: NodeType = NodeType.HONEST,
    ):
        self.node_id = node_id
        self.n_nodes = n_nodes
        self.node_type = node_type
        self.state = BlockchainState()
        self.voted_epochs: Set[int] = set()
        self.pending_transactions: List[Any] = []

        # Add genesis block
        genesis = Block.genesis()
        self.state.add_block(genesis)
        self.state.notarized.add(genesis.block_hash)

    def get_leader(self, epoch: int) -> int:
        """
        Compute the leader for a given epoch using a hash function
        H: {0,1}* -> [n] (Section 7.1.1).
        """
        h = hashlib.sha256(f"epoch-{epoch}".encode()).hexdigest()
        return int(h, 16) % self.n_nodes

    def propose(self, epoch: int) -> Optional[Block]:
        """
        If this node is the leader for the given epoch, propose a block
        extending the longest notarized chain (Section 7.1.4).
        """
        if self.get_leader(epoch) != self.node_id:
            return None

        if self.node_type == NodeType.CORRUPT:
            return None  # corrupt nodes can skip proposals

        tip = self.state.get_longest_notarized_chain_tip()
        if tip is None:
            parent_hash = Block.genesis().block_hash
        else:
            parent_hash = tip

        txs = tuple(self.pending_transactions)
        self.pending_transactions = []

        block = Block(
            parent_hash=parent_hash,
            epoch=epoch,
            transactions=txs,
        )
        self.state.add_block(block)
        logger.debug(
            f"Node {self.node_id} proposes {block} in epoch {epoch}"
        )
        return block

    def receive_proposal(self, block: Block, leader_id: int, epoch: int) -> Optional[Vote]:
        """
        Handle a received proposal. An honest node votes if:
        1. It has not already voted in this epoch
        2. The block extends one of the longest notarized chains
        (Section 7.1.4)
        """
        self.state.add_block(block)

        if self.node_type == NodeType.CORRUPT:
            return None

        if epoch in self.voted_epochs:
            return None

        if self.get_leader(epoch) != leader_id:
            return None

        # Check that the proposal extends a longest notarized chain
        tip = self.state.get_longest_notarized_chain_tip()
        if tip is not None:
            longest_len = self.state.get_chain_length(tip)
            proposal_parent_len = self.state.get_chain_length(block.parent_hash)
            # The parent of the proposed block should be at the longest
            # notarized chain length
            if proposal_parent_len < longest_len:
                logger.debug(
                    f"Node {self.node_id} rejects proposal: "
                    f"parent chain too short ({proposal_parent_len} < {longest_len})"
                )
                return None

        self.voted_epochs.add(epoch)
        vote = Vote(
            node_id=self.node_id,
            block_hash=block.block_hash,
            epoch=epoch,
        )
        logger.debug(f"Node {self.node_id} votes for {block}")
        return vote

    def receive_vote(self, vote: Vote) -> bool:
        """
        Process a received vote. Returns True if the vote caused a
        new notarization.
        """
        became_notarized = self.state.add_vote(vote, self.n_nodes)
        if became_notarized:
            logger.debug(
                f"Node {self.node_id}: block {vote.block_hash[:8]} "
                f"became notarized in epoch {vote.epoch}"
            )
        return became_notarized

    def check_finalization(self) -> List[Block]:
        """Check and apply the finalization rule."""
        return self.state.check_finalization()

    def get_finalized_log(self) -> List[Block]:
        """Return the node's finalized log."""
        return list(self.state.finalized_log)

    def add_transaction(self, tx: Any) -> None:
        """Add a pending transaction."""
        self.pending_transactions.append(tx)


# ---------------------------------------------------------------------------
# Streamlet Protocol Engine
# ---------------------------------------------------------------------------

class StreamletProtocol:
    """
    Orchestrates the full Streamlet protocol execution across multiple nodes
    with a simulated network.

    This engine:
    - Manages epoch progression
    - Coordinates proposals, votes, and finalizations
    - Tracks protocol metrics (notarizations, finalizations, etc.)
    - Supports configurable fault ratios and network conditions
    """

    def __init__(
        self,
        n_nodes: int,
        n_corrupt: int = 0,
        epoch_duration: float = 1.0,
        network_delay: float = 0.1,
        max_network_delay: float = 0.5,
    ):
        if n_corrupt >= n_nodes / 3:
            raise ValueError(
                f"Streamlet requires f < n/3: got f={n_corrupt}, n={n_nodes}"
            )

        self.n_nodes = n_nodes
        self.n_corrupt = n_corrupt
        self.epoch_duration = epoch_duration

        # Initialize nodes (last n_corrupt nodes are corrupt)
        self.nodes: List[StreamletNode] = []
        for i in range(n_nodes):
            node_type = (
                NodeType.CORRUPT if i >= n_nodes - n_corrupt else NodeType.HONEST
            )
            self.nodes.append(StreamletNode(i, n_nodes, node_type))

        # Network
        self.network = NetworkSimulator(
            n_nodes=n_nodes,
            base_delay=network_delay,
            max_delay=max_network_delay,
        )

        # Metrics
        self.metrics: Dict[str, Any] = {
            "epochs_run": 0,
            "blocks_proposed": 0,
            "blocks_notarized": 0,
            "blocks_finalized": 0,
            "votes_cast": 0,
            "epoch_leaders": [],
            "leader_is_honest": [],
            "notarization_history": [],
            "finalization_history": [],
        }

    def get_leader(self, epoch: int) -> int:
        """Get the leader for a given epoch."""
        return self.nodes[0].get_leader(epoch)

    def run(self, n_epochs: int, transactions: Optional[List[List[Any]]] = None) -> Dict[str, Any]:
        """
        Run the Streamlet protocol for n_epochs.

        Args:
            n_epochs: Number of epochs to simulate.
            transactions: Optional list of transaction batches, one per epoch.

        Returns:
            Dictionary of protocol execution results and metrics.
        """
        logger.info(
            f"Starting Streamlet: {self.n_nodes} nodes, "
            f"{self.n_corrupt} corrupt, {n_epochs} epochs"
        )

        for epoch in range(1, n_epochs + 1):
            self._run_epoch(
                epoch,
                transactions[epoch - 1] if transactions and epoch - 1 < len(transactions) else [],
            )
            self.metrics["epochs_run"] = epoch

        return self._compile_results()

    def _run_epoch(self, epoch: int, transactions: List[Any]) -> None:
        """Execute a single epoch of the protocol."""
        leader_id = self.get_leader(epoch)
        leader_node = self.nodes[leader_id]
        is_honest_leader = leader_node.node_type == NodeType.HONEST

        self.metrics["epoch_leaders"].append(leader_id)
        self.metrics["leader_is_honest"].append(is_honest_leader)

        # Distribute transactions to the leader
        for tx in transactions:
            leader_node.add_transaction(tx)

        # --- Phase 1: Propose ---
        proposed_block = leader_node.propose(epoch)

        if proposed_block is not None:
            self.metrics["blocks_proposed"] += 1
            # Broadcast proposal
            self.network.broadcast(
                sender=leader_id,
                msg_type="proposal",
                payload={
                    "block": proposed_block,
                    "leader_id": leader_id,
                    "epoch": epoch,
                },
            )

        # Advance network time to deliver proposals
        epoch_start = epoch * self.epoch_duration
        self.network.current_time = epoch_start
        delivered = self.network.deliver(epoch_start + self.epoch_duration * 0.4)

        # --- Phase 2: Vote ---
        all_votes: List[Vote] = []

        if proposed_block is not None:
            for node in self.nodes:
                # Each node receives the proposal and may vote
                vote = node.receive_proposal(proposed_block, leader_id, epoch)
                if vote is not None:
                    all_votes.append(vote)
                    self.metrics["votes_cast"] += 1

            # Broadcast votes
            for vote in all_votes:
                self.network.broadcast(
                    sender=vote.node_id,
                    msg_type="vote",
                    payload=vote,
                )

        # Advance network time to deliver votes
        delivered = self.network.deliver(epoch_start + self.epoch_duration * 0.8)

        # --- Phase 3: Tally votes and check notarization ---
        new_notarization = False
        for vote in all_votes:
            for node in self.nodes:
                became_notarized = node.receive_vote(vote)
                if became_notarized and not new_notarization:
                    new_notarization = True
                    self.metrics["blocks_notarized"] += 1
                    self.metrics["notarization_history"].append({
                        "epoch": epoch,
                        "block_hash": vote.block_hash[:16],
                    })

        # --- Phase 4: Check finalization ---
        for node in self.nodes:
            newly_finalized = node.check_finalization()
            if newly_finalized and node.node_id == 0:
                # Record finalization from node 0's perspective
                for block in newly_finalized:
                    self.metrics["blocks_finalized"] += 1
                    self.metrics["finalization_history"].append({
                        "epoch": epoch,
                        "finalized_block_epoch": block.epoch,
                        "block_hash": block.block_hash[:16],
                    })

    def _compile_results(self) -> Dict[str, Any]:
        """Compile final results from all nodes."""
        node_logs = {}
        for node in self.nodes:
            finalized = node.get_finalized_log()
            node_logs[node.node_id] = {
                "finalized_length": len(finalized),
                "finalized_epochs": [b.epoch for b in finalized],
                "node_type": node.node_type.value,
            }

        # Check consistency: all honest nodes' finalized logs should be
        # prefixes of each other
        honest_logs = [
            node_logs[i]["finalized_epochs"]
            for i in range(self.n_nodes)
            if self.nodes[i].node_type == NodeType.HONEST
        ]
        is_consistent = self._check_consistency(honest_logs)

        return {
            "n_nodes": self.n_nodes,
            "n_corrupt": self.n_corrupt,
            "metrics": self.metrics,
            "node_logs": node_logs,
            "consistency_check": is_consistent,
        }

    @staticmethod
    def _check_consistency(logs: List[List[int]]) -> bool:
        """
        Verify consistency property (Theorem 7): any two honest nodes'
        finalized logs must be prefixes of each other.
        """
        for i in range(len(logs)):
            for j in range(i + 1, len(logs)):
                log_i = logs[i]
                log_j = logs[j]
                min_len = min(len(log_i), len(log_j))
                if log_i[:min_len] != log_j[:min_len]:
                    return False
        return True
