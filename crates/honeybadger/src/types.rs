//! Defines common data types used throughout the HoneyBadgerBFT library.
//!
//! This module includes types for identifying nodes, representing epochs,
//! transactions, batches, cryptographic hashes, inter-node messages for
//! sub-protocols (RBC and ABA), timer requests, and protocol outputs.

use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;

use sha2::{Digest, Sha256};

/// Represents an epoch number in the HoneyBadgerBFT protocol.
/// Epochs are sequential, starting from an initial value (often 0).
pub type EpochNum = u64;

/// Represents a round number within an Asynchronous Binary Agreement (ABA)
/// instance.
pub type AbaRound = u32;

/// A trait defining the requirements for a node identifier.
///
/// Node identifiers must be comparable, hashable, cloneable, and displayable,
/// as well as safe to send and share across threads. This allows the library
/// user to define custom types for node identification (e.g., public keys,
/// integer IDs).
pub trait NodeIdentifier:
    Debug + Display + Clone + Copy + PartialEq + Eq + PartialOrd + Ord + Hash + Sync + Send
{
}

/// Implements `NodeIdentifier` for `u32`.
/// Allows using `u32` values as node identifiers.
impl NodeIdentifier for u32 {}
/// Implements `NodeIdentifier` for `u64`.
/// Allows using `u64` values as node identifiers.
impl NodeIdentifier for u64 {}

/// Represents a transaction as a vector of bytes.
/// Transactions are opaque to the HoneyBadgerBFT protocol itself; their
/// interpretation is left to the application.
pub type Transaction = Vec<u8>;

/// Represents a batch of transactions.
///
/// A batch is a collection of transactions proposed by a node in a given epoch.
/// To ensure consistent hashing, transactions within a batch are stored in a
/// canonical order (sorted lexicographically).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Batch {
    /// The list of transactions included in this batch.
    /// Transactions are kept sorted to ensure canonical representation for
    /// hashing.
    pub transactions: Vec<Transaction>,
}

impl Batch {
    /// Creates a new `Batch` from a vector of transactions.
    ///
    /// The transactions will be sorted internally to ensure a canonical
    /// representation, which is crucial for consistent hashing across different
    /// nodes.
    ///
    /// ## Arguments
    ///
    /// * `transactions`: A `Vec<Transaction>` to be included in the batch. The
    ///   order of transactions in this input vector does not matter, as they
    ///   will be sorted.
    ///
    /// ## Returns
    ///
    /// A new `Batch` instance containing the sorted transactions.
    pub fn new(mut transactions: Vec<Transaction>) -> Self {
        transactions.sort(); // Ensure canonical order for hashing
        Batch { transactions }
    }
}

/// Represents the cryptographic hash of a `Batch`.
/// Typically, this is a SHA-256 hash.
pub type BatchHash = [u8; 32];

/// Computes the SHA-256 hash of a batch.
///
/// This function ensures a canonical hash by first sorting transactions within
/// the batch (if not already done by `Batch::new`) and then hashing them with
/// length prefixes to prevent ambiguity.
///
/// ## Arguments
///
/// * `batch`: A reference to the `Batch` to be hashed. It is assumed that
///   `batch.transactions` are already in a canonical (sorted) order, as
///   enforced by `Batch::new`.
///
/// ## Returns
///
/// A `BatchHash` (typically `[u8; 32]`) representing the SHA-256 digest of the
/// batch.
///
/// ## Notes
///
/// - The hashing process includes the length of each transaction before its
///   content. This prevents ambiguities where concatenations of different sets
///   of transactions might otherwise produce the same byte string.
pub fn hash_batch(batch: &Batch) -> BatchHash {
    let mut hasher = Sha256::new();
    // Assuming transactions within the batch are already sorted (e.g., by
    // Batch::new).
    for tx in &batch.transactions {
        // Prefix with length to avoid ambiguities, e.g. hash(tx1 || tx2) vs
        // hash(tx1_prefix_of_txA || txA_suffix_same_as_tx2)
        hasher.update((tx.len() as u64).to_le_bytes());
        hasher.update(tx);
    }
    let result = hasher.finalize();
    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(&result[..32]);
    hash_array
}

/// Output of a CommonCoin processing step.
#[derive(Debug, Default, Clone)] // Added Clone
pub struct CoinOutput {
    // Renamed to avoid confusion with AbaProcessOutput
    /// A share/message this node should broadcast for the coin protocol.
    pub share_to_broadcast: Option<CoinShare>,
    /// The final boolean value if the coin has been decided for the given
    /// (epoch, round).
    pub decided_value: Option<bool>,
    // Future consideration: Could also include coin-specific timer requests.
}

impl CoinOutput {
    pub fn empty() -> Self {
        CoinOutput::default()
    }

    pub fn new<S, V>(share: S, value: V) -> Self
    where
        S: Into<Option<CoinShare>>,
        V: Into<Option<bool>>,
    {
        CoinOutput {
            share_to_broadcast: share.into(),
            decided_value: value.into(),
        }
    }

    /// Creates a `CoinOutput` with the `share_to_broadcast` field set from the
    /// input, and `decided_value` set to `None`.
    ///
    /// The `share_input` can be either a direct `CoinShare` or an
    /// `Option<CoinShare>`.
    pub fn with_share<S>(share_input: S) -> Self
    where
        S: Into<Option<CoinShare>>,
    {
        CoinOutput {
            share_to_broadcast: share_input.into(),
            decided_value: None,
        }
    }

    /// Creates a `CoinOutput` with the `decided_value` field set from the
    /// input, and `share_to_broadcast` set to `None`.
    ///
    /// The `value_input` can be either a direct `bool` or an `Option<bool>`.
    pub fn with_value<V>(value_input: V) -> Self
    where
        V: Into<Option<bool>>,
    {
        CoinOutput {
            share_to_broadcast: None,
            decided_value: value_input.into(),
        }
    }

    /// Updates the `CoinOutput` with a new share to broadcast.
    pub fn and_share<S>(mut self, share_input: S) -> Self
    where
        S: Into<Option<CoinShare>>,
    {
        self.share_to_broadcast = share_input.into();
        self
    }

    /// Updates the `CoinOutput` with a new decided value.
    pub fn and_value<V>(mut self, value_input: V) -> Self
    where
        V: Into<Option<bool>>,
    {
        self.decided_value = value_input.into();
        self
    }
}

// =============================================================================
// MESSAGE TYPES
// =============================================================================

/// Represents the content of a message in the Reliable Broadcast (RBC)
/// sub-protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RbcMessageContent {
    /// A `VAL` message, typically sent by the proposer, containing the actual
    /// `Batch` being proposed.
    Val(Batch),
    /// An `ECHO` message, sent by nodes to indicate they have received and
    /// validated a `VAL` message (or an `ECHO` from others). It contains the
    /// `BatchHash` of the proposed batch.
    Echo(BatchHash),
    /// A `READY` message, sent by nodes when they have received a sufficient
    /// number of `ECHO` messages for a specific `BatchHash`. It also contains
    /// the `BatchHash`.
    Ready(BatchHash),
}

/// Represents a message for the Reliable Broadcast (RBC) sub-protocol.
///
/// Each RBC message is associated with a specific proposer and epoch, and
/// contains the actual content (`VAL`, `ECHO`, or `READY`) along with the
/// sender's ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RbcMessage<NodeId> {
    /// The ID of the node proposing the batch for the RBC instance this message
    /// belongs to.
    pub pub_proposer_id: NodeId,
    /// The epoch number this RBC message is for.
    pub pub_epoch: EpochNum,
    /// The actual content of the RBC message.
    pub pub_content: RbcMessageContent,
    /// The ID of the node that sent this message.
    pub pub_sender_id: NodeId,
}

/// Represents the content of a message in the Asynchronous Binary Agreement
/// (ABA) sub-protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AbaMessageContent {
    /// An `EST` (estimate) message, indicating the sender's initial or current
    /// estimate for the binary value in a given round.
    Est {
        /// The boolean value being estimated.
        value: bool,
        /// The ABA round number this estimate pertains to.
        round: AbaRound,
    },
    /// An `AUX` (auxiliary) message, sent when a node has collected enough
    /// `EST` messages for a particular value in a round.
    Aux {
        /// The boolean value supported by `EST` messages.
        value: bool,
        /// The ABA round number this `AUX` message pertains to.
        round: AbaRound,
    },
    /// A `COIN` message, containing a share for the common coin mechanism used
    /// to help ABA instances converge.
    Coin {
        /// The sender's share for the common coin.
        share: CoinShare,
        /// The ABA round number this coin share is for.
        round: AbaRound,
    },
}

/// Represents an opaque share for the common coin mechanism in ABA.
/// The specific format and interpretation of the share depend on the common
/// coin implementation being used.
pub type CoinShare = Vec<u8>;

/// Represents a message for the Asynchronous Binary Agreement (ABA)
/// sub-protocol.
///
/// Each ABA message is associated with a specific batch proposer (whose batch's
/// inclusion is being decided) and epoch. It contains the ABA content (`EST`,
/// `AUX`, or `COIN`) and the sender's ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AbaMessage<NodeId> {
    /// The ID of the node whose proposed batch's inclusion is being decided by
    /// the ABA instance this message belongs to.
    pub pub_batch_proposer_id: NodeId,
    /// The epoch number this ABA message is for.
    pub pub_epoch: EpochNum,
    /// The actual content of the ABA message.
    pub pub_content: AbaMessageContent,
    /// The ID of the node that sent this message.
    pub pub_sender_id: NodeId,
}

/// Represents a top-level message in the HoneyBadgerBFT protocol.
/// It can be either an RBC message or an ABA message.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HoneyBadgerMessage<NodeId> {
    /// A message belonging to a Reliable Broadcast (RBC) instance.
    Rbc(RbcMessage<NodeId>),
    /// A message belonging to an Asynchronous Binary Agreement (ABA) instance.
    Aba(AbaMessage<NodeId>),
}

impl<NodeId: NodeIdentifier> HoneyBadgerMessage<NodeId> {
    /// Returns the epoch number associated with this HoneyBadger message.
    ///
    /// ## Returns
    ///
    /// The `EpochNum` of the underlying RBC or ABA message.
    pub fn epoch(&self) -> EpochNum {
        match self {
            HoneyBadgerMessage::Rbc(msg) => msg.pub_epoch,
            HoneyBadgerMessage::Aba(msg) => msg.pub_epoch,
        }
    }

    /// Returns the sender's ID of this HoneyBadger message.
    ///
    /// ## Returns
    ///
    /// The `NodeId` of the node that sent the underlying RBC or ABA message.
    pub fn sender_id(&self) -> NodeId {
        match self {
            HoneyBadgerMessage::Rbc(msg) => msg.pub_sender_id,
            HoneyBadgerMessage::Aba(msg) => msg.pub_sender_id,
        }
    }
}

// =============================================================================
// TYPES FOR TIMER REQUESTS AND EXPIRIES
// =============================================================================

/// Represents a request to the host application to set a timer.
///
/// The HoneyBadgerBFT protocol logic is deterministic and does not manage time
/// directly. Instead, it outputs `TimerRequest`s when it needs a timeout to be
/// scheduled. The application is responsible for managing these timers and
/// notifying the protocol instance when they expire via `TimerExpiry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerRequest<NodeId> {
    /// A request to set a timeout for a specific round of an ABA instance.
    SetAbaRoundTimeout {
        /// The epoch number of the ABA instance.
        epoch: EpochNum,
        /// The ID of the node whose batch proposal the ABA instance is deciding
        /// on.
        batch_proposer_id: NodeId,
        /// The ABA round number for which the timeout is being set.
        round: AbaRound,
        /// The suggested duration for the timeout. The application may choose
        /// to use this or its own configured value.
        suggested_duration: Duration,
    },
    /// A request to set a timeout for an entire RBC instance.
    /// This is typically used to ensure liveness if a proposer is faulty or
    /// slow.
    SetRbcTimeout {
        /// The epoch number of the RBC instance.
        epoch: EpochNum,
        /// The ID of the node proposing the batch in the RBC instance.
        proposer_id: NodeId,
        /// The suggested duration for the timeout.
        suggested_duration: Duration,
    },
}

/// Represents the expiration of a previously set timer.
///
/// The host application sends this to the HoneyBadgerBFT instance to notify it
/// that a requested timer has fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerExpiry<NodeId> {
    /// Indicates that the timeout for a specific ABA round has expired.
    AbaRoundTimeout {
        /// The epoch number of the ABA instance.
        epoch: EpochNum,
        /// The ID of the node whose batch proposal the ABA instance was
        /// deciding on.
        batch_proposer_id: NodeId,
        /// The ABA round number that timed out.
        round: AbaRound,
    },
    /// Indicates that the timeout for an RBC instance has expired.
    RbcTimeout {
        /// The epoch number of the RBC instance.
        epoch: EpochNum,
        /// The ID of the node whose batch proposal the RBC instance was for.
        proposer_id: NodeId,
    },
}

// =============================================================================
// OUTPUT TYPES
// =============================================================================

/// Represents a batch that has been finalized by the HoneyBadgerBFT protocol
/// for a specific epoch and proposer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecidedBatch<NodeId> {
    /// The actual `Batch` of transactions that was decided.
    pub batch: Batch,
    /// The ID of the node that proposed this batch.
    pub proposer_id: NodeId,
    /// The epoch number in which this batch was decided.
    pub epoch: EpochNum,
}

/// Represents the complete output for a single epoch: a map from each
/// proposer's ID to the `Batch` they contributed that was included in the
/// finalized set for that epoch.
pub type EpochOutput<NodeId> = BTreeMap<NodeId, Batch>;

/// Bundles outputs from an ABA (Asynchronous Binary Agreement) processing step.
///
/// When an ABA instance processes a message or a timer expiry, it may need to
/// send new messages or request a new timer. This struct collects those
/// outputs.
#[derive(Debug, Clone)]
pub struct AbaProcessOutput<NodeId> {
    /// A list of `AbaMessage`s that should be broadcast to other nodes.
    pub messages_to_send: Vec<AbaMessage<NodeId>>,
    /// An optional `TimerRequest` if the ABA instance needs a new timer to be
    /// set.
    pub timer_request_to_set: Option<TimerRequest<NodeId>>,
}

impl<NodeId> Default for AbaProcessOutput<NodeId> {
    /// Creates a default, empty `AbaProcessOutput`.
    ///
    /// ## Returns
    ///
    /// An `AbaProcessOutput` with no messages to send and no timer request.
    fn default() -> Self {
        AbaProcessOutput {
            messages_to_send: Vec::new(),
            timer_request_to_set: None,
        }
    }
}

/// Bundles all outputs from a HoneyBadgerBFT processing step.
///
/// When the main `HoneyBadgerBft` instance processes an incoming message or a
/// timer expiry, it can result in various actions: messages to be sent to
/// peers, newly decided batches that form part of an epoch's output, and new
/// timers to be requested from the host application.
#[derive(Debug, Clone)]
pub struct HbbftProcessOutput<NodeId> {
    /// A list of `HoneyBadgerMessage`s (RBC or ABA) that should be broadcast
    /// to other nodes.
    pub messages_to_send: Vec<HoneyBadgerMessage<NodeId>>,
    /// A list of `DecidedBatch`es that were newly finalized in this processing
    /// step. These contribute to the overall output of an epoch.
    pub newly_decided_batches: Vec<DecidedBatch<NodeId>>,
    /// A list of `TimerRequest`s if the protocol needs new timers to be set by
    /// the application.
    pub timer_requests_to_set: Vec<TimerRequest<NodeId>>,
}

impl<NodeId> Default for HbbftProcessOutput<NodeId> {
    /// Creates a default, empty `HbbftProcessOutput`.
    ///
    /// ## Returns
    ///
    /// An `HbbftProcessOutput` with no messages to send, no newly decided
    /// batches, and no timer requests.
    fn default() -> Self {
        HbbftProcessOutput {
            messages_to_send: Vec::new(),
            newly_decided_batches: Vec::new(),
            timer_requests_to_set: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Batch Tests ---
    #[test]
    fn batch_new_sorts_transactions() {
        let tx1: Transaction = vec![1, 2, 3];
        let tx2: Transaction = vec![0, 1, 2];
        let tx3: Transaction = vec![4, 5];
        let batch = Batch::new(vec![tx1.clone(), tx3.clone(), tx2.clone()]);
        assert_eq!(batch.transactions, vec![tx2, tx1, tx3]);
    }

    #[test]
    fn batch_new_empty() {
        let batch = Batch::new(vec![]);
        assert!(batch.transactions.is_empty());
    }

    #[test]
    fn batch_new_single_transaction() {
        let tx1: Transaction = vec![1, 2, 3];
        let batch = Batch::new(vec![tx1.clone()]);
        assert_eq!(batch.transactions, vec![tx1]);
    }

    // --- hash_batch Tests ---
    #[test]
    fn hash_batch_consistency() {
        let tx1: Transaction = vec![1, 2, 3];
        let tx2: Transaction = vec![4, 5, 6];
        let batch1 = Batch::new(vec![tx1.clone(), tx2.clone()]);
        let batch2 = Batch::new(vec![tx1.clone(), tx2.clone()]); // Identical content
        assert_eq!(hash_batch(&batch1), hash_batch(&batch2));
    }

    #[test]
    fn hash_batch_difference() {
        let tx1: Transaction = vec![1, 2, 3];
        let tx2: Transaction = vec![4, 5, 6];
        let tx3: Transaction = vec![7, 8, 9];
        let batch1 = Batch::new(vec![tx1.clone(), tx2.clone()]);
        let batch2 = Batch::new(vec![tx1.clone(), tx3.clone()]);
        assert_ne!(hash_batch(&batch1), hash_batch(&batch2));
    }

    #[test]
    fn hash_batch_order_invariance_due_to_batch_new() {
        let tx1: Transaction = vec![1, 0, 0];
        let tx2: Transaction = vec![0, 1, 0];
        let batch1 = Batch::new(vec![tx1.clone(), tx2.clone()]); // tx1, tx2
        let batch2 = Batch::new(vec![tx2.clone(), tx1.clone()]); // tx2, tx1 (Batch::new sorts to tx1, tx2)
        // Internally, batch1 and batch2 will have transactions sorted as [tx2, tx1]
        // because tx2 ([0,1,0]) comes before tx1 ([1,0,0]) lexicographically.
        // Let's re-verify the sorting order for clarity in the test.
        let sorted_txs = {
            let mut txs = vec![tx1.clone(), tx2.clone()];
            txs.sort();
            txs
        };
        assert_eq!(batch1.transactions, sorted_txs);
        assert_eq!(batch2.transactions, sorted_txs);
        assert_eq!(hash_batch(&batch1), hash_batch(&batch2));
    }

    #[test]
    fn hash_batch_ambiguity_prevention_with_length_prefix() {
        // Scenario 1: tx_a = [1,2], tx_b = [3] -> batch = [[1,2], [3]]
        let tx_a: Transaction = vec![1, 2];
        let tx_b: Transaction = vec![3];
        let batch1 = Batch::new(vec![tx_a, tx_b]); // Sorted: [[3], [1,2]]

        // Scenario 2: tx_c = [1,2,3] -> batch = [[1,2,3]]
        let tx_c: Transaction = vec![1, 2, 3];
        let batch2 = Batch::new(vec![tx_c]);

        // Scenario 3: tx_d = [1], tx_e = [2,3] -> batch = [[1], [2,3]]
        let tx_d: Transaction = vec![1];
        let tx_e: Transaction = vec![2, 3];
        let batch3 = Batch::new(vec![tx_d, tx_e]); // Sorted: [[1], [2,3]]

        // If no length prefix, hash([1,2] || [3]) might be same as hash([1,2,3])
        // or hash([1] || [2,3]) might be same as hash([1,2,3])
        // With length prefixing, they should be different.

        let hash1 = hash_batch(&batch1);
        let hash2 = hash_batch(&batch2);
        let hash3 = hash_batch(&batch3);

        assert_ne!(hash1, hash2, "Hash of [[3], [1,2]] should differ from [[1,2,3]]");
        assert_ne!(hash3, hash2, "Hash of [[1], [2,3]] should differ from [[1,2,3]]");
        // Also ensure batch1 and batch3 are different if their contents are truly
        // different after sorting
        assert_ne!(hash1, hash3, "Hash of [[3], [1,2]] should differ from [[1], [2,3]]");
    }
}
