// This content is intended for src/types.rs

use std::collections::BTreeMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::time::Duration;

use sha2::{Digest, Sha256}; // For hashing

// --- Basic Identifiers ---
// pub type NodeId = u64;
pub type EpochNum = u64;
pub type AbaRound = u32;

pub trait NodeIdentifier:
    Debug + Display + Clone + Copy + PartialEq + Eq + PartialOrd + Ord + Hash + Sync + Send
{
}

impl NodeIdentifier for u32 {}
impl NodeIdentifier for u64 {}

// --- Transaction and Batch ---
pub type Transaction = Vec<u8>; // A transaction is a sequence of bytes

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Batch {
    // Transactions are stored sorted to ensure that identical sets of transactions
    // always result in the same Batch representation for hashing.
    // The proposer is responsible for sorting them before creating the Batch.
    // Alternatively, the `new` method for Batch could enforce this.
    pub transactions: Vec<Transaction>,
}

impl Batch {
    /// Creates a new Batch. Transactions will be sorted internally for
    /// canonical representation.
    pub fn new(mut transactions: Vec<Transaction>) -> Self {
        transactions.sort(); // Ensure canonical order for hashing
        Batch { transactions }
    }
}

pub type BatchHash = [u8; 32]; // e.g., SHA-256 hash

/// Computes the SHA256 hash of a batch.
/// Relies on the `transactions` field within Batch being in a canonical order.
/// (e.g., sorted lexicographically). The `Batch::new` constructor can enforce
/// this.
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

// --- Messages ---
// These structs will NOT have serde::Serialize/Deserialize derived.
// The consuming crate (e.g., the libp2p integration) will be responsible
// for defining how these are serialized/deserialized for network transport.

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RbcMessageContent {
    Val(Batch),
    Echo(BatchHash),
    Ready(BatchHash),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RbcMessage<NodeId> {
    pub pub_proposer_id: NodeId,
    pub pub_epoch: EpochNum,
    pub pub_content: RbcMessageContent,
    pub pub_sender_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AbaMessageContent {
    Est { value: bool, round: AbaRound },
    Aux { value: bool, round: AbaRound },
    Coin { share: CoinShare, round: AbaRound },
}

pub type CoinShare = Vec<u8>; // Represents an opaque share for the common coin.

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AbaMessage<NodeId> {
    pub pub_batch_proposer_id: NodeId,
    pub pub_epoch: EpochNum,
    pub pub_content: AbaMessageContent,
    pub pub_sender_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HoneyBadgerMessage<NodeId> {
    Rbc(RbcMessage<NodeId>),
    Aba(AbaMessage<NodeId>),
}

// --- Timer-related types for interaction with the consuming application ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerRequest<NodeId> {
    SetAbaRoundTimeout {
        epoch: EpochNum,
        // Identifies which ABA instance this timer is for within the HBBFT context
        batch_proposer_id: NodeId,
        round: AbaRound,
        suggested_duration: Duration,
    },
    SetRbcTimeout {
        epoch: EpochNum,
        proposer_id: NodeId, // The proposer of the RBC instance
        suggested_duration: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerExpiry<NodeId> {
    AbaRoundTimeout { epoch: EpochNum, batch_proposer_id: NodeId, round: AbaRound },
    RbcTimeout { epoch: EpochNum, proposer_id: NodeId },
}

// --- Output Structures ---
// DecidedBatch might still be useful to derive Serialize/Deserialize
// if the application consuming the output of HBBFT wants to easily serialize
// it, but it's not strictly necessary for the core HBBFT logic itself.
// For now, we'll keep it plain.
#[derive(Clone, Debug, PartialEq, Eq)] // Removed Serialize/Deserialize for now
pub struct DecidedBatch<NodeId> {
    pub batch: Batch,
    pub proposer_id: NodeId,
    pub epoch: EpochNum,
}

// Map from proposer_id to their accepted Batch for a given epoch
pub type EpochOutput<NodeId> = BTreeMap<NodeId, Batch>;

/// Bundles outputs from an ABA processing step.
#[derive(Debug, Clone)]
pub struct AbaProcessOutput<NodeId> {
    pub messages_to_send: Vec<AbaMessage<NodeId>>,
    pub timer_request_to_set: Option<TimerRequest<NodeId>>,
}

impl<NodeId> Default for AbaProcessOutput<NodeId> {
    fn default() -> Self {
        AbaProcessOutput {
            messages_to_send: Vec::new(),
            timer_request_to_set: None,
        }
    }
}

/// Bundles outputs from an HBBFT processing step.
#[derive(Debug, Clone)]
pub struct HbbftProcessOutput<NodeId> {
    pub messages_to_send: Vec<HoneyBadgerMessage<NodeId>>,
    pub newly_decided_batches: Vec<DecidedBatch<NodeId>>,
    pub timer_requests_to_set: Vec<TimerRequest<NodeId>>,
}

impl<NodeId> Default for HbbftProcessOutput<NodeId> {
    fn default() -> Self {
        HbbftProcessOutput {
            messages_to_send: Vec::new(),
            newly_decided_batches: Vec::new(),
            timer_requests_to_set: Vec::new(),
        }
    }
}

impl<NodeId: NodeIdentifier> HoneyBadgerMessage<NodeId> {
    pub fn epoch(&self) -> EpochNum {
        match self {
            HoneyBadgerMessage::Rbc(msg) => msg.pub_epoch,
            HoneyBadgerMessage::Aba(msg) => msg.pub_epoch,
        }
    }

    pub fn sender_id(&self) -> NodeId {
        match self {
            HoneyBadgerMessage::Rbc(msg) => msg.pub_sender_id,
            HoneyBadgerMessage::Aba(msg) => msg.pub_sender_id,
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
