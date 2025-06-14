// This content is intended for src/types.rs

use std::collections::BTreeMap;

use sha2::{Digest, Sha256}; // For hashing

// --- Basic Identifiers ---
pub type NodeId = u64;
pub type EpochNum = u64;
pub type AbaRound = u32;

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
    /// Creates a new Batch. Transactions will be sorted internally for canonical representation.
    pub fn new(mut transactions: Vec<Transaction>) -> Self {
        transactions.sort(); // Ensure canonical order for hashing
        Batch { transactions }
    }
}

pub type BatchHash = [u8; 32]; // e.g., SHA-256 hash

/// Computes the SHA256 hash of a batch.
/// Relies on the `transactions` field within Batch being in a canonical order.
/// (e.g., sorted lexicographically). The `Batch::new` constructor can enforce this.
pub fn hash_batch(batch: &Batch) -> BatchHash {
    let mut hasher = Sha256::new();
    // Assuming transactions within the batch are already sorted (e.g., by Batch::new).
    for tx in &batch.transactions {
        // Prefix with length to avoid ambiguities, e.g. hash(tx1 || tx2) vs hash(tx1_prefix_of_txA
        // || txA_suffix_same_as_tx2)
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
pub struct RbcMessage {
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
pub struct AbaMessage {
    pub pub_batch_proposer_id: NodeId,
    pub pub_epoch: EpochNum,
    pub pub_content: AbaMessageContent,
    pub pub_sender_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HoneyBadgerMessage {
    Rbc(RbcMessage),
    Aba(AbaMessage),
}

// --- Timer-related types for interaction with the consuming application ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerRequest {
    SetAbaRoundTimeout {
        epoch: EpochNum,
        // Identifies which ABA instance this timer is for within the HBBFT context
        batch_proposer_id: NodeId,
        round: AbaRound,
        // Note: The actual duration of the timeout will be determined by the
        // consuming application based on its configuration or heuristics.
    },
    SetRbcTimeout {
        epoch: EpochNum,
        proposer_id: NodeId, // The proposer of the RBC instance
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerExpiry {
    AbaRoundTimeout { epoch: EpochNum, batch_proposer_id: NodeId, round: AbaRound },
    RbcTimeout { epoch: EpochNum, proposer_id: NodeId },
}

// --- Output Structures ---
// DecidedBatch might still be useful to derive Serialize/Deserialize
// if the application consuming the output of HBBFT wants to easily serialize it,
// but it's not strictly necessary for the core HBBFT logic itself.
// For now, we'll keep it plain.
#[derive(Clone, Debug, PartialEq, Eq)] // Removed Serialize/Deserialize for now
pub struct DecidedBatch {
    pub batch: Batch,
    pub proposer_id: NodeId,
    pub epoch: EpochNum,
}

// Map from proposer_id to their accepted Batch for a given epoch
pub type EpochOutput = BTreeMap<NodeId, Batch>;

/// Bundles outputs from an ABA processing step.
#[derive(Debug, Default, Clone)]
pub struct AbaProcessOutput {
    pub messages_to_send: Vec<AbaMessage>,
    pub timer_request_to_set: Option<TimerRequest>,
}

/// Bundles outputs from an HBBFT processing step.
#[derive(Debug, Default, Clone)]
pub struct HbbftProcessOutput {
    pub messages_to_send: Vec<HoneyBadgerMessage>,
    pub newly_decided_batches: Vec<DecidedBatch>,
    pub timer_requests_to_set: Vec<TimerRequest>,
}

// --- Threshold Cryptography Placeholders ---
// These would also be opaque byte vectors if they were part of messages.
pub type CommonCoinPubKey = Vec<u8>;
pub type CommonCoinSigShare = Vec<u8>;

// --- Error Type ---
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HbbftError {
    InvalidMessage(String),
    InternalError(String),
    MisbehavingNode { node_id: NodeId, reason: String },
    EpochMismatch { expected: EpochNum, got: EpochNum },
    NotEnoughShares,
    ConfigurationError(String),
    // Potentially an error variant for serialization/deserialization issues
    // if the library were to handle raw bytes at its boundary:
    // SerializationError(String),
    // DeserializationError(String),
}

impl HoneyBadgerMessage {
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
