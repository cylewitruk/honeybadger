use crate::{EpochNum, NodeId};

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
