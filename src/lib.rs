mod aba;
mod config;
mod hbbft;
mod rbc;
mod thresh_crypto;
mod types;

// --- Public API Exports ---

// Core state machine and configuration
pub use config::HoneyBadgerConfig;
pub use hbbft::HoneyBadgerBft;
// Message content types, public if users need to inspect message details
pub use types::{AbaMessage, AbaMessageContent, RbcMessage, RbcMessageContent};
// Primary input/output types for the HBBFT state machine
pub use types::{
    AbaRound, // Though less common for direct use, it's part of TimerRequest
    Batch,    // Useful for constructing/inspecting batches, e.g. in DecidedBatch
    BatchHash,
    DecidedBatch, // Represents a finalized batch
    EpochNum,
    EpochOutput,        // For get_epoch_output
    HbbftError,         // Error type
    HbbftProcessOutput, // Bundles messages, decisions, and timer requests
    HoneyBadgerMessage, // Envelope for network messages
    NodeId,
    TimerRequest, // For application-managed timers
    Transaction,
};
