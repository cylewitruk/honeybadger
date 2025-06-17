mod aba;
mod config;
mod error;
mod hbbft;
mod rbc;
mod types;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

// Core state machine and configuration
pub use aba::CommonCoin;
pub use config::{CoinFactory, HoneyBadgerConfig, HoneyBadgerConfigBuilder};
pub use error::HbbftError;
pub use hbbft::HoneyBadgerBft;
// Message content types, public if users need to inspect message details
pub use types::{AbaMessage, AbaMessageContent, RbcMessage, RbcMessageContent};
// Primary input/output types for the HBBFT state machine
pub use types::{
    AbaRound, // Though less common for direct use, it's part of TimerRequest
    Batch,    // Useful for constructing/inspecting batches, e.g. in DecidedBatch
    BatchHash,
    CoinShare,
    DecidedBatch, // Represents a finalized batch
    EpochNum,
    EpochOutput,        // For get_epoch_output
    HbbftProcessOutput, // Bundles messages, decisions, and timer requests
    HoneyBadgerMessage, // Envelope for network messages
    NodeIdentifier,
    TimerExpiry,  // For application to notify HBBFT of timer events
    TimerRequest, // For application-managed timers
    Transaction,
};
