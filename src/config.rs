use std::collections::BTreeSet;
use std::sync::Arc;

use crate::EpochNum;
use crate::aba::CommonCoin;
use crate::types::{AbaRound, NodeId};

/// Default number of recent finalized epochs for which to retain detailed
/// protocol state.
const DEFAULT_EPOCHS_TO_RETAIN_STATE: EpochNum = 3;
/// Default timeout duration in milliseconds for an ABA round.
const DEFAULT_ABA_ROUND_TIMEOUT_MS: u64 = 2000;
/// Default timeout duration in milliseconds for an RBC instance.
const DEFAULT_RBC_INSTANCE_TIMEOUT_MS: u64 = 5000;
/// Default maximum number of rounds into the future for which an ABA instance
/// will queue messages.
const DEFAULT_ABA_MAX_FUTURE_ROUND_WINDOW: AbaRound = 5;

/// A factory function for creating CommonCoin instances.
///
/// This type alias defines the signature for a function that can produce
/// `CommonCoin` trait objects. It's used by `CoinFactory` to allow
/// dependency injection of different common coin mechanisms.
type ActualCoinFactoryFn =
    dyn Fn(NodeId, EpochNum, Arc<HoneyBadgerConfig>) -> Box<dyn CommonCoin> + Send + Sync;

/// A wrapper for a common coin factory function.
///
/// This struct encapsulates the logic for creating `CommonCoin` instances.
/// It's designed to be cloneable and debuggable, facilitating its use within
/// `HoneyBadgerConfig`. The actual factory function is wrapped in an `Arc`
/// to allow shared ownership.
///
/// ## Notes
/// - The primary purpose of this wrapper is to enable `HoneyBadgerConfig` to
///   derive `Clone` and `Debug`, as function pointers or `dyn Fn` types
///   themselves don't directly support these traits in a straightforward manner
///   for struct derivation.
pub struct CoinFactory(Arc<ActualCoinFactoryFn>);

impl CoinFactory {
    pub fn new(factory_fn: Arc<ActualCoinFactoryFn>) -> Self {
        CoinFactory(factory_fn)
    }

    /// Creates a new `CommonCoin` instance using the wrapped factory function.
    ///
    /// ## Arguments
    ///
    /// * `node_id`: The ID of the node for which the coin is being created.
    /// * `epoch_num`: The epoch number for which the coin will be used.
    /// * `config`: A shared reference to the `HoneyBadgerConfig`.
    ///
    /// ## Returns
    ///
    /// A `Box<dyn CommonCoin>` instance.
    pub fn create_coin(
        &self,
        node_id: NodeId,
        epoch_num: EpochNum,
        config: Arc<HoneyBadgerConfig>,
    ) -> Box<dyn CommonCoin> {
        (self.0)(node_id, epoch_num, config)
    }
}

/// Configuration for a HoneyBadgerBFT instance.
///
/// This struct holds all necessary parameters for initializing and running
/// a HoneyBadgerBFT node. It includes network information, fault tolerance
/// parameters, timeout settings, and a factory for creating common coin
/// instances.
///
/// It is recommended to use `HoneyBadgerConfigBuilder` to construct instances
/// of this struct to ensure all parameters are validated.
#[derive(Clone, Debug)]
pub struct HoneyBadgerConfig {
    /// The ID of the node this configuration belongs to.
    pub own_id: NodeId,
    /// A sorted set of all node IDs participating in the consensus.
    pub all_node_ids: BTreeSet<NodeId>,
    /// `N`: The total number of nodes in the network.
    pub num_total_nodes: usize,
    /// `F`: The maximum number of faulty (Byzantine) nodes the system is
    /// configured to tolerate. Typically, `N >= 3F + 1`.
    pub num_faulty_nodes: usize,
    /// The number of recent finalized epochs for which detailed protocol state
    /// (like RBC/ABA instances and messages) should be retained. Older state
    /// will be pruned.
    pub epochs_to_retain_state: EpochNum,
    /// The default timeout duration in milliseconds for an ABA (Asynchronous
    /// Binary Agreement) round.
    pub default_aba_round_timeout_ms: u64,
    /// The default timeout duration in milliseconds for an RBC (Reliable
    /// Broadcast) instance if it doesn't receive a `VAL` message from its
    /// proposer.
    pub default_rbc_instance_timeout_ms: u64,
    /// A factory for creating `CommonCoin` instances for ABA sub-protocols.
    pub coin_factory: CoinFactory,
    /// The maximum number of rounds into the future (relative to an ABA
    /// instance's current round) for which it will queue received `EST` or
    /// `AUX` messages.
    pub aba_max_future_round_window: AbaRound,
}

impl Clone for CoinFactory {
    fn clone(&self) -> Self {
        CoinFactory(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for CoinFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CoinFactory")
            .field(&"<factory_fn>")
            .finish()
    }
}

/// A builder for creating `HoneyBadgerConfig` instances.
///
/// This builder provides a fluent API for setting configuration parameters
/// and includes validation logic in its `build` method to ensure a consistent
/// and valid configuration.
#[derive(Debug)]
pub struct HoneyBadgerConfigBuilder {
    // Mandatory fields
    own_id: NodeId,
    all_node_ids: BTreeSet<NodeId>,
    num_faulty_nodes: usize,
    coin_factory: CoinFactory,

    // Fields with defaults
    epochs_to_retain_state: EpochNum,
    default_aba_round_timeout_ms: u64,
    default_rbc_instance_timeout_ms: u64,
    aba_max_future_round_window: AbaRound,
}

impl HoneyBadgerConfigBuilder {
    /// Creates a new builder for `HoneyBadgerConfig`.
    ///
    /// ## Arguments
    ///
    /// * `own_id`: The ID of this node.
    /// * `all_node_ids`: A set of all node IDs in the network.
    /// * `num_faulty_nodes`: The maximum number of faulty nodes (F) the system
    ///   should tolerate.
    /// * `coin_factory`: A factory to produce `CommonCoin` instances.
    pub fn new(
        own_id: NodeId,
        all_node_ids: BTreeSet<NodeId>,
        num_faulty_nodes: usize,
        coin_factory: CoinFactory,
    ) -> Self {
        HoneyBadgerConfigBuilder {
            own_id,
            all_node_ids,
            num_faulty_nodes,
            coin_factory,
            epochs_to_retain_state: DEFAULT_EPOCHS_TO_RETAIN_STATE,
            default_aba_round_timeout_ms: DEFAULT_ABA_ROUND_TIMEOUT_MS,
            default_rbc_instance_timeout_ms: DEFAULT_RBC_INSTANCE_TIMEOUT_MS,
            aba_max_future_round_window: DEFAULT_ABA_MAX_FUTURE_ROUND_WINDOW,
        }
    }

    /// Sets the number of recent finalized epochs for which detailed protocol
    /// state should be retained.
    ///
    /// Default: `DEFAULT_EPOCHS_TO_RETAIN_STATE` (3). Must be at least 1.
    pub fn epochs_to_retain_state(mut self, epochs: EpochNum) -> Self {
        self.epochs_to_retain_state = epochs;
        self
    }

    /// Sets the default timeout duration in milliseconds for an ABA round.
    ///
    /// Default: `DEFAULT_ABA_ROUND_TIMEOUT_MS` (2000ms). Must be greater than
    /// 0.
    pub fn default_aba_round_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.default_aba_round_timeout_ms = timeout_ms;
        self
    }

    /// Sets the default timeout duration in milliseconds for an RBC instance.
    ///
    /// Default: `DEFAULT_RBC_INSTANCE_TIMEOUT_MS` (5000ms). Must be greater
    /// than 0.
    pub fn default_rbc_instance_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.default_rbc_instance_timeout_ms = timeout_ms;
        self
    }

    /// Sets the maximum number of future rounds for which an ABA instance will
    /// queue messages.
    ///
    /// Messages for rounds beyond `current_round + window` will be discarded.
    /// Default: `DEFAULT_ABA_MAX_FUTURE_ROUND_WINDOW` (5). Must be at least 1.
    pub fn aba_max_future_round_window(mut self, window: AbaRound) -> Self {
        self.aba_max_future_round_window = window;
        self
    }

    /// Builds the `HoneyBadgerConfig` after validating the parameters.
    ///
    /// ## Returns
    ///
    /// * `Ok(HoneyBadgerConfig)`: If all parameters are valid.
    /// * `Err(String)`: If any validation check fails, containing an error
    ///   message.
    pub fn build(self) -> Result<HoneyBadgerConfig, String> {
        // Validation logic moved here from the old HoneyBadgerConfig::new
        let own_id = self.own_id; // To avoid self.own_id in format strings if self is consumed
        let num_faulty_nodes = self.num_faulty_nodes; // Keep this for the format string below
        let num_total_nodes = self.all_node_ids.len(); // Corrected: use self.all_node_ids

        if num_total_nodes == 0 {
            return Err("Total number of nodes (N) cannot be zero.".to_string());
        }
        if num_faulty_nodes >= num_total_nodes {
            return Err("Number of faulty nodes (F) must be less than total nodes (N).".to_string());
        }
        // Common HBBFT requirement: N >= 3F + 1
        if num_total_nodes < 3 * num_faulty_nodes + 1 {
            return Err(format!(
                "Invalid configuration: N ({num_total_nodes}) must be >= 3F ({num_faulty_nodes}) \
                 + 1. N={num_total_nodes} F={num_faulty_nodes}"
            ));
        }
        if !self.all_node_ids.contains(&own_id) {
            return Err(format!("Own ID {own_id} not found in the set of all node IDs."));
        }
        if self.epochs_to_retain_state == 0 {
            // Require at least 1 to avoid ambiguity or overly aggressive pruning.
            return Err("epochs_to_retain_state must be at least 1.".to_string());
        }
        if self.default_aba_round_timeout_ms == 0 {
            return Err("default_aba_round_timeout_ms must be greater than 0.".to_string());
        }
        if self.default_rbc_instance_timeout_ms == 0 {
            return Err("default_rbc_instance_timeout_ms must be greater than 0.".to_string());
        }
        if self.aba_max_future_round_window == 0 {
            return Err("aba_max_future_round_window must be at least 1.".to_string());
        }

        Ok(HoneyBadgerConfig {
            own_id,
            all_node_ids: self.all_node_ids,
            num_total_nodes,
            num_faulty_nodes,
            epochs_to_retain_state: self.epochs_to_retain_state,
            default_aba_round_timeout_ms: self.default_aba_round_timeout_ms,
            default_rbc_instance_timeout_ms: self.default_rbc_instance_timeout_ms,
            coin_factory: self.coin_factory,
            aba_max_future_round_window: self.aba_max_future_round_window,
        })
    }
}

impl HoneyBadgerConfig {
    /// Returns the high threshold for message counts, typically `N - F`.
    ///
    /// This is often used as a quorum size for strong agreement, e.g., needing
    /// `N - F` `ECHO` or `READY` messages in RBC, or `N - F` `AUX` messages
    /// in ABA to decide.
    pub fn high_threshold(&self) -> usize {
        self.num_total_nodes - self.num_faulty_nodes
    }

    /// Returns the low threshold for message counts, typically `F + 1`.
    ///
    /// This is often used as a quorum size for weaker conditions or to ensure
    /// at least one honest node is included, e.g., needing `F + 1` `EST`
    /// messages to send an `AUX` in ABA, or `F + 1` `READY` messages to
    /// relay a `READY` in RBC.
    pub fn low_threshold(&self) -> usize {
        self.num_faulty_nodes + 1
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::testing::DummyCommonCoin;
    use crate::types::NodeId;

    fn create_node_ids(n: usize) -> BTreeSet<NodeId> {
        (0..n as NodeId).collect()
    }

    #[test]
    fn config_new_valid() {
        let all_ids = create_node_ids(4);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(0, all_ids, 1, dummy_coin_factory)
            .epochs_to_retain_state(3)
            .default_aba_round_timeout_ms(1000)
            .default_rbc_instance_timeout_ms(2000)
            .aba_max_future_round_window(5)
            .build();
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.num_total_nodes, 4);
        assert_eq!(config.num_faulty_nodes, 1);
    }

    #[test]
    fn config_new_n_zero() {
        let all_ids = create_node_ids(0);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(0, all_ids, 0, dummy_coin_factory).build();
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "Total number of nodes (N) cannot be zero.");
    }

    #[test]
    fn config_new_f_ge_n() {
        let all_ids = create_node_ids(3);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config =
            HoneyBadgerConfigBuilder::new(0, all_ids.clone(), 3, dummy_coin_factory.clone())
                .build(); // F = N
        assert!(config.is_err());
        assert_eq!(
            config.unwrap_err(),
            "Number of faulty nodes (F) must be less than total nodes (N)."
        );

        let config_f_gt_n =
            HoneyBadgerConfigBuilder::new(0, all_ids, 4, dummy_coin_factory).build(); // F > N
        assert!(config_f_gt_n.is_err());
    }

    #[test]
    fn config_new_n_lt_3f_plus_1() {
        let all_ids = create_node_ids(3); // N=3
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(0, all_ids, 1, dummy_coin_factory).build(); // F=1. N=3, 3F+1=4. N < 3F+1
        assert!(config.is_err());
        assert_eq!(
            config.unwrap_err(),
            "Invalid configuration: N (3) must be >= 3F (1) + 1. N=3 F=1"
        );
    }

    #[test]
    fn config_new_own_id_not_in_all_ids() {
        let all_ids = create_node_ids(4);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(5, all_ids, 1, dummy_coin_factory).build(); // own_id 5 not in 0..3
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "Own ID 5 not found in the set of all node IDs.");
    }

    #[test]
    fn config_new_epochs_to_retain_state_zero() {
        let all_ids = create_node_ids(4);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(0, all_ids, 1, dummy_coin_factory)
            .epochs_to_retain_state(0)
            .build();
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "epochs_to_retain_state must be at least 1.");
    }

    #[test]
    fn config_new_timeout_zero() {
        let all_ids = create_node_ids(4);
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config_aba_timeout =
            HoneyBadgerConfigBuilder::new(0, all_ids.clone(), 1, dummy_coin_factory.clone())
                .default_aba_round_timeout_ms(0)
                .build();
        assert!(config_aba_timeout.is_err());
        assert_eq!(
            config_aba_timeout.unwrap_err(),
            "default_aba_round_timeout_ms must be greater than 0."
        );

        let config_rbc_timeout =
            HoneyBadgerConfigBuilder::new(0, all_ids.clone(), 1, dummy_coin_factory.clone())
                .default_rbc_instance_timeout_ms(0)
                .build();
        assert!(config_rbc_timeout.is_err());
        assert_eq!(
            config_rbc_timeout.unwrap_err(),
            "default_rbc_instance_timeout_ms must be greater than 0."
        );

        let config_future_window_zero =
            HoneyBadgerConfigBuilder::new(0, all_ids, 1, dummy_coin_factory)
                .aba_max_future_round_window(0)
                .build();
        assert!(config_future_window_zero.is_err());
        assert_eq!(
            config_future_window_zero.unwrap_err(),
            "aba_max_future_round_window must be at least 1."
        );
    }

    #[test]
    fn config_threshold_methods() {
        let all_ids = create_node_ids(7); // N=7
        let dummy_coin_factory = CoinFactory::new(Arc::new(|_, _, _| Box::new(DummyCommonCoin {})));
        let config = HoneyBadgerConfigBuilder::new(0, all_ids, 2, dummy_coin_factory)
            .build()
            .unwrap(); // F=2
        assert_eq!(config.high_threshold(), 5); // N-F = 7-2 = 5
        assert_eq!(config.low_threshold(), 3); // F+1 = 2+1 = 3
    }
}
