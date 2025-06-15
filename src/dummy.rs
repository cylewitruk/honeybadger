//! This module is conditionally compiled and only available if the "dummy"
//! feature is enabled.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::aba::CommonCoin;
use crate::config::{CoinFactory, HoneyBadgerConfig, HoneyBadgerConfigBuilder};
use crate::types::{AbaRound, CoinShare, EpochNum, NodeId};

#[derive(Debug)]
pub struct DummyCommonCoin;

impl CommonCoin for DummyCommonCoin {
    fn request_coin_value(
        &mut self,
        _epoch: EpochNum,
        round: AbaRound,
    ) -> (Option<CoinShare>, Option<bool>) {
        (None, Some(round % 2 == 0)) // No shares to send, coin is immediately known
    }

    fn submit_coin_share(
        &mut self,
        _epoch: EpochNum,
        _round: AbaRound,
        _share_content: CoinShare,
        _sender_id: NodeId,
    ) -> Option<bool> {
        None // Dummy coin doesn't process shares
    }

    fn get_coin_for_round_fallback(&mut self, _epoch: EpochNum, round: AbaRound) -> bool {
        round % 2 == 0 // Fallback is the same predictable logic
    }
}

/// A mock common coin that requires F+1 shares to decide.
/// The coin value itself is deterministic based on the round number.
#[derive(Debug)]
pub struct MockInteractiveCoin {
    config: Arc<HoneyBadgerConfig>, // To access F+1 threshold
    own_id: NodeId,
    // Stores received shares for each round: round -> set of sender_ids
    received_shares_for_round: HashMap<AbaRound, HashSet<NodeId>>,
}

impl MockInteractiveCoin {
    pub fn new(config: Arc<HoneyBadgerConfig>, own_id: NodeId) -> Self {
        MockInteractiveCoin {
            config,
            own_id,
            received_shares_for_round: HashMap::new(),
        }
    }
}

impl CommonCoin for MockInteractiveCoin {
    fn request_coin_value(
        &mut self,
        _epoch: EpochNum,
        round: AbaRound,
    ) -> (Option<CoinShare>, Option<bool>) {
        // Generate a unique "share" for this node and round
        let own_share = format!("share_ep{}_r{}_id{}", _epoch, round, self.own_id).into_bytes();
        (Some(own_share), None) // No immediate decision, shares must be exchanged
    }

    fn submit_coin_share(
        &mut self,
        _epoch: EpochNum,
        round: AbaRound,
        _share_content: CoinShare, // We don't inspect the content for this mock
        sender_id: NodeId,
    ) -> Option<bool> {
        let shares_for_this_round = self
            .received_shares_for_round
            .entry(round)
            .or_default();
        shares_for_this_round.insert(sender_id);

        if shares_for_this_round.len() >= self.config.low_threshold() {
            // F+1 shares
            Some(round % 2 == 0) // Deterministic coin outcome
        } else {
            None // Not enough shares yet
        }
    }

    fn get_coin_for_round_fallback(&mut self, _epoch: EpochNum, round: AbaRound) -> bool {
        round % 2 == 0 // Fallback is the same predictable logic
    }
}

/// Creates a default `HoneyBadgerConfig` using `DummyCommonCoin` for testing
/// purposes.
pub fn create_dummy_config(n: usize, f: usize, own_id: NodeId) -> Arc<HoneyBadgerConfig> {
    let all_node_ids: BTreeSet<NodeId> = (0..n as NodeId).collect();
    let dummy_coin_factory = CoinFactory::new(Arc::new(|_node_id, _epoch, _config| {
        Box::new(DummyCommonCoin {}) as Box<dyn CommonCoin>
    }));

    Arc::new(
        HoneyBadgerConfigBuilder::new(own_id, all_node_ids, f, dummy_coin_factory)
            .epochs_to_retain_state(3)
            .default_aba_round_timeout_ms(1000)
            .default_rbc_instance_timeout_ms(2000)
            .aba_max_future_round_window(5)
            .build()
            .unwrap(),
    )
}

/// Creates a `HoneyBadgerConfig` using `MockInteractiveCoin` for integration
/// testing.
pub fn create_interactive_mock_config(
    n: usize,
    f: usize,
    own_id: NodeId,
) -> Arc<HoneyBadgerConfig> {
    let all_node_ids: BTreeSet<NodeId> = (0..n as NodeId).collect();
    let interactive_coin_factory =
        CoinFactory::new(Arc::new(move |node_id, _epoch, config_for_coin| {
            Box::new(MockInteractiveCoin::new(config_for_coin, node_id)) as Box<dyn CommonCoin>
        }));

    Arc::new(
        HoneyBadgerConfigBuilder::new(own_id, all_node_ids.clone(), f, interactive_coin_factory)
            .build()
            .unwrap(),
    )
}
