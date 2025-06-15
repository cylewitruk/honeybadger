//! This module is conditionally compiled and only available if the "dummy"
//! feature is enabled.

use std::collections::BTreeSet;
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
