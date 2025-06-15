use std::collections::BTreeSet;
use std::sync::Arc;

use crate::aba::{CommonCoin, DummyCommonCoin};
use crate::config::{CoinFactory, HoneyBadgerConfig, HoneyBadgerConfigBuilder};
use crate::types::NodeId;

/// Creates a default `HoneyBadgerConfig` for testing purposes.
/// Uses `DummyCommonCoin` and sets explicit default values consistent
/// across tests.
pub fn create_test_config(n: usize, f: usize, own_id: NodeId) -> Arc<HoneyBadgerConfig> {
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
