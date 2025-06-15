// This content is intended for src/config.rs

use std::collections::BTreeSet;

use crate::EpochNum;
use crate::types::NodeId;

#[derive(Clone, Debug)]
pub struct HoneyBadgerConfig {
    pub own_id: NodeId,                 // ID of this node
    pub all_node_ids: BTreeSet<NodeId>, // Set of all node IDs in the network
    pub num_total_nodes: usize,         // N: Total number of nodes
    pub num_faulty_nodes: usize,        /* F: Maximum number of faulty nodes to tolerate (e.g.,
                                         * N = 3F + 1)
                                         * Other parameters might include:
                                         * - Timeout values (though HBBFT is asynchronous,
                                         *   practical implementations might use timeouts)
                                         * - Threshold crypto public keys / key shares
                                         * - Max batch size, etc. */
    // Number of recent finalized epochs for which to retain detailed protocol state.
    pub epochs_to_retain_state: EpochNum,
    pub default_aba_round_timeout_ms: u64,
    pub default_rbc_instance_timeout_ms: u64,
}

impl HoneyBadgerConfig {
    pub fn new(
        own_id: NodeId,
        all_node_ids: BTreeSet<NodeId>,
        num_faulty_nodes: usize,
        epochs_to_retain_state: EpochNum, // New parameter
        default_aba_round_timeout_ms: u64,
        default_rbc_instance_timeout_ms: u64,
    ) -> Result<Self, String> {
        let num_total_nodes = all_node_ids.len();
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
        if !all_node_ids.contains(&own_id) {
            return Err(format!("Own ID {own_id} not found in the set of all node IDs."));
        }
        if epochs_to_retain_state == 0 {
            // Require at least 1 to avoid ambiguity or overly aggressive pruning.
            return Err("epochs_to_retain_state must be at least 1.".to_string());
        }
        if default_aba_round_timeout_ms == 0 {
            return Err("default_aba_round_timeout_ms must be greater than 0.".to_string());
        }
        if default_rbc_instance_timeout_ms == 0 {
            return Err("default_rbc_instance_timeout_ms must be greater than 0.".to_string());
        }

        Ok(HoneyBadgerConfig {
            own_id,
            all_node_ids,
            num_total_nodes,
            num_faulty_nodes,
            epochs_to_retain_state,
            default_aba_round_timeout_ms,
            default_rbc_instance_timeout_ms,
        })
    }

    // Quorum thresholds
    pub fn high_threshold(&self) -> usize {
        // Typically N - F
        self.num_total_nodes - self.num_faulty_nodes
    }

    pub fn low_threshold(&self) -> usize {
        // Typically F + 1
        self.num_faulty_nodes + 1
    }

    pub fn aba_quorum_threshold(&self) -> usize {
        // This often is N-F or related to what makes ABA proceed.
        // A common choice for ABA related to Bracha's broadcast for example is N-F for echos,
        // and F+1 for readies to ensure liveness, or N-2F if it's about common subset for votes.
        // Let's assume F+1 for collecting enough ESTs/AUXs to send next message,
        // and N-F for strong support (like for Ready in RBC or certain ABA states).
        // This needs to be consistent with the specific ABA protocol being implemented.
        // For now, using N-F for strong support, F+1 for weaker support.
        // A typical threshold for collecting messages to make progress in ABA might be F+1.
        // A threshold for forming a set for agreement in CONF might be N-F.
        // The common coin often needs N-F shares.
        // For collecting messages in steps of ABA, e.g. for receiving EST_i(v) from enough parties
        // usually F+1 suffices for a some conditions, N-F for others.
        // For Byzantine Agreement (BA) on a bit:
        // - To set your own value based on others: need to see N-F proposals for one value.
        // - To accept a value as decided: need to see N-F confirmations.
        // For now, this is a general "quorum". We might need more specific ones.
        self.num_total_nodes - self.num_faulty_nodes // N - F
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::types::NodeId;

    fn create_node_ids(n: usize) -> BTreeSet<NodeId> {
        (0..n as NodeId).collect()
    }

    #[test]
    fn config_new_valid() {
        let all_ids = create_node_ids(4);
        let config = HoneyBadgerConfig::new(0, all_ids, 1, 3, 1000, 2000);
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.num_total_nodes, 4);
        assert_eq!(config.num_faulty_nodes, 1);
    }

    #[test]
    fn config_new_n_zero() {
        let all_ids = create_node_ids(0);
        let config = HoneyBadgerConfig::new(0, all_ids, 0, 3, 1000, 2000);
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "Total number of nodes (N) cannot be zero.");
    }

    #[test]
    fn config_new_f_ge_n() {
        let all_ids = create_node_ids(3);
        let config = HoneyBadgerConfig::new(0, all_ids.clone(), 3, 3, 1000, 2000); // F = N
        assert!(config.is_err());
        assert_eq!(
            config.unwrap_err(),
            "Number of faulty nodes (F) must be less than total nodes (N)."
        );

        let config_f_gt_n = HoneyBadgerConfig::new(0, all_ids, 4, 3, 1000, 2000); // F > N
        assert!(config_f_gt_n.is_err());
    }

    #[test]
    fn config_new_n_lt_3f_plus_1() {
        let all_ids = create_node_ids(3); // N=3
        let config = HoneyBadgerConfig::new(0, all_ids, 1, 3, 1000, 2000); // F=1. N=3, 3F+1=4. N < 3F+1
        assert!(config.is_err());
        assert_eq!(
            config.unwrap_err(),
            "Invalid configuration: N (3) must be >= 3F (1) + 1. N=3 F=1"
        );
    }

    #[test]
    fn config_new_own_id_not_in_all_ids() {
        let all_ids = create_node_ids(4);
        let config = HoneyBadgerConfig::new(5, all_ids, 1, 3, 1000, 2000); // own_id 5 not in 0..3
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "Own ID 5 not found in the set of all node IDs.");
    }

    #[test]
    fn config_new_epochs_to_retain_state_zero() {
        let all_ids = create_node_ids(4);
        let config = HoneyBadgerConfig::new(0, all_ids, 1, 0, 1000, 2000);
        assert!(config.is_err());
        assert_eq!(config.unwrap_err(), "epochs_to_retain_state must be at least 1.");
    }

    #[test]
    fn config_new_timeout_zero() {
        let all_ids = create_node_ids(4);
        let config_aba_timeout = HoneyBadgerConfig::new(0, all_ids.clone(), 1, 3, 0, 2000);
        assert!(config_aba_timeout.is_err());
        assert_eq!(
            config_aba_timeout.unwrap_err(),
            "default_aba_round_timeout_ms must be greater than 0."
        );

        let config_rbc_timeout = HoneyBadgerConfig::new(0, all_ids, 1, 3, 1000, 0);
        assert!(config_rbc_timeout.is_err());
        assert_eq!(
            config_rbc_timeout.unwrap_err(),
            "default_rbc_instance_timeout_ms must be greater than 0."
        );
    }

    #[test]
    fn config_threshold_methods() {
        let all_ids = create_node_ids(7); // N=7
        let config = HoneyBadgerConfig::new(0, all_ids, 2, 3, 1000, 2000).unwrap(); // F=2
        assert_eq!(config.high_threshold(), 5); // N-F = 7-2 = 5
        assert_eq!(config.low_threshold(), 3); // F+1 = 2+1 = 3
        assert_eq!(config.aba_quorum_threshold(), 5); // N-F = 7-2 = 5
    }
}
