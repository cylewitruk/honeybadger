// This content is intended for src/config.rs

use std::collections::BTreeSet;

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
}

impl HoneyBadgerConfig {
    pub fn new(
        own_id: NodeId,
        all_node_ids: BTreeSet<NodeId>,
        num_faulty_nodes: usize,
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

        Ok(HoneyBadgerConfig {
            own_id,
            all_node_ids,
            num_total_nodes,  // Corrected: remove pub from here
            num_faulty_nodes, // Corrected: remove pub from here
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
