// This content is intended for src/hbbft.rs

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::aba::BinaryByzantineAgreement;
use crate::config::HoneyBadgerConfig;
use crate::error::HbbftError;
use crate::rbc::ReliableBroadcast;
use crate::types::{
    AbaProcessOutput, Batch, DecidedBatch, EpochNum, EpochOutput, HbbftProcessOutput,
    HoneyBadgerMessage, NodeId, TimerExpiry, TimerRequest, Transaction,
};

// Represents the state of HoneyBadgerBFT for a single node.
pub struct HoneyBadgerBft {
    config: HoneyBadgerConfig,
    current_epoch: EpochNum,
    own_id: NodeId, // For convenience

    // Per-epoch state:
    // For each (proposer_id, epoch), there is an RBC instance.
    // Map: epoch -> (proposer_id -> RBC instance)
    rbc_instances: HashMap<EpochNum, HashMap<NodeId, ReliableBroadcast>>,

    // For each (batch_proposer_id, epoch), there is an ABA instance.
    // Map: epoch -> (batch_proposer_id -> ABA instance)
    aba_instances: HashMap<EpochNum, HashMap<NodeId, BinaryByzantineAgreement>>,

    // Store intended proposed batches for epochs, if we are the proposer.
    // This stores the batch *before* its RBC is necessarily initiated.
    // epoch -> (our intended batch for our RBC in this epoch)
    my_epoch_proposals: HashMap<EpochNum, Batch>,

    // Store outputs of RBC instances that have decided.
    // epoch -> (proposer_id -> Batch)
    rbc_outputs: HashMap<EpochNum, HashMap<NodeId, Batch>>,

    // Store outputs of ABA instances that have decided.
    // epoch -> (batch_proposer_id -> bool)
    aba_outputs: HashMap<EpochNum, HashMap<NodeId, bool>>,

    // Final decided batches for each epoch, ready for the application.
    // epoch -> EpochOutput (BTreeMap<NodeId, Batch>)
    decided_epoch_outputs: BTreeMap<EpochNum, EpochOutput>,

    // Transactions waiting to be proposed in a batch.
    pending_transactions: Vec<Transaction>,
}

impl HoneyBadgerBft {
    pub fn new(config: HoneyBadgerConfig, initial_epoch: EpochNum) -> Self {
        let own_id = config.own_id;
        HoneyBadgerBft {
            config,
            current_epoch: initial_epoch,
            own_id,
            rbc_instances: HashMap::new(),
            aba_instances: HashMap::new(),
            my_epoch_proposals: HashMap::new(),
            rbc_outputs: HashMap::new(),
            aba_outputs: HashMap::new(),
            decided_epoch_outputs: BTreeMap::new(),
            pending_transactions: Vec::new(),
        }
    }

    /// Adds a transaction to the buffer for inclusion in a future batch.
    /// If this node had previously prepared an empty proposal for the current
    /// epoch and that proposal's RBC has not yet been initiated, submitting
    /// a new transaction will trigger a re-proposal for the current epoch
    /// with the new (and any other pending) transactions.
    pub fn submit_transaction(
        &mut self,
        tx: Transaction,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        self.pending_transactions.push(tx);
        // Transactions are now only buffered. The application must explicitly call
        // `propose_batch` or `prepare_and_propose_current_epoch` to include them.
        Ok(HbbftProcessOutput::default())
    }

    /// Records the intent to propose a batch for a given epoch and may initiate
    /// its broadcast. This node will propose `own_batch_txs` via its RBC
    /// instance for this epoch. It also prepares to participate in RBC/ABA
    /// for all other nodes.
    pub fn propose_batch(
        &mut self,
        own_batch_txs: Vec<Transaction>,
        epoch_num: EpochNum,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        if epoch_num < self.current_epoch {
            return Err(HbbftError::EpochMismatch {
                expected: self.current_epoch,
                got: epoch_num,
            });
        }

        let batch_to_propose = Batch::new(own_batch_txs);
        let mut hbbft_output = HbbftProcessOutput::default();

        // Ensure instances are active and get initial timer requests for other
        // proposers' RBCs.
        if let Some(timer_reqs) = self.ensure_epoch_active_and_get_timers(epoch_num) {
            hbbft_output
                .timer_requests_to_set
                .extend(timer_reqs);
        }

        // Check if our RBC for this epoch has already been initiated.
        let rbc_is_initiated = self
            .rbc_instances
            .get(&epoch_num)
            .and_then(|epoch_map| epoch_map.get(&self.own_id))
            .is_some_and(|rbc| rbc.is_initiated());

        if rbc_is_initiated {
            // RBC has started, we cannot change the proposal content.
            if self.my_epoch_proposals.get(&epoch_num) != Some(&batch_to_propose) {
                eprintln!(
                    "[HBBFT] Warning: Node {} attempted to propose different batch for epoch {} \
                     after its RBC was initiated.",
                    self.own_id, epoch_num
                );
            }
            // No new RBC messages from us, but timer requests for others are
            // still valid.
        } else {
            // RBC not initiated, so we can set/update our intended proposal.
            self.my_epoch_proposals
                .insert(epoch_num, batch_to_propose);
            // DbgPrint!("[HBBFT] Node {} recorded proposal intent for epoch
            // {}", self.own_id, epoch_num);
        }

        // Attempt to broadcast our own proposal if it's for the current epoch and
        // ready. This call will check `is_initiated` again internally.
        if epoch_num == self.current_epoch {
            let broadcast_output = self.try_broadcast_own_proposal_if_ready(epoch_num)?;
            hbbft_output
                .messages_to_send
                .extend(broadcast_output.messages_to_send);
            // `try_broadcast_own_proposal_if_ready` doesn't set timers itself.
        }

        Ok(hbbft_output)
    }

    /// Handles an incoming HoneyBadgerMessage.
    pub fn handle_message(
        &mut self,
        message: HoneyBadgerMessage,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        let message_epoch = message.epoch();
        if message_epoch < self.current_epoch
            && self
                .decided_epoch_outputs
                .contains_key(&message_epoch)
        {
            return Ok(HbbftProcessOutput::default());
        }

        let mut hbbft_output = HbbftProcessOutput::default();
        if let Some(timer_reqs) = self.ensure_epoch_active_and_get_timers(message_epoch) {
            hbbft_output
                .timer_requests_to_set
                .extend(timer_reqs);
        }

        match message {
            HoneyBadgerMessage::Rbc(rbc_msg) => {
                let proposer_id = rbc_msg.pub_proposer_id;
                if let Some(rbc_epoch_map) = self
                    .rbc_instances
                    .get_mut(&message_epoch)
                {
                    if let Some(rbc_instance) = rbc_epoch_map.get_mut(&proposer_id) {
                        let responses = rbc_instance.handle_message(&rbc_msg)?;
                        for resp in responses {
                            hbbft_output
                                .messages_to_send
                                .push(HoneyBadgerMessage::Rbc(resp));
                        }

                        let output_already_recorded = self
                            .rbc_outputs
                            .get(&message_epoch)
                            .is_some_and(|epoch_map| epoch_map.contains_key(&proposer_id));

                        if !output_already_recorded && rbc_instance.is_decided() {
                            if let Some(decided_batch) = rbc_instance.output() {
                                self.store_rbc_output(message_epoch, proposer_id, decided_batch);
                                let aba_init_output: AbaProcessOutput =
                                    self.initiate_aba_for_rbc(message_epoch, proposer_id, true)?;
                                hbbft_output.messages_to_send.extend(
                                    aba_init_output
                                        .messages_to_send
                                        .into_iter()
                                        .map(HoneyBadgerMessage::Aba),
                                );
                                if let Some(timer_req) = aba_init_output.timer_request_to_set {
                                    hbbft_output
                                        .timer_requests_to_set
                                        .push(timer_req);
                                }
                            }
                        }
                    }
                }
            }
            HoneyBadgerMessage::Aba(aba_msg) => {
                let batch_proposer_id = aba_msg.pub_batch_proposer_id;
                if let Some(aba_epoch_map) = self
                    .aba_instances
                    .get_mut(&message_epoch)
                {
                    if let Some(aba_instance) = aba_epoch_map.get_mut(&batch_proposer_id) {
                        if !aba_instance.is_decided() {
                            let aba_proc_output = aba_instance.handle_message(&aba_msg)?;
                            for resp in aba_proc_output.messages_to_send {
                                hbbft_output
                                    .messages_to_send
                                    .push(HoneyBadgerMessage::Aba(resp));
                            }
                            if let Some(timer_req) = aba_proc_output.timer_request_to_set {
                                hbbft_output
                                    .timer_requests_to_set
                                    .push(timer_req);
                            }
                            if aba_instance.is_decided() {
                                if let Some(decided_value) = aba_instance.output() {
                                    self.store_aba_output(
                                        message_epoch,
                                        batch_proposer_id,
                                        decided_value,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        if message_epoch >= self.current_epoch
            && !self
                .decided_epoch_outputs
                .contains_key(&message_epoch)
        {
            let finalize_output = self.try_finalize_epoch(message_epoch)?;
            hbbft_output
                .newly_decided_batches
                .extend(finalize_output.newly_decided_batches);
            hbbft_output
                .messages_to_send
                .extend(finalize_output.messages_to_send);
            hbbft_output
                .timer_requests_to_set
                .extend(finalize_output.timer_requests_to_set);
        }

        // After processing and potential finalization, try broadcasting own proposal
        // for current epoch.
        let broadcast_output = self.try_broadcast_own_proposal_if_ready(self.current_epoch)?;
        hbbft_output
            .messages_to_send
            .extend(broadcast_output.messages_to_send);
        // No new timers from try_broadcast... itself.

        Ok(hbbft_output)
    }

    /// Handles a timer expiry event notified by the consuming application.
    pub fn handle_timer_expiry(
        &mut self,
        expiry: TimerExpiry,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        let mut hbbft_output = HbbftProcessOutput::default();

        // Determine the epoch affected by this timer expiry and handle the specific
        // timeout logic.
        let affected_epoch = match expiry {
            TimerExpiry::AbaRoundTimeout {
                epoch,
                batch_proposer_id,
                round,
            } => {
                if let Some(aba_epoch_map) = self.aba_instances.get_mut(&epoch) {
                    if let Some(aba_instance) = aba_epoch_map.get_mut(&batch_proposer_id) {
                        // Only process timeout if it's for the current round of an undecided ABA
                        if !aba_instance.is_decided() && round == aba_instance.current_round_debug()
                        {
                            let aba_proc_output = aba_instance.handle_round_timeout(round)?;
                            hbbft_output.messages_to_send.extend(
                                aba_proc_output
                                    .messages_to_send
                                    .into_iter()
                                    .map(HoneyBadgerMessage::Aba),
                            );
                            if let Some(timer_req) = aba_proc_output.timer_request_to_set {
                                hbbft_output
                                    .timer_requests_to_set
                                    .push(timer_req);
                            }
                        }
                    }
                }
                epoch // This is the epoch affected by this timer
            }
            TimerExpiry::RbcTimeout { epoch, proposer_id } => {
                let rbc_has_delivered = self
                    .rbc_outputs
                    .get(&epoch)
                    .is_some_and(|epoch_map| epoch_map.contains_key(&proposer_id));

                if !rbc_has_delivered {
                    let aba_init_output = self.initiate_aba_for_rbc(epoch, proposer_id, false)?;
                    hbbft_output.messages_to_send.extend(
                        aba_init_output
                            .messages_to_send
                            .into_iter()
                            .map(HoneyBadgerMessage::Aba),
                    );
                    if let Some(timer_req) = aba_init_output.timer_request_to_set {
                        hbbft_output
                            .timer_requests_to_set
                            .push(timer_req);
                    }
                }
                epoch // This is the epoch affected by this timer
            }
        };

        if affected_epoch >= self.current_epoch
            && !self
                .decided_epoch_outputs
                .contains_key(&affected_epoch)
        {
            let finalize_output = self.try_finalize_epoch(affected_epoch)?;
            hbbft_output
                .newly_decided_batches
                .extend(finalize_output.newly_decided_batches);
            hbbft_output
                .messages_to_send
                .extend(finalize_output.messages_to_send);
            hbbft_output
                .timer_requests_to_set
                .extend(finalize_output.timer_requests_to_set);
        }

        // After processing and potential finalization, try broadcasting own proposal
        // for current epoch.
        let broadcast_output = self.try_broadcast_own_proposal_if_ready(self.current_epoch)?;
        hbbft_output
            .messages_to_send
            .extend(broadcast_output.messages_to_send);

        Ok(hbbft_output)
    }

    /// Creates RBC and ABA instances for all N nodes for a given epoch if they
    /// don't exist. This is an internal helper, primarily for instance
    /// creation.
    fn ensure_epoch_instances_internal(&mut self, epoch_num: EpochNum) {
        if epoch_num < self.current_epoch
            && self
                .decided_epoch_outputs
                .contains_key(&epoch_num)
        {
            return;
        }

        let own_id = self.own_id;
        let shared_config = Arc::new(self.config.clone());
        self.rbc_instances
            .entry(epoch_num)
            .or_insert_with(|| {
                shared_config
                    .all_node_ids
                    .iter()
                    .map(|&node_id| {
                        let rbc = ReliableBroadcast::new(
                            Arc::clone(&shared_config),
                            node_id,
                            epoch_num,
                            own_id,
                        );
                        (node_id, rbc)
                    })
                    .collect()
            });
        self.aba_instances
            .entry(epoch_num)
            .or_insert_with(|| {
                shared_config
                    .all_node_ids
                    .iter()
                    .map(|&node_id| {
                        let coin = shared_config.coin_factory.create_coin(
                            node_id,
                            epoch_num,
                            Arc::clone(&shared_config),
                        );
                        let aba = BinaryByzantineAgreement::new(
                            Arc::clone(&shared_config),
                            node_id,
                            epoch_num,
                            own_id,
                            coin,
                        );
                        (node_id, aba)
                    })
                    .collect()
            });
    }

    /// Ensures instances for an epoch are created and returns TimerRequests for
    /// RBCs of other proposers if their output is not yet known.
    fn ensure_epoch_active_and_get_timers(
        &mut self,
        epoch_num: EpochNum,
    ) -> Option<Vec<TimerRequest>> {
        if epoch_num < self.current_epoch
            && self
                .decided_epoch_outputs
                .contains_key(&epoch_num)
        {
            return None;
        }

        self.ensure_epoch_instances_internal(epoch_num);

        let mut rbc_timer_requests = Vec::new();

        for proposer_id_ref in self.config.all_node_ids.iter() {
            let proposer_id = *proposer_id_ref;
            if proposer_id == self.own_id {
                continue;
            }

            if !self
                .rbc_outputs
                .get(&epoch_num)
                .is_some_and(|epoch_map| epoch_map.contains_key(&proposer_id))
            {
                rbc_timer_requests.push(TimerRequest::SetRbcTimeout {
                    epoch: epoch_num,
                    proposer_id,
                    suggested_duration_ms: self
                        .config
                        .default_rbc_instance_timeout_ms,
                });
            }
        }

        if rbc_timer_requests.is_empty() { None } else { Some(rbc_timer_requests) }
    }

    fn store_rbc_output(&mut self, epoch_num: EpochNum, proposer_id: NodeId, batch: Batch) {
        self.rbc_outputs
            .entry(epoch_num)
            .or_default()
            .insert(proposer_id, batch);
    }

    fn initiate_aba_for_rbc(
        &mut self,
        epoch_num: EpochNum,
        batch_proposer_id: NodeId,
        rbc_delivered_value: bool,
    ) -> Result<AbaProcessOutput, HbbftError> {
        if let Some(aba_map) = self.aba_instances.get_mut(&epoch_num) {
            if let Some(aba_instance) = aba_map.get_mut(&batch_proposer_id) {
                if !aba_instance.is_decided()
                    && aba_instance.current_round_debug() == 0
                    && aba_instance.output().is_none()
                {
                    return aba_instance.propose(rbc_delivered_value);
                }
            }
        }
        Ok(AbaProcessOutput::default())
    }

    fn store_aba_output(&mut self, epoch_num: EpochNum, batch_proposer_id: NodeId, value: bool) {
        self.aba_outputs
            .entry(epoch_num)
            .or_default()
            .insert(batch_proposer_id, value);
    }

    /// Checks if all ABA instances for a given epoch have decided.
    /// If so, collects the final output for that epoch.
    fn try_finalize_epoch(
        &mut self,
        epoch_num: EpochNum,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        let mut output = HbbftProcessOutput::default();
        if self
            .decided_epoch_outputs
            .contains_key(&epoch_num)
        {
            return Ok(output);
        }

        let num_nodes = self.config.num_total_nodes;
        let all_abas_decided = self
            .aba_outputs
            .get(&epoch_num)
            .is_some_and(|outputs| outputs.len() == num_nodes);

        if all_abas_decided {
            let mut final_epoch_output = EpochOutput::new();
            let epoch_aba_out = self
                .aba_outputs
                .get(&epoch_num)
                .unwrap();

            for node_id_ref in self.config.all_node_ids.iter() {
                let node_id = *node_id_ref;
                if let Some(true) = epoch_aba_out.get(&node_id) {
                    if let Some(batch) = self
                        .rbc_outputs
                        .get(&epoch_num)
                        .and_then(|epoch_map| epoch_map.get(&node_id))
                    {
                        final_epoch_output.insert(node_id, batch.clone());
                        output
                            .newly_decided_batches
                            .push(DecidedBatch {
                                batch: batch.clone(),
                                proposer_id: node_id,
                                epoch: epoch_num,
                            });
                    } else {
                        return Err(HbbftError::InternalError(format!(
                            "Epoch {epoch_num}: ABA for {node_id} output true, but corresponding \
                             RBC batch missing."
                        )));
                    }
                }
            }
            self.decided_epoch_outputs
                .insert(epoch_num, final_epoch_output);

            if epoch_num == self.current_epoch {
                self.current_epoch += 1;
                // The decision to propose for the new self.current_epoch is now up to the
                // application. It can call submit_transaction then propose_batch,
                // or call prepare_and_propose_current_epoch.
                // We only advance the epoch number here.
                self.prune_old_epoch_state();
            }
        }
        Ok(output)
    }

    /// Retrieves all finalized batches for a given epoch.
    pub fn get_epoch_output(&self, epoch_num: EpochNum) -> Option<&EpochOutput> {
        self.decided_epoch_outputs
            .get(&epoch_num)
    }

    /// Returns the current epoch number this node is working on or has last
    /// completed.
    pub fn current_epoch(&self) -> EpochNum {
        self.current_epoch
    }

    /// Prepares and proposes a batch for the current epoch using pending
    /// transactions.
    pub fn prepare_and_propose_current_epoch(&mut self) -> Result<HbbftProcessOutput, HbbftError> {
        let txs_to_propose = std::mem::take(&mut self.pending_transactions);
        // `propose_batch` will handle instance creation, timer requests for others,
        // and initiating its own RBC if not already done (via
        // try_broadcast_own_proposal_if_ready).
        self.propose_batch(txs_to_propose, self.current_epoch)
    }

    /// Prunes state for epochs that are older than the retention window.
    fn prune_old_epoch_state(&mut self) {
        let epochs_to_retain = self.config.epochs_to_retain_state;
        if let Some((&max_finalized_epoch, _)) = self
            .decided_epoch_outputs
            .iter()
            .next_back()
        {
            let oldest_epoch_to_keep_details =
                max_finalized_epoch.saturating_sub(epochs_to_retain.saturating_sub(1));

            self.rbc_instances
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
            self.aba_instances
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
            self.my_epoch_proposals
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
            self.rbc_outputs
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
            self.aba_outputs
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
            self.decided_epoch_outputs
                .retain(|&epoch, _| epoch >= oldest_epoch_to_keep_details);
        }
    }

    /// Checks if this node has an intended proposal for `epoch_num` and if its
    /// RBC has not been initiated. If so, initiates the RBC and returns
    /// messages.
    fn try_broadcast_own_proposal_if_ready(
        &mut self,
        epoch_num: EpochNum,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        let mut output = HbbftProcessOutput::default();

        if epoch_num < self.current_epoch {
            return Ok(output);
        }

        if let Some(batch_to_propose) = self
            .my_epoch_proposals
            .get(&epoch_num)
            .cloned()
        {
            // Ensure instances are created for this epoch. This is important if this method
            // is called before ensure_epoch_active_and_get_timers has run for this epoch.
            self.ensure_epoch_instances_internal(epoch_num);

            if let Some(rbc_epoch_map) = self.rbc_instances.get_mut(&epoch_num) {
                if let Some(our_rbc_instance) = rbc_epoch_map.get_mut(&self.own_id) {
                    if !our_rbc_instance.is_initiated() && !our_rbc_instance.is_decided() {
                        // DbgPrint!("[HBBFT] Node {} broadcasting own proposal for epoch {}",
                        // self.own_id, epoch_num);
                        match our_rbc_instance.propose(batch_to_propose) {
                            Ok(rbc_messages) => {
                                for rbc_msg in rbc_messages {
                                    output
                                        .messages_to_send
                                        .push(HoneyBadgerMessage::Rbc(rbc_msg));
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    // --- Getters and Helpers ---
    pub fn config(&self) -> &HoneyBadgerConfig {
        &self.config
    }

    pub fn own_id(&self) -> NodeId {
        self.own_id
    }
}
