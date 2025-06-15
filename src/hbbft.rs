// This content is intended for src/hbbft.rs

//! Implements the main HoneyBadgerBFT state machine.
//!
//! This module orchestrates the Reliable Broadcast (RBC) and Asynchronous
//! Binary Agreement (ABA) sub-protocols to achieve byzantine fault-tolerant
//! consensus on batches of transactions across multiple epochs.

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

/// Represents the state of the HoneyBadgerBFT protocol for a single node.
///
/// This struct manages the overall consensus process, including handling
/// incoming messages, managing sub-protocol instances (RBC and ABA) for
/// various epochs and proposers, processing timer expiries, and producing
/// finalized epoch outputs.
pub struct HoneyBadgerBft {
    /// Shared configuration for this HBBFT instance.
    config: HoneyBadgerConfig,
    /// The current epoch number this node is actively working on or has most
    /// recently completed.
    current_epoch: EpochNum,
    /// The ID of this node. Stored for convenience.
    own_id: NodeId,

    /// Stores `ReliableBroadcast` instances.
    /// Mapping: `EpochNum` -> (`NodeId` (proposer) -> `ReliableBroadcast`
    /// instance).
    rbc_instances: HashMap<EpochNum, HashMap<NodeId, ReliableBroadcast>>,

    /// Stores `BinaryByzantineAgreement` instances.
    /// Mapping: `EpochNum` -> (`NodeId` (batch_proposer) ->
    /// `BinaryByzantineAgreement` instance).
    aba_instances: HashMap<EpochNum, HashMap<NodeId, BinaryByzantineAgreement>>,

    /// Stores batches this node intends to propose for specific epochs.
    /// This is populated before the node's own RBC instance for that epoch is
    /// necessarily initiated. Mapping: `EpochNum` -> `Batch` (this node's
    /// intended proposal for that epoch).
    my_epoch_proposals: HashMap<EpochNum, Batch>,

    /// Stores batches delivered by `ReliableBroadcast` instances.
    /// Mapping: `EpochNum` -> (`NodeId` (proposer) -> `Batch`).
    rbc_outputs: HashMap<EpochNum, HashMap<NodeId, Batch>>,

    /// Stores the boolean decisions from `BinaryByzantineAgreement` instances.
    /// Mapping: `EpochNum` -> (`NodeId` (batch_proposer) -> `bool` (decision)).
    aba_outputs: HashMap<EpochNum, HashMap<NodeId, bool>>,

    /// Stores the final, ordered set of decided batches for each epoch, ready
    /// for the application. Mapping: `EpochNum` -> `EpochOutput` (which is
    /// a `BTreeMap<NodeId, Batch>`).
    decided_epoch_outputs: BTreeMap<EpochNum, EpochOutput>,

    /// A buffer of transactions submitted by the application, waiting to be
    /// included in a future batch proposal by this node.
    pending_transactions: Vec<Transaction>,
}

impl HoneyBadgerBft {
    /// Creates a new `HoneyBadgerBft` instance.
    ///
    /// ## Arguments
    ///
    /// * `config`: The configuration for this HBBFT instance.
    /// * `initial_epoch`: The epoch number at which this node will start.
    ///
    /// ## Returns
    /// A new `HoneyBadgerBft` instance.
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
    ///
    /// Transactions are buffered and will be included when this node next
    /// proposes a batch, typically triggered by `propose_batch` or
    /// `prepare_and_propose_current_epoch`.
    ///
    /// ## Arguments
    ///
    /// * `tx`: The `Transaction` to be submitted.
    ///
    /// ## Returns
    ///
    /// * `Ok(HbbftProcessOutput)`: Currently returns a default, empty output as
    ///   this method only buffers the transaction.
    /// * `Err(HbbftError)`: This path currently does not return errors.
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
    /// its broadcast if it's for the current epoch.
    ///
    /// This method allows the application to specify the exact content of a
    /// batch this node should propose for `epoch_num`. It also ensures that
    /// the necessary RBC and ABA instances are initialized for `epoch_num`
    /// to allow participation with other nodes.
    ///
    /// ## Arguments
    ///
    /// * `own_batch_txs`: A vector of `Transaction`s to be included in this
    ///   node's proposed batch.
    /// * `epoch_num`: The epoch for which this batch is being proposed.
    ///
    /// ## Returns
    /// * `Ok(HbbftProcessOutput)`: Contains messages to send (e.g., initial RBC
    ///   messages if proposing for the current epoch) and timer requests.
    /// * `Err(HbbftError)`: If `epoch_num` is less than the current epoch.
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
    ///
    /// This is the primary entry point for processing messages received from
    /// other nodes. It routes the message to the appropriate RBC or ABA
    /// sub-protocol instance, processes the output from that instance, and
    /// attempts to finalize the epoch if enough sub-protocols have
    /// completed.
    ///
    /// ## Arguments
    ///
    /// * `message`: The `HoneyBadgerMessage` received from the network.
    ///
    /// ## Returns
    ///
    /// * `Ok(HbbftProcessOutput)`: Contains any messages to send, newly decided
    ///   batches, and timer requests.
    /// * `Err(HbbftError)`: If an error occurs during message processing (e.g.,
    ///   epoch mismatch).
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
    ///
    /// This method processes timer expiries for RBC instances (e.g., waiting
    /// for a proposer's `VAL`) or ABA rounds. It triggers the appropriate
    /// timeout handling logic in the sub-protocols and attempts to finalize
    /// the epoch if the timeout leads to further progress.
    ///
    /// ## Arguments
    ///
    /// * `expiry`: The `TimerExpiry` event that occurred.
    ///
    /// ## Returns
    /// * `Ok(HbbftProcessOutput)`: Contains any messages to send, newly decided
    ///   batches, and timer requests.
    /// * `Err(HbbftError)`: If an error occurs during timeout processing.
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
    /// don't already exist.
    ///
    /// This is an internal helper method used to ensure that the necessary
    /// sub-protocol instances are initialized when an epoch becomes active,
    /// either through a proposal or by receiving a message for that epoch.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch for which to ensure instances are created.
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

    /// A test-only wrapper around `ensure_epoch_active_and_get_timers`.
    ///
    /// This method is exposed for testing purposes to allow direct invocation
    /// of the internal epoch activation logic.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch to activate.
    ///
    /// ## Notes
    /// - This method should only be used in test code.
    #[cfg(any(test, feature = "testing"))]
    pub fn _ensure_epoch_active_and_get_timers(&mut self, epoch_num: EpochNum) {
        self.ensure_epoch_active_and_get_timers(epoch_num);
    }

    /// Ensures instances for an epoch are created and returns TimerRequests for
    /// RBCs of other proposers if their output is not yet known for that epoch.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch to check and activate.
    ///
    /// ## Returns
    /// `Some(Vec<TimerRequest>)` if any RBC timers need to be set for other
    /// proposers, `None` otherwise (e.g., if the epoch is old and decided,
    /// or all RBCs for others have already delivered).
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

    /// Stores the output (a delivered batch) of an RBC instance.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch of the RBC instance.
    /// * `proposer_id`: The proposer ID for the RBC instance.
    /// * `batch`: The `Batch` that was delivered by the RBC.
    fn store_rbc_output(&mut self, epoch_num: EpochNum, proposer_id: NodeId, batch: Batch) {
        self.rbc_outputs
            .entry(epoch_num)
            .or_default()
            .insert(proposer_id, batch);
    }

    /// Initiates an ABA instance for a given proposer's batch within an epoch.
    ///
    /// This is typically called after an RBC instance either delivers a batch
    /// (leading to proposing `true` in ABA) or times out (leading to
    /// proposing `false` in ABA).
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch number.
    /// * `batch_proposer_id`: The ID of the proposer whose batch this ABA is
    ///   for.
    /// * `rbc_delivered_value`: `true` if the corresponding RBC delivered a
    ///   batch, `false` otherwise.
    ///
    /// ## Returns
    /// * `Ok(AbaProcessOutput)`: Output from the ABA `propose` call.
    /// * `Err(HbbftError)`: If an error occurs.
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

    /// Stores the output (a boolean decision) of an ABA instance.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch of the ABA instance.
    /// * `batch_proposer_id`: The proposer ID for whom the ABA instance
    ///   decided.
    /// * `value`: The `bool` decision from the ABA.
    fn store_aba_output(&mut self, epoch_num: EpochNum, batch_proposer_id: NodeId, value: bool) {
        self.aba_outputs
            .entry(epoch_num)
            .or_default()
            .insert(batch_proposer_id, value);
    }

    /// Checks if all ABA instances for a given epoch have decided.
    /// If so, it collects the final output for that epoch (batches from
    /// proposers whose ABA decided `true` and whose RBC delivered a batch).
    /// If the epoch is finalized, it also advances `self.current_epoch` and
    /// prunes old state.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch to attempt to finalize.
    ///
    /// ## Returns
    /// * `Ok(HbbftProcessOutput)`: Contains newly decided batches if the epoch
    ///   was finalized.
    /// * `Err(HbbftError)`: This path currently does not return errors from
    ///   `try_finalize_epoch` itself but can propagate errors from sub-calls if
    ///   they were to occur.
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
                        // ABA decided true, but RBC didn't deliver a batch.
                        // This indicates an issue with that specific proposer (e.g. equivocation
                        // where RBC timed out, but ABA coin led to true).
                        // We should not include this batch and log it, but not fail the entire
                        // epoch finalization for other valid batches.
                        eprintln!(
                            "Epoch {epoch_num}: ABA for {node_id} output true, but corresponding \
                             RBC batch missing. Excluding this batch from epoch output."
                        );
                        // Continue to next proposer_id
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
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch number for which to retrieve the output.
    ///
    /// ## Returns
    /// `Some(&EpochOutput)` if the epoch has been finalized, `None` otherwise.
    pub fn get_epoch_output(&self, epoch_num: EpochNum) -> Option<&EpochOutput> {
        self.decided_epoch_outputs
            .get(&epoch_num)
    }

    /// Returns the current epoch number this node is working on or has last
    /// completed.
    ///
    /// ## Returns
    /// The current `EpochNum`.
    pub fn current_epoch(&self) -> EpochNum {
        self.current_epoch
    }

    /// Prepares and proposes a batch for the current epoch using pending
    /// transactions.
    ///
    /// This method takes any transactions currently in
    /// `self.pending_transactions`, forms a batch, and then calls
    /// `propose_batch` for `self.current_epoch`.
    ///
    /// ## Returns
    /// * `Ok(HbbftProcessOutput)`: Output from the `propose_batch` call.
    /// * `Err(HbbftError)`: If `propose_batch` returns an error.
    pub fn prepare_and_propose_current_epoch(&mut self) -> Result<HbbftProcessOutput, HbbftError> {
        let txs_to_propose = std::mem::take(&mut self.pending_transactions);
        // `propose_batch` will handle instance creation, timer requests for others,
        // and initiating its own RBC if not already done (via
        // try_broadcast_own_proposal_if_ready).
        self.propose_batch(txs_to_propose, self.current_epoch)
    }

    /// Prunes state for epochs that are older than the retention window.
    ///
    /// This method is called internally after an epoch is finalized and
    /// `current_epoch` advances. It removes entries from internal state
    /// maps (RBC/ABA instances, outputs, etc.) for epochs older than
    /// `max_finalized_epoch - epochs_to_retain_state + 1`.
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
    /// the resulting messages.
    ///
    /// ## Arguments
    /// * `epoch_num`: The epoch for which to potentially broadcast this node's
    ///   proposal.
    ///
    /// ## Returns
    /// * `Ok(HbbftProcessOutput)`: Contains RBC messages if a proposal was
    ///   broadcast.
    /// * `Err(HbbftError)`: If an error occurs during the RBC `propose` call.
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

    /// Returns a reference to the configuration of this HBBFT instance.
    pub fn config(&self) -> &HoneyBadgerConfig {
        &self.config
    }

    /// Returns the ID of this node.
    pub fn own_id(&self) -> NodeId {
        self.own_id
    }

    /// Returns the counts of entries in various internal epoch-keyed maps.
    ///
    /// This method is intended for testing and debugging purposes to inspect
    /// the size of internal state.
    ///
    /// ## Returns
    /// A tuple containing the number of epochs present in:
    /// `(rbc_instances, aba_instances, my_epoch_proposals, rbc_outputs,
    /// aba_outputs, decided_epoch_outputs)`
    #[cfg(any(test, feature = "testing"))]
    pub fn get_internal_map_epoch_counts_for_testing(
        &self,
    ) -> (usize, usize, usize, usize, usize, usize) {
        (
            self.rbc_instances.keys().len(),
            self.aba_instances.keys().len(),
            self.my_epoch_proposals.keys().len(),
            self.rbc_outputs.keys().len(),
            self.aba_outputs.keys().len(),
            self.decided_epoch_outputs.keys().len(),
        )
    }

    /// Returns the oldest epoch numbers found as keys in various internal state
    /// maps.
    ///
    /// This method is intended for testing and debugging, particularly for
    /// verifying state pruning logic.
    ///
    /// ## Returns
    /// A tuple `(Option<EpochNum>, Option<EpochNum>, Option<EpochNum>)`
    /// representing: `(oldest_rbc_epoch, oldest_aba_epoch,
    /// oldest_decided_epoch_output_epoch)`. `None` if the respective map is
    /// empty.
    #[cfg(any(test, feature = "testing"))]
    pub fn get_oldest_retained_epochs_for_testing(
        &self,
    ) -> (Option<EpochNum>, Option<EpochNum>, Option<EpochNum>) {
        // Returns (oldest_rbc_epoch, oldest_aba_epoch, oldest_decided_epoch)
        // Useful for checking which specific epochs are retained.
        let oldest_rbc = self.rbc_instances.keys().min().copied();
        let oldest_aba = self.aba_instances.keys().min().copied();
        let oldest_decided = self
            .decided_epoch_outputs
            .keys()
            .min()
            .copied();
        (oldest_rbc, oldest_aba, oldest_decided)
    }
}
