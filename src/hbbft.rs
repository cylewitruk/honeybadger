// This content is intended for src/hbbft.rs

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::aba::{BinaryByzantineAgreement, DummyCommonCoin}; // Using DummyCommonCoin for now
use crate::config::HoneyBadgerConfig;
use crate::rbc::ReliableBroadcast;
use crate::types::{
    AbaProcessOutput, Batch, DecidedBatch, EpochNum, EpochOutput, HbbftError, HbbftProcessOutput,
    HoneyBadgerMessage, NodeId, Transaction,
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

    // Store proposed batches for the current epoch, if we are the proposer for an RBC.
    // epoch -> (our proposed batch for our RBC in this epoch)
    my_epoch_proposals: HashMap<EpochNum, Batch>,

    // Store outputs of RBC instances that have decided.
    // epoch -> (proposer_id -> Batch)
    rbc_outputs: HashMap<EpochNum, HashMap<NodeId, Batch>>,

    // Store outputs of ABA instances that have decided.
    // epoch -> (batch_proposer_id -> bool)
    aba_outputs: HashMap<EpochNum, HashMap<NodeId, bool>>,

    // Final decided batches for each epoch, ready for the application.
    // epoch -> Set of (ProposerId, Batch)
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
    pub fn submit_transaction(&mut self, tx: Transaction) {
        self.pending_transactions.push(tx);
    }

    /// Initiates the HBBFT protocol for a given epoch with a proposed batch.
    /// This node will propose its `own_batch` via its RBC instance for this epoch.
    /// It also prepares to participate in RBC/ABA for all other nodes.
    /// Returns messages to be broadcast (typically the first RBC message from this node).
    pub fn propose_batch(
        &mut self,
        own_batch_txs: Vec<Transaction>, // Transactions this node wants to propose for this epoch
        epoch_num: EpochNum,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        // Changed return type
        if epoch_num < self.current_epoch {
            return Err(HbbftError::EpochMismatch { expected: self.current_epoch, got: epoch_num });
        }
        if self.my_epoch_proposals.contains_key(&epoch_num) {
            // Already proposed for this epoch or handled proposal initiation
            return Ok(HbbftProcessOutput::default());
        }

        // Create the batch
        let batch_to_propose = Batch::new(own_batch_txs); // Batch::new sorts transactions internally

        self.my_epoch_proposals
            .insert(epoch_num, batch_to_propose.clone());

        // Initialize RBC and ABA instances for this epoch if not already present
        self.ensure_epoch_instances(epoch_num);

        let mut hbbft_output = HbbftProcessOutput::default();

        // Initiate our own RBC instance for this epoch
        if let Some(proposer_rbc_map) = self.rbc_instances.get_mut(&epoch_num) {
            if let Some(our_rbc_instance) = proposer_rbc_map.get_mut(&self.own_id) {
                if !our_rbc_instance.is_decided() {
                    // Should not be decided yet if we are just proposing
                    match our_rbc_instance.propose(batch_to_propose.clone()) {
                        // Pass batch by value or clone
                        Ok(rbc_messages) => {
                            // rbc_messages is Vec<RbcMessage>
                            for rbc_msg in rbc_messages {
                                hbbft_output
                                    .messages_to_send
                                    .push(HoneyBadgerMessage::Rbc(rbc_msg));
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(hbbft_output)
    }

    /// Handles an incoming HoneyBadgerMessage.
    /// Returns a list of messages to be broadcast in response, and any newly decided batches.
    /// Also returns any timer requests that need to be set by the application.
    pub fn handle_message(
        // Changed return type
        &mut self,
        message: HoneyBadgerMessage,
    ) -> Result<HbbftProcessOutput, HbbftError> {
        let message_epoch = message.epoch();
        if message_epoch < self.current_epoch
            && self.decided_epoch_outputs.contains_key(&message_epoch)
        {
            // Message for an old, already decided epoch. Can be ignored or handled for liveness for
            // others. For now, ignoring. A robust version might re-send relevant old
            // messages.
            return Ok(HbbftProcessOutput::default());
        }

        // Initialize instances for the message's epoch if it's new to us and not too old.
        self.ensure_epoch_instances(message_epoch);

        let mut hbbft_output = HbbftProcessOutput::default();

        match message {
            HoneyBadgerMessage::Rbc(rbc_msg) => {
                let proposer_id = rbc_msg.pub_proposer_id;
                if let Some(rbc_epoch_map) = self.rbc_instances.get_mut(&message_epoch) {
                    if let Some(rbc_instance) = rbc_epoch_map.get_mut(&proposer_id) {
                        let responses = rbc_instance.handle_message(&rbc_msg)?;
                        for resp in responses {
                            hbbft_output
                                .messages_to_send
                                .push(HoneyBadgerMessage::Rbc(resp));
                        }

                        // Directly check self.rbc_outputs instead of calling
                        // self.rbc_output_exists()
                        let output_already_recorded = self
                            .rbc_outputs
                            .get(&message_epoch)
                            .map_or(false, |epoch_map| epoch_map.contains_key(&proposer_id));

                        // Check if this RBC instance just decided and we haven't recorded its
                        // output yet
                        if !output_already_recorded && rbc_instance.is_decided() {
                            if let Some(decided_batch) = rbc_instance.output() {
                                self.store_rbc_output(message_epoch, proposer_id, decided_batch);
                                // RBC decided, now propose 'true' to corresponding ABA_proposer_id
                                let aba_init_output: AbaProcessOutput =
                                    self.initiate_aba_for_rbc(message_epoch, proposer_id, true)?;
                                hbbft_output.messages_to_send.extend(
                                    aba_init_output
                                        .messages_to_send
                                        .into_iter()
                                        .map(HoneyBadgerMessage::Aba),
                                );
                                if let Some(timer_req) = aba_init_output.timer_request_to_set {
                                    hbbft_output.timer_requests_to_set.push(timer_req);
                                }
                            } else {
                                // This would mean rbc.is_decided() is true, but output() is None.
                                // Potentially implies it decided on a hash but doesn't have the
                                // batch. This needs robust handling
                                // (e.g. state sync or error).
                                // For now, we'd not proceed to ABA for this.
                                // Consider this an error or a state where we wait for batch data.
                            }
                        }
                    }
                }
            }
            HoneyBadgerMessage::Aba(aba_msg) => {
                let batch_proposer_id = aba_msg.pub_batch_proposer_id;
                if let Some(aba_epoch_map) = self.aba_instances.get_mut(&message_epoch) {
                    if let Some(aba_instance) = aba_epoch_map.get_mut(&batch_proposer_id) {
                        if !aba_instance.is_decided() {
                            // Only process if not already decided
                            let aba_proc_output = aba_instance.handle_message(&aba_msg)?;
                            for resp in aba_proc_output.messages_to_send {
                                hbbft_output
                                    .messages_to_send
                                    .push(HoneyBadgerMessage::Aba(resp));
                            }
                            if let Some(timer_req) = aba_proc_output.timer_request_to_set {
                                hbbft_output.timer_requests_to_set.push(timer_req);
                            }
                            // Check if this ABA instance just decided
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

        // After any message handling, check if an epoch can be finalized
        if message_epoch >= self.current_epoch
            && !self.decided_epoch_outputs.contains_key(&message_epoch)
        {
            if let Some(final_output) = self.try_finalize_epoch(message_epoch)? {
                hbbft_output.newly_decided_batches.extend(final_output);
            }
        }

        Ok(hbbft_output)
    }

    /// Creates RBC and ABA instances for all N nodes for a given epoch if they don't exist.
    fn ensure_epoch_instances(&mut self, epoch_num: EpochNum) {
        if epoch_num < self.current_epoch && self.decided_epoch_outputs.contains_key(&epoch_num) {
            return; // Don't create for already decided old epochs
        }

        let own_id = self.own_id;
        let shared_config = Arc::new(self.config.clone()); // Create an Arc for the config

        // Ensure RBC instances
        self.rbc_instances.entry(epoch_num).or_insert_with(|| {
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

        // Ensure ABA instances
        self.aba_instances.entry(epoch_num).or_insert_with(|| {
            shared_config
                .all_node_ids
                .iter()
                .map(|&node_id| {
                    // Each ABA instance gets its own common coin; for DummyCommonCoin it's cheap.
                    // For a real threshold coin, the setup might be more involved (sharing key).
                    let coin = Box::new(DummyCommonCoin {});
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

        // If this is for an epoch where our RBC hasn't delivered a value yet,
        // and all other RBCs have been processed (or timed out),
        // we might need to input `false` to our own ABA.
        // HBBFT is designed such that each node `j` eventually provides a value `v_j` to `ACS_j`.
        // The `RBC_j` ensures `v_j` is decided.
        // If an RBC_k (for k != self.own_id) doesn't deliver, eventually we need to input false
        // into ABA_k. This "timeout" or lack-of-RBC-output condition needs to be handled.
        // For now, initiate_aba_for_rbc is called when RBC_output is available.
        // We may need a mechanism to eventually propose 'false' to ABAs for RBCs that don't
        // deliver.
    }

    fn rbc_output_exists(&self, epoch_num: EpochNum, proposer_id: NodeId) -> bool {
        self.rbc_outputs
            .get(&epoch_num)
            .map_or(false, |epoch_map| epoch_map.contains_key(&proposer_id))
    }

    fn store_rbc_output(&mut self, epoch_num: EpochNum, proposer_id: NodeId, batch: Batch) {
        self.rbc_outputs
            .entry(epoch_num)
            .or_default()
            .insert(proposer_id, batch);
        // DbgPrint!("[HBBFT Epoch {}] RBC output stored for proposer {}", epoch_num, proposer_id);
    }

    fn initiate_aba_for_rbc(
        &mut self,
        epoch_num: EpochNum,
        batch_proposer_id: NodeId,
        rbc_delivered_value: bool,
    ) -> Result<AbaProcessOutput, HbbftError> {
        if let Some(aba_map) = self.aba_instances.get_mut(&epoch_num) {
            if let Some(aba_instance) = aba_map.get_mut(&batch_proposer_id) {
                // Only propose if ABA hasn't already started processing significantly or decided
                if !aba_instance.is_decided() && aba_instance.output().is_none() {
                    // Basic check
                    // The propose method itself now returns AbaProcessOutput
                    return aba_instance.propose(rbc_delivered_value);
                }
            }
        }
        // If ABA instance not found or already started/decided, return empty output
        Ok(AbaProcessOutput::default())
    }

    fn store_aba_output(&mut self, epoch_num: EpochNum, batch_proposer_id: NodeId, value: bool) {
        self.aba_outputs
            .entry(epoch_num)
            .or_default()
            .insert(batch_proposer_id, value);
        // DbgPrint!("[HBBFT Epoch {}] ABA output for batch_proposer {} stored: {}", epoch_num,
        // batch_proposer_id, value);
    }

    /// Checks if all ABA instances for a given epoch have decided.
    /// If so, collects the final output for that epoch.
    fn try_finalize_epoch(
        &mut self,
        epoch_num: EpochNum,
    ) -> Result<Option<Vec<DecidedBatch>>, HbbftError> {
        if self.decided_epoch_outputs.contains_key(&epoch_num) {
            return Ok(None); // Already finalized
        }

        let num_nodes = self.config.num_total_nodes;
        let mut all_abas_decided = false;
        if let Some(epoch_aba_outputs) = self.aba_outputs.get(&epoch_num) {
            if epoch_aba_outputs.len() == num_nodes {
                all_abas_decided = true;
            }
        }

        // Additional condition: For every ABA_j that decided true, we must have the output from
        // RBC_j. HBBFT ensures this because ABA_j only starts with true if RBC_j delivered.
        // If an ABA decides 'false', we don't need the batch from that proposer.
        if all_abas_decided {
            let mut final_epoch_output = EpochOutput::new();
            let mut app_decisions = Vec::new();

            let epoch_rbc_out = self.rbc_outputs.entry(epoch_num).or_default();
            let epoch_aba_out = self.aba_outputs.get(&epoch_num).unwrap(); // Safe due to check above

            for node_id in self.config.all_node_ids.iter() {
                if let Some(true) = epoch_aba_out.get(node_id) {
                    if let Some(batch) = epoch_rbc_out.get(node_id) {
                        final_epoch_output.insert(*node_id, batch.clone());
                        app_decisions.push(DecidedBatch {
                            batch: batch.clone(),
                            proposer_id: *node_id,
                            epoch: epoch_num,
                        });
                    } else {
                        // This indicates a problem: ABA_node_id decided true, but we don't have
                        // RBC_node_id's output. This state should ideally
                        // not be reached if ABA is initiated correctly after RBC.
                        // For a truly robust system, this might require a state sync or error
                        // handling. DbgPrint!("[HBBFT Epoch {}] Error: ABA
                        // for {} output true, but no RBC batch found!", epoch_num, node_id);
                        return Err(HbbftError::InternalError(format!(
                            "Epoch {}: ABA for {} output true, but corresponding RBC batch \
                             missing.",
                            epoch_num, node_id
                        )));
                    }
                }
            }
            self.decided_epoch_outputs
                .insert(epoch_num, final_epoch_output);
            // DbgPrint!("[HBBFT Epoch {}] FINALIZED with {} batches.", epoch_num,
            // app_decisions.len());

            // Advance to the next epoch if this was the current one being processed.
            if epoch_num == self.current_epoch {
                self.current_epoch += 1;
                // DbgPrint!("[HBBFT] Advanced to epoch {}", self.current_epoch);
                // TODO: Here, you might trigger `propose_batch` for self.current_epoch if there are
                // pending transactions.
            }
            return Ok(Some(app_decisions));
        }
        Ok(None)
    }

    /// Retrieves all finalized batches for a given epoch.
    pub fn get_epoch_output(&self, epoch_num: EpochNum) -> Option<&EpochOutput> {
        self.decided_epoch_outputs.get(&epoch_num)
    }

    /// Returns the current epoch number this node is working on or has last completed.
    pub fn current_epoch(&self) -> EpochNum {
        self.current_epoch
    }

    /// Attempts to take pending transactions and form a batch for the current epoch.
    /// This should be called by the application layer when it's ready to start a new epoch
    /// proposal.
    pub fn prepare_and_propose_current_epoch(&mut self) -> Result<HbbftProcessOutput, HbbftError> {
        // Changed return type
        let txs_to_propose = std::mem::take(&mut self.pending_transactions);
        if txs_to_propose.is_empty() {
            // Propose an empty batch if no transactions are available,
            // HBBFT requires all nodes to propose something for ACS.
            // DbgPrint!("[HBBFT Epoch {}] No transactions, proposing empty batch for self (node
            // {})", self.current_epoch, self.own_id);
        } else {
            // DbgPrint!("[HBBFT Epoch {}] Proposing batch with {} transactions for self (node {})",
            // self.current_epoch, txs_to_propose.len(), self.own_id);
        }
        self.propose_batch(txs_to_propose, self.current_epoch)
    }
}
