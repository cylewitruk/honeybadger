// This content is intended for src/aba.rs

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::config::HoneyBadgerConfig;
use crate::types::{
    AbaMessage, AbaMessageContent, AbaProcessOutput, AbaRound, EpochNum, HbbftError, NodeId,
    TimerRequest,
};

// Placeholder for a common coin implementation.
// In a real system, this would involve threshold signatures.
pub trait CommonCoin: std::fmt::Debug {
    fn get_coin(&self, epoch: EpochNum, round: AbaRound) -> bool; // Simplified: returns a pre-determined or random bit
}

// A dummy common coin for now.
#[derive(Debug)] // Add this
pub struct DummyCommonCoin {
    // Make it predictable for testing: round number determines coin.
}
impl CommonCoin for DummyCommonCoin {
    fn get_coin(&self, _epoch: EpochNum, round: AbaRound) -> bool {
        round % 2 == 0 // Heads for even rounds, tails for odd rounds
    }
}

// #[derive(Debug)] // Temporarily remove derive to implement manually
pub struct BinaryByzantineAgreement {
    config: Arc<HoneyBadgerConfig>,
    batch_proposer_id: NodeId, // The proposer of the batch this ABA is about
    epoch: EpochNum,
    own_id: NodeId,
    coin: Box<dyn CommonCoin>, // Using a trait object for the common coin

    current_round: AbaRound,
    current_estimate: Option<bool>, // Our estimate for the current_round, used to send EST

    // Received messages, per round:
    // Round -> (Sender -> Value)
    est_values: HashMap<AbaRound, HashMap<NodeId, bool>>,
    aux_values: HashMap<AbaRound, HashMap<NodeId, bool>>,

    // For each round, have we broadcasted EST / AUX for it?
    sent_est: HashMap<AbaRound, bool>,
    sent_aux: HashMap<AbaRound, bool>,

    decided_value: Option<bool>,
    decided_round: Option<AbaRound>,
}

impl std::fmt::Debug for BinaryByzantineAgreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryByzantineAgreement")
            .field("config", &self.config)
            .field("batch_proposer_id", &self.batch_proposer_id)
            .field("epoch", &self.epoch)
            .field("own_id", &self.own_id)
            // The crucial line: if dyn CommonCoin is truly Debug, this will work.
            // If it fails here, the problem is fundamental.
            .field("coin", &self.coin)
            .field("current_round", &self.current_round)
            .field("current_estimate", &self.current_estimate)
            .field("est_values", &self.est_values)
            .field("aux_values", &self.aux_values)
            .field("sent_est", &self.sent_est)
            .field("sent_aux", &self.sent_aux)
            .field("decided_value", &self.decided_value)
            .field("decided_round", &self.decided_round)
            .finish()
    }
}

impl BinaryByzantineAgreement {
    pub fn new(
        config: Arc<HoneyBadgerConfig>,
        batch_proposer_id: NodeId,
        epoch: EpochNum,
        own_id: NodeId,
        coin: Box<dyn CommonCoin>,
    ) -> Self {
        BinaryByzantineAgreement {
            config,
            batch_proposer_id,
            epoch,
            own_id,
            coin,
            current_round: 0,
            current_estimate: None,
            est_values: HashMap::new(),
            aux_values: HashMap::new(),
            sent_est: HashMap::new(),
            sent_aux: HashMap::new(),
            decided_value: None,
            decided_round: None,
        }
    }

    /// Starts the ABA process with an initial value `v`.
    /// This is typically called after the corresponding RBC has delivered.
    /// The `initial_value` is `true` if RBC delivered, `false` otherwise (or if timed out waiting
    /// for RBC).
    pub fn propose(&mut self, initial_value: bool) -> Result<AbaProcessOutput, HbbftError> {
        if self.current_round != 0 || self.sent_est.contains_key(&0) {
            return Ok(AbaProcessOutput::default()); // Already proposed or past round 0
        }

        self.current_estimate = Some(initial_value);
        self.sent_est.insert(0, true);

        let est_message = AbaMessage {
            pub_batch_proposer_id: self.batch_proposer_id,
            pub_epoch: self.epoch,
            pub_content: AbaMessageContent::Est { value: initial_value, round: 0 },
            pub_sender_id: self.own_id,
        };
        let timer_request = TimerRequest::SetAbaRoundTimeout {
            epoch: self.epoch,
            batch_proposer_id: self.batch_proposer_id,
            round: 0,
        };
        Ok(AbaProcessOutput {
            messages_to_send: vec![est_message],
            timer_request_to_set: Some(timer_request),
        })
    }

    /// Handles an incoming ABA message for this instance.
    pub fn handle_message(&mut self, message: &AbaMessage) -> Result<AbaProcessOutput, HbbftError> {
        if message.pub_batch_proposer_id != self.batch_proposer_id
            || message.pub_epoch != self.epoch
        {
            return Err(HbbftError::EpochMismatch { expected: self.epoch, got: message.pub_epoch });
        }
        if self.is_decided() {
            return Ok(AbaProcessOutput::default()); // Already decided
        }

        let sender_id = message.pub_sender_id;
        let mut output = AbaProcessOutput::default();

        // Basic ABA logic (greatly simplified, based on common patterns like Mosteiro-Cao):
        // This needs to be fleshed out with the specific ABA protocol steps.
        // Key idea:
        // 1. Exchange EST(v) for round r.
        // 2. If N-F EST(v) received, broadcast AUX(v). Update estimate.
        // 3. If N-F AUX(v) received, form a CONF set.
        // 4. If CONF set is singleton {v}, decide v.
        // 5. Else, use common coin and proceed to next round with new estimate based on coin.

        match &message.pub_content {
            // Added early return for old rounds if decided.
            AbaMessageContent::Est { value, round } => {
                if *round == self.current_round {
                    let round_ests = self.est_values.entry(*round).or_default();
                    round_ests.insert(sender_id, *value);
                    let sub_output = self.try_send_aux(*round)?;
                    output.messages_to_send.extend(sub_output.messages_to_send);
                    // try_send_aux does not set timers, so no need to update
                    // output.timer_request_to_set
                } else if *round > self.current_round {
                    // Queue for future round
                    let round_ests = self.est_values.entry(*round).or_default();
                    round_ests.insert(sender_id, *value);
                    // No immediate response or timer change from a future message
                }
                // Implicitly ignore messages for *round < self.current_round if not decided
            }
            AbaMessageContent::Aux { value, round } => {
                if *round == self.current_round {
                    let round_auxs = self.aux_values.entry(*round).or_default();
                    round_auxs.insert(sender_id, *value);
                    let sub_output = self.try_decide_or_advance_round(*round)?;
                    output.messages_to_send.extend(sub_output.messages_to_send);
                    // If try_decide_or_advance_round started a new round or decided,
                    // its timer_request_to_set will be authoritative.
                    if sub_output.timer_request_to_set.is_some() || self.is_decided() {
                        output.timer_request_to_set = sub_output.timer_request_to_set;
                    }
                } else if *round > self.current_round {
                    // Queue for future round
                    let round_auxs = self.aux_values.entry(*round).or_default();
                    round_auxs.insert(sender_id, *value);
                    // No immediate response or timer change from a future message
                }
            }
            AbaMessageContent::Coin { .. } => { // Removed Conf handling
                // Handle coin shares if using a threshold coin.
                // For DummyCommonCoin, this is not directly used in message handling for now.
            }
        }

        Ok(output)
    }

    /// Checks if enough EST messages for a value `v` in `round` have been received
    /// to broadcast an AUX(v) message for that round.
    /// Returns messages to send (AUX) and potentially timer requests if a new round is started.
    fn try_send_aux(&mut self, round: AbaRound) -> Result<AbaProcessOutput, HbbftError> {
        let mut output = AbaProcessOutput::default();
        if self.sent_aux.get(&round).copied().unwrap_or(false) || self.is_decided() {
            return Ok(output);
        }

        if let Some(ests_for_round) = self.est_values.get(&round) {
            let mut counts = HashMap::new(); // value -> count
            for val in ests_for_round.values() {
                *counts.entry(*val).or_insert(0) += 1;
            }

            for (val, count) in counts {
                if count >= self.config.low_threshold() {
                    // F+1 ESTs for value `val`
                    // We don't change self.current_estimate here.
                    // self.current_estimate is for *our* EST proposal for the current round.
                    // The value `val` is what we are sending in AUX.
                    self.sent_aux.insert(round, true);
                    output.messages_to_send.push(AbaMessage {
                        pub_batch_proposer_id: self.batch_proposer_id,
                        pub_epoch: self.epoch,
                        pub_content: AbaMessageContent::Aux { value: val, round },
                        pub_sender_id: self.own_id,
                    });
                    // In many ABAs, you only send one AUX per round.
                    // A correct node sends AUX for the first such value it observes.
                    // Our loop structure and `break` achieve this.
                    break;
                }
            }
        }
        Ok(output)
    }

    /// Checks if enough AUX messages for `round` have been received to form a CONF set.
    /// If CONF set is singleton {v}, decide v.
    /// Else if CONF set is non-empty (or other conditions met), consult coin and advance.
    fn try_decide_or_advance_round(
        &mut self,
        round: AbaRound,
    ) -> Result<AbaProcessOutput, HbbftError> {
        if self.is_decided() {
            return Ok(AbaProcessOutput::default());
        }

        if let Some(auxs_for_round) = self.aux_values.get(&round) {
            let mut aux_counts = HashMap::new(); // value -> count
            for val in auxs_for_round.values() {
                *aux_counts.entry(*val).or_insert(0) += 1;
            }

            let mut current_conf_values = BTreeSet::new();
            for (val, count) in &aux_counts {
                if *count >= self.config.high_threshold() {
                    // N-F AUX messages for value `val`
                    current_conf_values.insert(*val);
                }
            }

            if !current_conf_values.is_empty() {
                // If |conf_values| == 1, say {v}, then decide v.
                if current_conf_values.len() == 1 {
                    let decided_val = *current_conf_values.iter().next().unwrap();
                    self.decided_value = Some(decided_val);
                    self.decided_round = Some(round);
                    // DbgPrint!("[ABA ({},{},R{})] DECIDED on {}", self.batch_proposer_id,
                    // self.epoch, round, decided_val); Optionally, could
                    // broadcast a DECIDE message or rely on others reaching same state.
                    return Ok(AbaProcessOutput::default()); // Decided, no new messages or timer.
                } else {
                    // |conf_values| > 1 (e.g. {0,1}), since it's not empty and not 1.
                    // A common condition is: if any value v has F+1 AUX messages,
                    // and we haven't decided, we should eventually consult the coin for this round.
                    // For simplicity here, if N-F AUX for multiple values, use coin.
                    let coin_val = self.coin.get_coin(self.epoch, round);
                    // DbgPrint!("[ABA ({},{},R{})] CONF set {:?}, coin for round {} is {}. Starting next round.", self.batch_proposer_id, self.epoch, round, current_conf_values, round, coin_val);
                    return self.start_next_round(Some(coin_val)); // Return directly as start_next_round creates full output
                }
            } else {
                // No value has N-F AUX messages yet. Wait for more, or if round seems stuck,
                // A full ABA implementation would have a timeout here. If the timeout expires
                // and we haven't decided, we'd consult the coin for `round` and move to `round +
                // 1`. This is step 1.3 in your ROADMAP.md.
            }
        }
        Ok(AbaProcessOutput::default())
    }

    /// Handles a timeout for a specific ABA round.
    /// If the timeout is for the current, undecided round, it consults the coin and starts the next
    /// round.
    pub fn handle_round_timeout(
        &mut self,
        round_that_timed_out: AbaRound,
    ) -> Result<AbaProcessOutput, HbbftError> {
        if self.is_decided() || round_that_timed_out != self.current_round {
            // Already decided, or timeout is for a past/future round we are not currently in.
            return Ok(AbaProcessOutput::default());
        }

        // DbgPrint!("[ABA ({},{},R{})] Round {} TIMED OUT. Consulting coin.",
        // self.batch_proposer_id, self.epoch, self.current_round, round_that_timed_out);
        let coin_val = self.coin.get_coin(self.epoch, self.current_round);
        self.start_next_round(Some(coin_val))
    }

    fn start_next_round(
        &mut self,
        proposed_value_for_next_round: Option<bool>,
    ) -> Result<AbaProcessOutput, HbbftError> {
        if self.is_decided() {
            return Ok(AbaProcessOutput::default());
        }

        self.current_round += 1;

        // The estimate for the new round comes from the coin flip of the *previous* round (round r
        // determines estimate for r+1). The `proposed_value_for_next_round` argument should
        // be the result of coin.get_coin(epoch, self.current_round - 1).
        let val_to_propose = proposed_value_for_next_round.ok_or_else(|| {
            HbbftError::InternalError(
                "Cannot start next round without proposed value from coin.".to_string(),
            )
        })?;

        // DbgPrint!("[ABA ({},{},R{})] Starting new round. Estimate from coin (for prev round {}):
        // {}", self.batch_proposer_id, self.epoch, self.current_round, self.current_round -1,
        // val_to_propose);

        self.current_estimate = Some(val_to_propose); // Set estimate for new round
        self.sent_est.insert(self.current_round, true);
        // DbgPrint!("[ABA ({},{},R{})] Starting new round {}. Estimate from coin (for prev round):
        // {}",     self.batch_proposer_id, self.epoch, self.current_round, val_to_propose);

        let est_message = AbaMessage {
            pub_batch_proposer_id: self.batch_proposer_id,
            pub_epoch: self.epoch,
            pub_content: AbaMessageContent::Est {
                value: val_to_propose,
                round: self.current_round,
            },
            pub_sender_id: self.own_id,
        };
        let initial_timer_request = TimerRequest::SetAbaRoundTimeout {
            epoch: self.epoch,
            batch_proposer_id: self.batch_proposer_id,
            round: self.current_round,
        };

        let mut messages_to_send = vec![est_message];
        let mut current_timer_request = Some(initial_timer_request);

        // Process any messages that might have been queued for this new self.current_round
        // This might lead to sending AUX, deciding, or even starting another round.
        let processing_queued_output = self.process_potential_progress_for_current_round()?;
        messages_to_send.extend(processing_queued_output.messages_to_send);

        if self.is_decided() {
            // If processing queued led to a decision
            current_timer_request = None;
        } else if processing_queued_output.timer_request_to_set.is_some() {
            // If processing queued led to starting an even newer round (e.g. round + 1), use its
            // timer.
            current_timer_request = processing_queued_output.timer_request_to_set;
        }
        // Otherwise, the initial_timer_request for self.current_round remains.

        Ok(AbaProcessOutput { messages_to_send, timer_request_to_set: current_timer_request })
    }

    /// Helper to process ESTs and AUXs that might have been queued for the current round.
    /// This is called when a round starts.
    fn process_potential_progress_for_current_round(
        &mut self,
    ) -> Result<AbaProcessOutput, HbbftError> {
        let mut combined_output = AbaProcessOutput::default();

        if self.is_decided() {
            return Ok(combined_output);
        }

        // Try to send AUX based on current ESTs (which might include previously queued ones for
        // this round)
        let aux_output = self.try_send_aux(self.current_round)?;
        combined_output
            .messages_to_send
            .extend(aux_output.messages_to_send);
        // try_send_aux doesn't set timers or decide on its own.

        // Try to decide or advance round based on current AUXs (which might include previously
        // queued ones for this round)
        let decide_output = self.try_decide_or_advance_round(self.current_round)?;
        combined_output
            .messages_to_send
            .extend(decide_output.messages_to_send);
        // This one can set a timer if it starts a new round, or clear it (None) if it decides.
        combined_output.timer_request_to_set = decide_output.timer_request_to_set;

        Ok(combined_output)
    }

    /// Returns the decided value if this ABA instance has terminated.
    pub fn output(&self) -> Option<bool> {
        self.decided_value
    }

    pub fn is_decided(&self) -> bool {
        self.decided_value.is_some()
    }

    /// Returns the current round number of this ABA instance (for debugging/checks).
    pub fn current_round_debug(&self) -> AbaRound {
        self.current_round
    }
}
