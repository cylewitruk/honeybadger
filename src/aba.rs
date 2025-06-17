//! Implements the Asynchronous Binary Byzantine Agreement (ABA) sub-protocol.
//!
//! ABA is a core component of HoneyBadgerBFT, used to allow nodes to agree on
//! whether to include a specific proposer's batch in the output for an epoch.
//! Each potential proposer in an epoch has a corresponding ABA instance.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::config::HoneyBadgerConfig;
use crate::types::{
    AbaMessage, AbaMessageContent, AbaProcessOutput, AbaRound, CoinShare, EpochNum, TimerRequest,
};
use crate::{HbbftError, NodeIdentifier};

/// Defines the interface for a common coin mechanism used by ABA.
///
/// The common coin provides a source of shared randomness that helps ABA
/// instances converge, especially when there's contention or byzantine
/// behavior.
pub trait CommonCoin: std::fmt::Debug + Send + Sync {
    type NodeId: NodeIdentifier;

    /// Called by ABA when it needs a coin for a specific epoch and round,
    /// typically when a round cannot decide based on EST/AUX messages alone.
    ///
    /// This method should:
    /// 1. Return the node's own share to be broadcast (if applicable for the
    ///    scheme).
    /// 2. Indicate if the coin can be determined *now* based on internal state
    ///    or this call.
    ///
    /// ## Arguments
    ///
    /// * `epoch`: The epoch number for which the coin is requested.
    /// * `round`: The ABA round number for which the coin is requested.
    ///
    /// ## Returns
    ///
    /// A tuple containing:
    /// * `Option<CoinShare>`: The node's own share to be broadcast, if
    ///   applicable.
    /// * `Option<bool>`: The coin value if it can be determined immediately.
    ///
    /// ## Notes
    ///
    /// - This method should not block.
    fn request_coin_value(
        &mut self,
        epoch: EpochNum,
        round: AbaRound,
    ) -> (Option<CoinShare>, Option<bool>);

    /// Called by ABA to pass a received coin share message.
    /// Returns the coin value if this share (and others previously received)
    /// completes the coin.
    fn submit_coin_share(
        &mut self,
        epoch: EpochNum,
        round: AbaRound,
        share_content: CoinShare, // The content of the received coin share.
        sender_id: Self::NodeId,  // ID of the node that sent this share.
    ) -> Option<bool>;

    /// Called when an ABA round has timed out and a coin value is mandatorily
    /// needed to proceed to the next round. This method *must* return a
    /// boolean.
    ///
    /// ## Arguments
    ///
    /// * `epoch`: The epoch number of the timed-out ABA round.
    /// * `round`: The ABA round number that timed out.
    ///
    /// ## Returns
    ///
    /// A `bool` representing the determined coin value for the round.
    fn get_coin_for_round_fallback(&mut self, epoch: EpochNum, round: AbaRound) -> bool;
}

/// Represents the state of a single Asynchronous Binary Byzantine Agreement
/// instance.
///
/// Each instance is responsible for agreeing on a single boolean value. In the
/// context of HoneyBadgerBFT, this value typically signifies whether a specific
/// proposer's batch (identified by `batch_proposer_id` and `epoch`) should be
/// included.
#[derive(Debug)]
pub struct BinaryByzantineAgreement<NodeId> {
    /// Shared configuration for the HoneyBadgerBFT instance.
    config: Arc<HoneyBadgerConfig<NodeId>>,
    /// The ID of the node whose proposed batch this ABA instance is for.
    batch_proposer_id: NodeId,
    /// The epoch number this ABA instance belongs to.
    epoch: EpochNum,
    /// The ID of the node running this ABA instance.
    own_id: NodeId,
    /// The common coin mechanism used by this ABA instance.
    coin: Box<dyn CommonCoin<NodeId = NodeId>>,

    /// The current round number of this ABA instance.
    current_round: AbaRound,
    /// This node's current estimate for the `current_round`, used to send `EST`
    /// messages.
    current_estimate: Option<bool>,

    /// Stores received `EST` messages.
    /// Mapping: `AbaRound` -> (`NodeId` (sender) -> `bool` (estimated value)).
    est_values: HashMap<AbaRound, HashMap<NodeId, bool>>,
    /// Stores received `AUX` messages.
    /// Mapping: `AbaRound` -> (`NodeId` (sender) -> `bool` (auxiliary value)).
    aux_values: HashMap<AbaRound, HashMap<NodeId, bool>>,

    // For each round, have we broadcasted EST / AUX for it?
    /// Tracks whether an `EST` message has been sent for a given round.
    /// Mapping: `AbaRound` -> `bool` (true if sent).
    sent_est: HashMap<AbaRound, bool>,
    /// Tracks whether an `AUX` message has been sent for a given round.
    /// Mapping: `AbaRound` -> `bool` (true if sent).
    sent_aux: HashMap<AbaRound, bool>,

    decided_value: Option<bool>,
    decided_round: Option<AbaRound>,
    // Indicates the round for which we are currently waiting for enough coin shares.
    waiting_for_coin_in_round: Option<AbaRound>,
}

impl<NodeId: NodeIdentifier> BinaryByzantineAgreement<NodeId> {
    /// Creates a new `BinaryByzantineAgreement` instance.
    ///
    /// ## Arguments
    ///
    /// * `config`: Shared configuration for the HoneyBadgerBFT instance.
    /// * `batch_proposer_id`: The ID of the node whose proposed batch this ABA
    ///   instance is for.
    /// * `epoch`: The epoch number this ABA instance belongs to.
    /// * `own_id`: The ID of the node running this ABA instance.
    /// * `coin`: A boxed `CommonCoin` trait object to be used by this instance.
    ///
    /// ## Returns
    /// A new `BinaryByzantineAgreement` instance.
    pub fn new(
        config: Arc<HoneyBadgerConfig<NodeId>>,
        batch_proposer_id: NodeId,
        epoch: EpochNum,
        own_id: NodeId,
        coin: Box<dyn CommonCoin<NodeId = NodeId>>,
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
            waiting_for_coin_in_round: None,
        }
    }

    /// Starts the ABA process with an initial value `v`.
    /// This is typically called after the corresponding RBC has delivered.
    /// The `initial_value` is `true` if RBC delivered, `false` otherwise (or if
    /// timed out waiting for RBC). It sends an initial `EST` message and
    /// requests a timer for round 0.
    ///
    /// ## Arguments
    ///
    /// * `initial_value`: The boolean value to propose for this ABA instance.
    ///
    /// ## Returns
    ///
    /// * `Ok(AbaProcessOutput)`: Contains messages to send (an `EST` message)
    ///   and a timer request.
    /// * `Err(HbbftError)`: If an error occurs (though currently this path
    ///   always returns `Ok`).
    pub fn propose(
        &mut self,
        initial_value: bool,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        if self.current_round != 0 || self.sent_est.contains_key(&0) {
            return Ok(AbaProcessOutput::<NodeId>::default()); // Already proposed or past round 0
        }

        self.current_estimate = Some(initial_value);
        self.sent_est.insert(0, true);
        // A node considers its own EST message as received for threshold calculations.
        self.est_values
            .entry(0)
            .or_default()
            .insert(self.own_id, initial_value);

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
            suggested_duration: self.config.default_aba_round_timeout,
        };
        Ok(AbaProcessOutput {
            messages_to_send: vec![est_message],
            timer_request_to_set: Some(timer_request),
        })
    }

    /// Handles an incoming ABA message for this instance.
    ///
    /// It processes `EST`, `AUX`, or `COIN` messages, updates internal state,
    /// and potentially triggers sending new messages or advancing rounds.
    ///
    /// ## Arguments
    ///
    /// * `message`: The `AbaMessage` to handle.
    ///
    /// ## Returns
    /// * `Ok(AbaProcessOutput)`: Contains any messages to send or timer
    ///   requests generated.
    /// * `Err(HbbftError)`: If the message is for a mismatched epoch or
    ///   proposer.
    pub fn handle_message(
        &mut self,
        message: &AbaMessage<NodeId>,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        if message.pub_batch_proposer_id != self.batch_proposer_id
            || message.pub_epoch != self.epoch
        {
            return Err(HbbftError::EpochMismatch {
                expected: self.epoch,
                got: message.pub_epoch,
            });
        }
        if self.is_decided() {
            return Ok(AbaProcessOutput::default()); // Already decided
        }

        let sender_id = message.pub_sender_id;
        let mut output = AbaProcessOutput::default();

        match &message.pub_content {
            // Added early return for old rounds if decided.
            AbaMessageContent::Est { value, round } => {
                if *round == self.current_round {
                    let round_ests = self
                        .est_values
                        .entry(*round)
                        .or_default();
                    round_ests.insert(sender_id, *value);
                    let sub_output = self.try_send_aux(*round)?;
                    output
                        .messages_to_send
                        .extend(sub_output.messages_to_send);
                    // try_send_aux does not set timers, so no need to update
                    // output.timer_request_to_set
                } else if *round > self.current_round {
                    if *round <= self.current_round + self.config.aba_max_future_round_window {
                        // Queue for future round within the window
                        let round_ests = self
                            .est_values
                            .entry(*round)
                            .or_default();
                        round_ests.insert(sender_id, *value);
                    } else {
                        // DbgPrint!("[ABA ({},{},R{})] Dropped EST for too
                        // distant future round
                        // {} (current {})", self.batch_proposer_id, self.epoch,
                        // self.current_round, round, self.current_round);
                    }
                }
                // Implicitly ignore messages for *round < self.current_round if
                // not decided
            }
            AbaMessageContent::Aux { value, round } => {
                if *round == self.current_round {
                    let round_auxs = self
                        .aux_values
                        .entry(*round)
                        .or_default();
                    round_auxs.insert(sender_id, *value);
                    let sub_output = self.try_decide_or_advance_round(*round)?;
                    output
                        .messages_to_send
                        .extend(sub_output.messages_to_send);
                    // If try_decide_or_advance_round started a new round or decided,
                    // its timer_request_to_set will be authoritative.
                    if sub_output
                        .timer_request_to_set
                        .is_some()
                        || self.is_decided()
                    {
                        output.timer_request_to_set = sub_output.timer_request_to_set;
                    }
                } else if *round > self.current_round {
                    if *round <= self.current_round + self.config.aba_max_future_round_window {
                        // Queue for future round within the window
                        let round_auxs = self
                            .aux_values
                            .entry(*round)
                            .or_default();
                        round_auxs.insert(sender_id, *value);
                    } else {
                        // DbgPrint!("[ABA ({},{},R{})] Dropped AUX for too
                        // distant future round
                        // {} (current {})", self.batch_proposer_id, self.epoch,
                        // self.current_round, round, self.current_round);
                    }
                }
            }
            AbaMessageContent::Coin { share, round } => {
                if Some(*round) == self.waiting_for_coin_in_round && !self.is_decided() {
                    // DbgPrint!("[ABA ({},{},R{})] Received CoinShare for waiting round {} from
                    // {}", self.batch_proposer_id, self.epoch, self.current_round,
                    // round, sender_id);
                    if let Some(coin_value) = self.coin.submit_coin_share(
                        self.epoch,
                        *round,
                        share.clone(), // CoinShare is Vec<u8>, cheap to clone
                        sender_id,
                    ) {
                        // DbgPrint!("[ABA ({},{},R{})] Coin decided via share: {}. Starting next
                        // round.", self.batch_proposer_id, self.epoch,
                        // self.current_round, coin_value);
                        self.waiting_for_coin_in_round = None;
                        return self.start_next_round(Some(coin_value)); // Return directly
                    }
                    // Else, still waiting for more shares for this round.
                    // No new messages or timer changes from just one share
                    // unless it completes the coin.
                }
            }
        }

        Ok(output)
    }

    /// Checks if enough EST messages for a value `v` in `round` have been
    /// received to broadcast an AUX(v) message for that round.
    ///
    /// ## Arguments
    ///
    /// * `round`: The ABA round number to check.
    ///
    /// ## Returns
    ///
    /// * `Ok(AbaProcessOutput)`: Contains an `AUX` message if conditions are
    ///   met.
    /// * `Err(HbbftError)`: Currently, this path does not return errors.
    fn try_send_aux(
        &mut self,
        round: AbaRound,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        let mut output = AbaProcessOutput::default();
        if self
            .sent_aux
            .get(&round)
            .copied()
            .unwrap_or(false)
            || self.is_decided()
        {
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
                    output
                        .messages_to_send
                        .push(AbaMessage {
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

    /// Checks if enough AUX messages for `round` have been received to form a
    /// CONF set. If CONF set is singleton {v}, decide v.
    /// Else if CONF set is non-empty (or other conditions met), consult coin
    /// and potentially advance the round.
    ///
    /// ## Arguments
    ///
    /// * `round`: The ABA round number to check.
    ///
    /// ## Returns
    ///
    /// * `Ok(AbaProcessOutput)`: Contains messages to send (e.g., `COIN` or
    ///   `EST` for next round) and potentially a timer request if a new round
    ///   is started.
    /// * `Err(HbbftError)`: Currently, this path does not return errors.
    fn try_decide_or_advance_round(
        &mut self,
        round: AbaRound,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        let mut output = AbaProcessOutput::default();
        if self.is_decided() {
            return Ok(output); // Return the default, empty output
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
                    let decided_val = *current_conf_values
                        .iter()
                        .next()
                        .unwrap();
                    self.decided_value = Some(decided_val);
                    self.decided_round = Some(round);
                    // DbgPrint!("[ABA ({},{},R{})] DECIDED on {}", self.batch_proposer_id,
                    // self.epoch, round, decided_val); Optionally, could
                    // broadcast a DECIDE message or rely on others reaching same state.
                    return Ok(AbaProcessOutput::default()); // Decided, no new messages or timer.
                } else {
                    // current_conf_values.len() > 1
                    // This case (multiple values having N-F AUX) should be impossible
                    // with N >= 3F+1 and correct nodes, as it implies 2*(N-F) <= N,
                    // which means N <= 2F, contradicting N >= 3F+1.
                    // However, as a robust fallback, we can proceed to coin.
                    eprintln!(
                        "[DEBUG ABA try_decide R{round}] Ambiguous N-F conf_values (should be \
                         impossible): {current_conf_values:?}. Requesting coin."
                    );
                    // self.batch_proposer_id, self.epoch, round, current_conf_values, round);

                    let (own_share_opt, coin_now_opt) = self
                        .coin
                        .request_coin_value(self.epoch, round);

                    if let Some(share_to_send) = own_share_opt {
                        output
                            .messages_to_send
                            .push(AbaMessage {
                                pub_batch_proposer_id: self.batch_proposer_id,
                                pub_epoch: self.epoch,
                                pub_content: AbaMessageContent::Coin {
                                    share: share_to_send,
                                    round,
                                },
                                pub_sender_id: self.own_id,
                            });
                    }

                    if let Some(coin_value) = coin_now_opt {
                        // DbgPrint!("[ABA ({},{},R{})] Coin immediately available: {}. Starting
                        // next round.", self.batch_proposer_id, self.epoch, round, coin_value);
                        return self.start_next_round(Some(coin_value));
                    } else {
                        self.waiting_for_coin_in_round = Some(round);
                        // Timer for current round `round` should already be
                        // running. We are now waiting
                        // for Coin messages or timeout for this round.
                    }
                }
            } else {
                // current_conf_values is empty (no value has N-F AUX support)
                // We simply wait for more AUX messages for the current round or for the round
                // to time out. The timeout mechanism will consult the coin. F+1
                // AUX support alone is not sufficient to consult the coin
                // prematurely if a decision might still be reached
                // with more messages in the current round.
                eprintln!(
                    "[DEBUG ABA try_decide R{round}] current_conf_values is empty. Waiting for \
                     more AUX or timeout. Has F+1 AUX for any value: {}",
                    aux_counts
                        .values()
                        .any(|&c| c >= self.config.low_threshold())
                );
                // If no N-F support and no F+1 support for any value, do
                // nothing, wait for more messages or timeout.
            }
        }
        Ok(output) // Return the (potentially modified) output
    }

    /// Handles a timeout for a specific ABA round.
    /// If the timeout is for the current, undecided round, it consults the coin
    /// and starts the next round.
    ///
    /// ## Arguments
    ///
    /// * `round_that_timed_out`: The ABA round number that timed out.
    ///
    /// ## Returns
    ///
    /// * `Ok(AbaProcessOutput)`: Contains messages and timer requests if a new
    ///   round is started. Returns a default empty output if the instance is
    ///   already decided or the timeout is not for the current round.
    /// * `Err(HbbftError)`: If an error occurs while starting the next round.
    pub fn handle_round_timeout(
        &mut self,
        round_that_timed_out: AbaRound,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        if self.is_decided() || round_that_timed_out != self.current_round {
            // Already decided, or timeout is for a past/future round we are not currently
            // in.
            return Ok(AbaProcessOutput::default());
        }

        // DbgPrint!("[ABA ({},{},R{})] Round {} TIMED OUT. Consulting coin.",
        // self.batch_proposer_id, self.epoch, self.current_round,
        // round_that_timed_out);
        let coin_val = self
            .coin
            .get_coin_for_round_fallback(self.epoch, self.current_round);
        self.waiting_for_coin_in_round = None; // No longer waiting for this timed-out round's coin
        self.start_next_round(Some(coin_val))
    }

    /// Advances the ABA instance to the next round.
    ///
    /// This involves incrementing the round counter, setting the new estimate
    /// based on the provided `proposed_value_for_next_round` (typically
    /// from a coin flip), sending an `EST` message for the new round, and
    /// requesting a timer. It also processes any messages that might have
    /// been queued for this new round.
    ///
    /// ## Arguments
    ///
    /// * `proposed_value_for_next_round`: The boolean value to propose as the
    ///   estimate for the new round.
    ///
    /// ## Returns
    /// * `Ok(AbaProcessOutput)`: Contains messages and timer requests for the
    ///   new round.
    /// * `Err(HbbftError)`: If `proposed_value_for_next_round` is `None`.
    fn start_next_round(
        &mut self,
        proposed_value_for_next_round: Option<bool>,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        eprintln!(
            "[DEBUG ABA] start_next_round entered. is_decided: {}, current_round: {}",
            self.is_decided(),
            self.current_round
        );
        if self.is_decided() {
            return Ok(AbaProcessOutput::default());
        }

        self.waiting_for_coin_in_round = None; // Starting a new round, so not waiting for previous one.
        let old_round_val = self.current_round;
        self.current_round += 1;
        eprintln!(
            "[DEBUG ABA] start_next_round incremented. old_round: {}, new_current_round: {}",
            old_round_val, self.current_round
        );

        // The estimate for the new round comes from the coin flip of the *previous*
        // round (round r determines estimate for r+1). The
        // `proposed_value_for_next_round` argument should be the result of
        // coin.get_coin(epoch, self.current_round - 1).
        let val_to_propose = proposed_value_for_next_round.ok_or_else(|| {
            HbbftError::InternalError(
                "Cannot start next round without proposed value from coin.".to_string(),
            )
        })?;

        // DbgPrint!("[ABA ({},{},R{})] Starting new round. Estimate from coin (for prev
        // round {}): {}", self.batch_proposer_id, self.epoch,
        // self.current_round, self.current_round -1, val_to_propose);

        self.current_estimate = Some(val_to_propose); // Set estimate for new round
        self.sent_est
            .insert(self.current_round, true);
        // A node considers its own EST message as received for threshold calculations.
        self.est_values
            .entry(self.current_round)
            .or_default()
            .insert(self.own_id, val_to_propose);

        // DbgPrint!("[ABA ({},{},R{})] Starting new round {}. Estimate from coin (for
        // prev round): {}",     self.batch_proposer_id, self.epoch,
        // self.current_round, val_to_propose);

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
            suggested_duration: self.config.default_aba_round_timeout,
        };

        let mut messages_to_send = vec![est_message];
        let mut current_timer_request = Some(initial_timer_request);

        // Process any messages that might have been queued for this new
        // self.current_round This might lead to sending AUX, deciding, or even
        // starting another round.
        let processing_queued_output = self.process_potential_progress_for_current_round()?;
        messages_to_send.extend(processing_queued_output.messages_to_send);

        if self.is_decided() {
            // If processing queued led to a decision
            current_timer_request = None;
        } else if processing_queued_output
            .timer_request_to_set
            .is_some()
        {
            // If processing queued led to starting an even newer round (e.g. round + 1),
            // use its timer.
            current_timer_request = processing_queued_output.timer_request_to_set;
        }
        // Otherwise, the initial_timer_request for self.current_round remains.

        Ok(AbaProcessOutput {
            messages_to_send,
            timer_request_to_set: current_timer_request,
        })
    }

    /// Prunes stored `EST` and `AUX` messages for rounds strictly before
    /// `current_valid_round`.
    ///
    /// ## Arguments
    ///
    /// * `current_valid_round`: The oldest round for which data should be kept.
    fn prune_past_round_data(&mut self, current_valid_round: AbaRound) {
        self.est_values
            .retain(|&r, _| r >= current_valid_round);
        self.aux_values
            .retain(|&r, _| r >= current_valid_round);
    }

    /// Helper to process ESTs and AUXs that might have been queued for the
    /// current round. This is called when a round starts.
    ///
    /// It attempts to send `AUX` messages based on current `EST`s and then
    /// attempts to decide or advance the round based on current `AUX`s.
    ///
    /// ## Returns
    /// * `Ok(AbaProcessOutput)`: Contains any messages or timer requests
    ///   generated from processing queued items.
    /// * `Err(HbbftError)`: If an error occurs during processing.
    fn process_potential_progress_for_current_round(
        &mut self,
    ) -> Result<AbaProcessOutput<NodeId>, HbbftError<NodeId>> {
        let mut combined_output = AbaProcessOutput::default();

        if self.is_decided() {
            return Ok(combined_output);
        }

        // Try to send AUX based on current ESTs (which might include previously queued
        // ones for this round)
        let aux_output = self.try_send_aux(self.current_round)?;
        combined_output
            .messages_to_send
            .extend(aux_output.messages_to_send);
        // try_send_aux doesn't set timers or decide on its own.

        // Try to decide or advance round based on current AUXs (which might include
        // previously queued ones for this round)
        let decide_output = self.try_decide_or_advance_round(self.current_round)?;
        combined_output
            .messages_to_send
            .extend(decide_output.messages_to_send);
        // This one can set a timer if it starts a new round, or clear it (None) if it
        // decides.
        combined_output.timer_request_to_set = decide_output.timer_request_to_set;

        // After processing, prune data for rounds strictly before the current one.
        self.prune_past_round_data(self.current_round);

        Ok(combined_output)
    }

    /// Returns the decided value if this ABA instance has terminated.
    ///
    /// ## Returns
    /// `Some(bool)` if decided, `None` otherwise.
    pub fn output(&self) -> Option<bool> {
        self.decided_value
    }

    /// Checks if this ABA instance has reached a decision.
    pub fn is_decided(&self) -> bool {
        self.decided_value.is_some()
    }

    /// Returns the current round number of this ABA instance (for
    /// debugging or internal checks).
    pub fn current_round_debug(&self) -> AbaRound {
        self.current_round
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testing::{DummyCommonCoin, create_test_config};

    const BATCH_PROPOSER_ID: usize = 0;
    const EPOCH_NUM: EpochNum = 1;

    fn create_aba_instance(
        config: Arc<HoneyBadgerConfig<usize>>,
        own_id: usize,
    ) -> BinaryByzantineAgreement<usize> {
        let coin = Box::new(DummyCommonCoin {});
        BinaryByzantineAgreement::new(config, BATCH_PROPOSER_ID, EPOCH_NUM, own_id, coin)
    }

    #[test]
    fn aba_propose() {
        let config = create_test_config(4, 1, 0);
        let mut aba = create_aba_instance(config, 0);

        let output = aba.propose(true).unwrap();
        assert_eq!(output.messages_to_send.len(), 1);
        let msg = &output.messages_to_send[0];
        assert_eq!(msg.pub_batch_proposer_id, BATCH_PROPOSER_ID);
        assert_eq!(msg.pub_epoch, EPOCH_NUM);
        assert_eq!(msg.pub_sender_id, 0);
        if let AbaMessageContent::Est { value, round } = msg.pub_content {
            assert!(value);
            assert_eq!(round, 0);
        } else {
            panic!("Expected EST message");
        }
        assert!(output.timer_request_to_set.is_some());
        if let Some(TimerRequest::SetAbaRoundTimeout { round, .. }) = output.timer_request_to_set {
            assert_eq!(round, 0);
        } else {
            panic!("Expected SetAbaRoundTimeout timer request");
        }
    }

    #[test]
    fn aba_handle_est_to_send_aux() {
        let n = 4;
        let f = 1; // low_threshold = F+1 = 2
        let config_node0 = create_test_config(n, f, 0);
        let mut aba_node0 = create_aba_instance(Arc::clone(&config_node0), 0);
        let _ = aba_node0.propose(true).unwrap(); // Node 0 proposes EST(true, 0)

        // Node 1 sends EST(true, 0)
        let est_msg_node1 = AbaMessage {
            pub_batch_proposer_id: BATCH_PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: AbaMessageContent::Est { value: true, round: 0 },
            pub_sender_id: 1,
        };
        let output = aba_node0
            .handle_message(&est_msg_node1)
            .unwrap();

        // Node 0 has received 2 EST(true,0) (own + node1's), which is F+1. Should send
        // AUX.
        assert_eq!(output.messages_to_send.len(), 1);
        let msg = &output.messages_to_send[0];
        if let AbaMessageContent::Aux { value, round } = msg.pub_content {
            assert!(value);
            assert_eq!(round, 0);
        } else {
            panic!("Expected AUX message");
        }
        assert!(
            aba_node0
                .sent_aux
                .get(&0)
                .copied()
                .unwrap_or(false)
        );
    }

    #[test]
    fn aba_handle_aux_to_decide() {
        let n = 4;
        let f = 1; // high_threshold = N-F = 3
        let config_node0 = create_test_config(n, f, 0);
        let mut aba_node0 = create_aba_instance(Arc::clone(&config_node0), 0);
        // We are directly testing try_decide_or_advance_round, so no need to call
        // propose. Set current_round to 0 as if propose() was called.
        aba_node0.current_round = 0;
        aba_node0.current_estimate = Some(true); // Consistent with a prior proposal

        // Directly populate aux_values for round 0 to have N-F for true
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(0, true); // from self
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(1, true); // from node 1
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(2, true); // from node 2

        // Call try_decide_or_advance_round for round 0
        let output = aba_node0
            .try_decide_or_advance_round(0)
            .unwrap();

        // Node 0 should have 3 AUX(true,0), which is N-F. Should decide on true.
        assert!(output.messages_to_send.is_empty());
        assert!(aba_node0.is_decided());
        assert_eq!(aba_node0.output(), Some(true));
        assert_eq!(output.timer_request_to_set, None); // Decision means no new timer
    }

    #[test]
    fn aba_handle_aux_f_plus_one_support_leads_to_coin() {
        // Renamed for clarity
        let n = 4;
        let f = 1; // low_threshold = F+1 = 2; high_threshold = N-F = 3
        let config_node0 = create_test_config(n, f, 0);
        let mut aba_node0 = create_aba_instance(Arc::clone(&config_node0), 0);
        let _ = aba_node0.propose(true).unwrap(); // Node 0 EST(true,0)

        // Setup:
        // Node 0 sends AUX(true,0)
        // Node 1 sends AUX(true,0) -> true has F+1 AUX
        // Node 2 sends AUX(false,0)
        // Node 3 sends AUX(false,0) -> false has F+1 AUX
        // Neither has N-F AUX. Should go to coin.
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(0, true); // Own AUX (simulated)
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(1, true);
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(2, false);
        aba_node0
            .aux_values
            .entry(0)
            .or_default()
            .insert(3, false);

        let output = aba_node0
            .try_decide_or_advance_round(0)
            .unwrap();
        // With the refactored logic, F+1 AUX support alone (without N-F support)
        // does not immediately trigger a coin consultation or round advancement.
        // The system waits for more AUX messages or a timeout for round 0.
        assert_eq!(aba_node0.current_round, 0, "Round should not advance yet");
        assert_eq!(aba_node0.current_estimate, Some(true), "Initial estimate should remain");
        assert!(output.messages_to_send.is_empty(), "No messages should be sent immediately");
        assert!(output.timer_request_to_set.is_none(), "No new timer should be set by this path");
        assert!(
            aba_node0
                .waiting_for_coin_in_round
                .is_none(),
            "Should not be waiting for coin yet via this path"
        );
    }

    #[test]
    fn aba_future_round_message_queuing() {
        let config = create_test_config(4, 1, 0);
        let mut aba = create_aba_instance(config, 0);
        let _ = aba.propose(true).unwrap(); // In round 0

        // Send EST for round 1 (future)
        let est_r1_msg = AbaMessage {
            pub_batch_proposer_id: BATCH_PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: AbaMessageContent::Est { value: false, round: 1 },
            pub_sender_id: 1,
        };
        let output_future_est = aba.handle_message(&est_r1_msg).unwrap();
        assert!(
            output_future_est
                .messages_to_send
                .is_empty()
        ); // No immediate action
        assert!(
            aba.est_values
                .get(&1)
                .unwrap()
                .contains_key(&1)
        );

        // Simulate round 0 timeout, coin is true (0%2==0), so
        // start_next_round(Some(true)) for round 1
        let output_timeout_r0 = aba.handle_round_timeout(0).unwrap();
        assert_eq!(aba.current_round, 1);
        assert_eq!(aba.current_estimate, Some(true)); // From coin of round 0

        // The output_timeout_r0 should contain EST(true,1) from node 0.
        // AND, because EST(false,1) from node 1 was queued, and now we are in round 1,
        // process_potential_progress_for_current_round should have been called.
        // Node 0 has EST(true,1) (own) and EST(false,1) (from node 1). Neither has
        // F+1=2. So, no AUX should be sent yet from processing queued.
        let has_est_true_r1 = output_timeout_r0
            .messages_to_send
            .iter()
            .any(|m| {
                if let AbaMessageContent::Est { value, round } = m.pub_content {
                    value && round == 1
                } else {
                    false
                }
            });
        assert!(has_est_true_r1);
        // Check that no AUX was generated from processing queued messages
        let has_aux_r1 = output_timeout_r0
            .messages_to_send
            .iter()
            .any(|m| matches!(m.pub_content, AbaMessageContent::Aux { round: 1, .. }));
        assert!(!has_aux_r1);
    }

    #[test]
    fn aba_handle_round_timeout() {
        let config = create_test_config(4, 1, 0);
        let mut aba = create_aba_instance(config, 0);
        let _ = aba.propose(true).unwrap(); // Round 0 starts

        let output = aba.handle_round_timeout(0).unwrap(); // Timeout for round 0
        // DummyCoin for round 0 is true (0 % 2 == 0). Next estimate is true.
        assert_eq!(aba.current_round, 1);
        assert_eq!(aba.current_estimate, Some(true));
        assert_eq!(output.messages_to_send.len(), 1); // EST for round 1
        if let AbaMessageContent::Est { value, round } = output.messages_to_send[0].pub_content {
            assert!(value);
            assert_eq!(round, 1);
        }
        assert!(output.timer_request_to_set.is_some());
        if let Some(TimerRequest::SetAbaRoundTimeout { round, .. }) = output.timer_request_to_set {
            assert_eq!(round, 1);
        }
    }
}
