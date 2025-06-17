// This content is intended for src/rbc.rs

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::HbbftError;
use crate::config::HoneyBadgerConfig;
use crate::types::{Batch, BatchHash, EpochNum, NodeId, RbcMessage, RbcMessageContent, hash_batch};

#[derive(Debug)]
pub struct ReliableBroadcast {
    config: Arc<HoneyBadgerConfig>,
    proposer_id: NodeId,
    epoch: EpochNum,
    own_id: NodeId,

    // Payload storage: maps hash to the actual batch.
    // Populated when a VAL(batch) is received and validated.
    known_payloads: HashMap<BatchHash, Batch>,

    // Tracks the hash we've sent ECHO for. A node sends ECHO for at most one hash.
    initial_echo_sent_for_hash: Option<BatchHash>,
    // Tracks the hash we've sent READY for. A node sends READY for at most one hash.
    ready_sent_for_hash: Option<BatchHash>,

    // Stores senders for ECHO messages, per hash.
    echo_msg_senders: HashMap<BatchHash, HashSet<NodeId>>,
    // Stores senders for READY messages, per hash.
    ready_msg_senders: HashMap<BatchHash, HashSet<NodeId>>,

    // Final accepted hash for this RBC instance.
    accepted_hash: Option<BatchHash>,
    // Flag to ensure output() is called only once effectively.
    accepted_batch_delivered: bool,

    // Keep track of the first VAL message content received from the proposer.
    // This is to detect/ignore proposer equivocation regarding the batch content.
    // If proposer sends VAL(b1) then VAL(b2), we stick to the first one.
    first_val_content_hash_received: Option<BatchHash>,
}

impl ReliableBroadcast {
    pub fn new(
        config: Arc<HoneyBadgerConfig>, // Accept Arc<HoneyBadgerConfig>
        proposer_id: NodeId,
        epoch: EpochNum,
        own_id: NodeId,
    ) -> Self {
        ReliableBroadcast {
            config,
            proposer_id,
            epoch,
            own_id,
            known_payloads: HashMap::new(),
            initial_echo_sent_for_hash: None,
            ready_sent_for_hash: None,
            echo_msg_senders: HashMap::new(),
            ready_msg_senders: HashMap::new(),
            accepted_hash: None,
            accepted_batch_delivered: false,
            first_val_content_hash_received: None,
        }
    }

    /// Initiates RBC if this node is the proposer.
    /// Returns VAL and the proposer's own ECHO message.
    pub fn propose(&mut self, batch: Batch) -> Result<Vec<RbcMessage>, HbbftError> {
        if self.own_id != self.proposer_id {
            return Err(HbbftError::InternalError(
                "Non-proposer tried to initiate RBC VAL".to_string(),
            ));
        }
        if self
            .initial_echo_sent_for_hash
            .is_some()
            || self
                .first_val_content_hash_received
                .is_some()
        {
            // Already proposed and processed our own VAL/ECHO effectively
            return Ok(Vec::new());
        }

        let batch_hash = hash_batch(&batch);

        // Store our own proposal
        self.known_payloads
            .insert(batch_hash, batch.clone());
        self.first_val_content_hash_received = Some(batch_hash);

        // Mark that we (as proposer) have "sent" our ECHO for this hash
        self.initial_echo_sent_for_hash = Some(batch_hash);
        // Add self to echo senders for our own hash
        self.echo_msg_senders
            .entry(batch_hash)
            .or_default()
            .insert(self.own_id);

        let val_message = RbcMessage {
            pub_proposer_id: self.proposer_id,
            pub_epoch: self.epoch,
            pub_content: RbcMessageContent::Val(batch),
            pub_sender_id: self.own_id,
        };
        let echo_message = RbcMessage {
            pub_proposer_id: self.proposer_id,
            pub_epoch: self.epoch,
            pub_content: RbcMessageContent::Echo(batch_hash),
            pub_sender_id: self.own_id,
        };

        // Check if our own ECHO allows us to send READY
        // (will only happen if N=1, which is not BFT, but for completeness)
        let mut messages = vec![val_message, echo_message];
        messages.extend(self.check_and_send_ready_if_conditions_met(batch_hash)?);

        Ok(messages)
    }

    /// Handles an incoming RBC message.
    pub fn handle_message(&mut self, message: &RbcMessage) -> Result<Vec<RbcMessage>, HbbftError> {
        if message.pub_proposer_id != self.proposer_id || message.pub_epoch != self.epoch {
            return Err(HbbftError::EpochMismatch {
                expected: self.epoch,
                got: message.pub_epoch,
            });
        }
        if self.is_decided() {
            // If already decided, no more messages needed from us.
            return Ok(Vec::new());
        }

        let sender_id = message.pub_sender_id;
        let mut responses = Vec::new();

        match &message.pub_content {
            RbcMessageContent::Val(received_batch) => {
                if sender_id != self.proposer_id {
                    return Err(HbbftError::InvalidMessage(format!(
                        "RBC VAL from non-proposer {}. Expected {}.",
                        sender_id, self.proposer_id
                    )));
                }

                let received_batch_hash = hash_batch(received_batch);

                if self
                    .first_val_content_hash_received
                    .is_some()
                    && self.first_val_content_hash_received != Some(received_batch_hash)
                {
                    // Proposer is equivocating by sending different VAL contents.
                    // We stick to the first VAL's hash we processed. This VAL is ignored.
                    // A more advanced implementation might flag the proposer.
                    // DbgPrint!("[RBC ({},{})] Proposer {} equivocated VAL. Sticking to first.",
                    // self.proposer_id, self.epoch, self.proposer_id);
                    return Ok(Vec::new());
                }

                if self
                    .first_val_content_hash_received
                    .is_none()
                {
                    // First time receiving VAL
                    self.known_payloads
                        .insert(received_batch_hash, received_batch.clone());
                    self.first_val_content_hash_received = Some(received_batch_hash);

                    // If we haven't sent an ECHO yet for any hash, send one for this VAL's hash.
                    if self
                        .initial_echo_sent_for_hash
                        .is_none()
                    {
                        self.initial_echo_sent_for_hash = Some(received_batch_hash);
                        self.echo_msg_senders
                            .entry(received_batch_hash)
                            .or_default()
                            .insert(self.own_id); // Our own echo contribution
                        responses.push(RbcMessage {
                            pub_proposer_id: self.proposer_id,
                            pub_epoch: self.epoch,
                            pub_content: RbcMessageContent::Echo(received_batch_hash),
                            pub_sender_id: self.own_id,
                        });
                        // After sending ECHO, check if we can send READY for this hash
                        responses.extend(
                            self.check_and_send_ready_if_conditions_met(received_batch_hash)?,
                        );
                    }
                }
            }

            RbcMessageContent::Echo(echoed_hash) => {
                // Ignore ECHO if we don't have the payload. Node should wait for VAL.
                if !self
                    .known_payloads
                    .contains_key(echoed_hash)
                {
                    // DbgPrint!("[RBC ({},{})] Received ECHO for unknown hash {:?}, sender {}",
                    // self.proposer_id, self.epoch, echoed_hash, sender_id);
                    return Ok(Vec::new());
                }

                let senders = self
                    .echo_msg_senders
                    .entry(*echoed_hash)
                    .or_default();
                if senders.insert(sender_id) {
                    // If true, new sender added
                    // Try to send our own ECHO (relay condition)
                    if self.initial_echo_sent_for_hash.is_none() && // We haven't sent any ECHO yet
                        senders.len() >= self.config.low_threshold()
                    // F+1 ECHOs for this hash
                    {
                        self.initial_echo_sent_for_hash = Some(*echoed_hash);
                        // Add our own echo to the count immediately if we decide to send one
                        self.echo_msg_senders
                            .entry(*echoed_hash)
                            .or_default()
                            .insert(self.own_id);

                        responses.push(RbcMessage {
                            pub_proposer_id: self.proposer_id,
                            pub_epoch: self.epoch,
                            pub_content: RbcMessageContent::Echo(*echoed_hash),
                            pub_sender_id: self.own_id,
                        });
                    }
                    // After potentially sending/relaying ECHO, check if we can send READY for this
                    // hash
                    responses.extend(self.check_and_send_ready_if_conditions_met(*echoed_hash)?);
                }
            }

            RbcMessageContent::Ready(ready_hash) => {
                // Ignore READY if we don't have the payload.
                if !self
                    .known_payloads
                    .contains_key(ready_hash)
                {
                    // DbgPrint!("[RBC ({},{})] Received READY for unknown hash {:?}, sender {}",
                    // self.proposer_id, self.epoch, ready_hash, sender_id);
                    return Ok(Vec::new());
                }

                let senders = self
                    .ready_msg_senders
                    .entry(*ready_hash)
                    .or_default();
                if senders.insert(sender_id) {
                    // If true, new sender added
                    // Try to send our own READY (relay condition)
                    // Must have sent ECHO for this *specific hash* previously.
                    if self.ready_sent_for_hash.is_none() && // We haven't sent any READY yet
                        self.initial_echo_sent_for_hash == Some(*ready_hash) && // We are on board with this hash
                        senders.len() >= self.config.low_threshold()
                    // F+1 READYs for this hash
                    {
                        self.ready_sent_for_hash = Some(*ready_hash);
                        // Add our own READY to the count using the existing mutable reference
                        // `senders`
                        senders.insert(self.own_id);

                        responses.push(RbcMessage {
                            pub_proposer_id: self.proposer_id,
                            pub_epoch: self.epoch,
                            pub_content: RbcMessageContent::Ready(*ready_hash),
                            pub_sender_id: self.own_id,
                        });
                    }

                    // Check for delivery condition
                    // Must have N-F READYs for this hash.
                    // (Some variants use 2F+1, but N-F is stronger if N > 3F+1. For N=3F+1, they
                    // are equivalent for honest majority)
                    if self.accepted_hash.is_none() && senders.len() >= self.config.high_threshold()
                    // N-F READYs for this hash
                    {
                        // Check if we *also* sent READY for this hash, or at least ECHO'd this hash
                        // This ensures we deliver something we at least minimally
                        // processed/validated.
                        if self.ready_sent_for_hash == Some(*ready_hash)
                            || self.initial_echo_sent_for_hash == Some(*ready_hash)
                        {
                            self.accepted_hash = Some(*ready_hash);
                            // DbgPrint!("[RBC ({},{})] DELIVERED hash {:?} with
                            // N-F READYs",
                            // self.proposer_id, self.epoch, ready_hash);
                            // No message to send upon delivery, it's an
                            // internal state change.
                            // The output() method will be used to retrieve it.
                        } else {
                            // We have N-F READYs for a hash, but we never sent ECHO or READY for
                            // it. This might happen if we received VAL,
                            // but then many ECHOs for *another* hash from an equivocating proposer.
                            // This situation is complex. For safety, a node should ideally only
                            // deliver a value it has itself validated
                            // and ECHOed. However, Bracha's termination
                            // relies on the F+1 READY rule forcing everyone onto the same READY.
                            // If N-F nodes send READY(h), and this node receives N-F READY(h), it
                            // must deliver h. This implies it *must*
                            // have the payload for h. Our check
                            // `known_payloads.contains_key(ready_hash)` ensures this.
                            // DbgPrint!("[RBC ({},{})] Conditions met for delivery of {:?}, but
                            // this node hadn't sent ECHO/READY for it. Proceeding with delivery as
                            // N-F Ready.", self.proposer_id, self.epoch, ready_hash);
                            self.accepted_hash = Some(*ready_hash);
                        }
                    }
                }
            }
        }
        Ok(responses)
    }

    /// Helper to check and generate READY message if conditions met.
    /// This is called after receiving VAL, ECHO, or processing our own
    /// proposal.
    fn check_and_send_ready_if_conditions_met(
        &mut self,
        hash_to_check: BatchHash,
    ) -> Result<Vec<RbcMessage>, HbbftError> {
        let mut responses = Vec::new();
        if self.ready_sent_for_hash.is_some() || self.is_decided() {
            // Already sent READY or decided
            return Ok(responses);
        }

        // Must have payload and have sent ECHO for *this specific hash*
        if self
            .known_payloads
            .contains_key(&hash_to_check)
            && self.initial_echo_sent_for_hash == Some(hash_to_check)
        {
            if let Some(echo_senders_for_hash) = self
                .echo_msg_senders
                .get(&hash_to_check)
            {
                // If N-F ECHOs for this hash (which we also ECHOed)
                if echo_senders_for_hash.len() >= self.config.high_threshold() {
                    self.ready_sent_for_hash = Some(hash_to_check);
                    // Add our own READY to the count.
                    self.ready_msg_senders
                        .entry(hash_to_check)
                        .or_default()
                        .insert(self.own_id);

                    responses.push(RbcMessage {
                        pub_proposer_id: self.proposer_id,
                        pub_epoch: self.epoch,
                        pub_content: RbcMessageContent::Ready(hash_to_check),
                        pub_sender_id: self.own_id,
                    });

                    // After sending READY, immediately check for delivery condition with our own
                    // READY message.
                    if self.accepted_hash.is_none() {
                        if let Some(ready_senders_for_hash) = self
                            .ready_msg_senders
                            .get(&hash_to_check)
                        {
                            if ready_senders_for_hash.len() >= self.config.high_threshold() {
                                // DbgPrint!("[RBC ({},{})] DELIVERED hash {:?} (triggered by own
                                // READY with N-F total READYs)", self.proposer_id, self.epoch,
                                // hash_to_check);
                                self.accepted_hash = Some(hash_to_check);
                            }
                        }
                    }
                }
            }
        }
        Ok(responses)
    }

    /// Returns the decided Batch if RBC has completed.
    pub fn output(&mut self) -> Option<Batch> {
        if self.accepted_batch_delivered || self.accepted_hash.is_none() {
            return None;
        }

        if let Some(hash) = self.accepted_hash {
            if let Some(batch) = self.known_payloads.get(&hash) {
                self.accepted_batch_delivered = true;
                return Some(batch.clone());
            } else {
                // Should not happen if logic is correct: accepted a hash but don't have
                // payload. This would be a critical bug or inconsistency.
                // DbgPrint!("[RBC ({},{})] CRITICAL: Accepted hash {:?} but batch data not
                // found!", self.proposer_id, self.epoch, hash);
                return None;
            }
        }
        None
    }

    pub fn is_decided(&self) -> bool {
        self.accepted_hash.is_some()
    }

    /// Checks if the proposer's RBC has been initiated (i.e., VAL/ECHO sent).
    pub fn is_initiated(&self) -> bool {
        self.initial_echo_sent_for_hash
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testing::create_test_config;
    use crate::types::Batch;

    fn create_batch(tx_content: u8) -> Batch {
        Batch::new(vec![vec![tx_content]])
    }

    const PROPOSER_ID: NodeId = 0;
    const EPOCH_NUM: EpochNum = 1;

    #[test]
    fn rbc_propose_by_proposer() {
        let config = create_test_config(4, 1, PROPOSER_ID);
        let mut rbc =
            ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, PROPOSER_ID);
        let batch = create_batch(1);
        let messages = rbc.propose(batch.clone()).unwrap();

        assert_eq!(messages.len(), 2); // VAL and ECHO
        assert!(matches!(messages[0].pub_content, RbcMessageContent::Val(_)));
        assert!(matches!(messages[1].pub_content, RbcMessageContent::Echo(_)));
        assert_eq!(rbc.initial_echo_sent_for_hash, Some(hash_batch(&batch)));
        assert_eq!(
            rbc.known_payloads
                .get(&hash_batch(&batch)),
            Some(&batch)
        );
    }

    #[test]
    fn rbc_propose_by_non_proposer() {
        let config = create_test_config(4, 1, 1); // own_id = 1, proposer_id = 0
        let mut rbc = ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, 1);
        let batch = create_batch(1);
        let result = rbc.propose(batch);
        assert!(result.is_err());
    }

    #[test]
    fn rbc_propose_multiple_times() {
        let config = create_test_config(4, 1, PROPOSER_ID);
        let mut rbc =
            ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, PROPOSER_ID);
        let batch1 = create_batch(1);
        let batch2 = create_batch(2);

        let messages1 = rbc.propose(batch1.clone()).unwrap();
        assert_eq!(messages1.len(), 2);

        let messages2 = rbc.propose(batch2.clone()).unwrap(); // Should effectively do nothing new
        assert!(messages2.is_empty());
        assert_eq!(rbc.initial_echo_sent_for_hash, Some(hash_batch(&batch1))); // Still first batch
    }

    #[test]
    fn rbc_handle_val_from_proposer() {
        let config = create_test_config(4, 1, 1); // own_id = 1
        let mut rbc_node1 = ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, 1);
        let batch = create_batch(1);
        let val_message = RbcMessage {
            pub_proposer_id: PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch.clone()),
            pub_sender_id: PROPOSER_ID,
        };

        let responses = rbc_node1
            .handle_message(&val_message)
            .unwrap();
        assert_eq!(responses.len(), 1); // Node 1 sends ECHO
        assert!(matches!(responses[0].pub_content, RbcMessageContent::Echo(_)));
        if let RbcMessageContent::Echo(hash) = responses[0].pub_content {
            assert_eq!(hash, hash_batch(&batch));
        }
        assert_eq!(rbc_node1.initial_echo_sent_for_hash, Some(hash_batch(&batch)));
    }

    #[test]
    fn rbc_handle_val_equivocation() {
        let config = create_test_config(4, 1, 1);
        let mut rbc_node1 = ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, 1);
        let batch1 = create_batch(1);
        let batch2 = create_batch(2);

        let val_message1 = RbcMessage {
            pub_proposer_id: PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch1.clone()),
            pub_sender_id: PROPOSER_ID,
        };
        let val_message2 = RbcMessage {
            pub_proposer_id: PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch2.clone()),
            pub_sender_id: PROPOSER_ID,
        };

        let responses1 = rbc_node1
            .handle_message(&val_message1)
            .unwrap();
        assert_eq!(responses1.len(), 1); // Sends ECHO for batch1

        let responses2 = rbc_node1
            .handle_message(&val_message2)
            .unwrap();
        assert!(responses2.is_empty()); // Ignores second, different VAL
        assert_eq!(rbc_node1.first_val_content_hash_received, Some(hash_batch(&batch1)));
    }

    #[test]
    fn rbc_handle_val_from_non_proposer() {
        let config = create_test_config(4, 1, 1);
        let mut rbc_node1 = ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, 1);
        let batch = create_batch(1);
        let val_message = RbcMessage {
            pub_proposer_id: PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch.clone()),
            pub_sender_id: 2, // Sent by node 2
        };
        assert!(
            rbc_node1
                .handle_message(&val_message)
                .is_err()
        );
    }

    #[test]
    fn rbc_echo_relay_and_ready_sending() {
        let n = 4;
        let f = 1;
        let config_node0 = create_test_config(n, f, 0); // Proposer
        let mut rbc_node0 = ReliableBroadcast::new(Arc::clone(&config_node0), 0, EPOCH_NUM, 0);
        let batch = create_batch(10);
        let batch_hash = hash_batch(&batch);
        let _ = rbc_node0.propose(batch.clone()); // Node 0 proposes and sends its own VAL, ECHO

        // Simulate other nodes receiving VAL and sending ECHO
        let mut rbc_node1 = ReliableBroadcast::new(create_test_config(n, f, 1), 0, EPOCH_NUM, 1);
        let val_msg_from_0 = RbcMessage {
            pub_proposer_id: 0,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch.clone()),
            pub_sender_id: 0,
        };
        let echo1_responses = rbc_node1
            .handle_message(&val_msg_from_0)
            .unwrap();
        assert_eq!(echo1_responses.len(), 1); // Node1 sends ECHO(h)
        let echo_msg_from_1 = echo1_responses
            .into_iter()
            .find(|m| matches!(m.pub_content, RbcMessageContent::Echo(_)))
            .unwrap();

        // Node 0 receives ECHO from Node 1
        let responses_node0_after_echo1 = rbc_node0
            .handle_message(&echo_msg_from_1)
            .unwrap();
        // Node 0 already sent ECHO. It has 2 ECHOs (0, 1). F+1 = 2.
        // It should not send READY yet, needs N-F=3 ECHOs.
        assert!(responses_node0_after_echo1.is_empty());

        // Node 1 receives ECHO from Node 0 (proposer's initial ECHO)
        let echo_msg_from_0 = RbcMessage {
            pub_proposer_id: 0,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Echo(batch_hash),
            pub_sender_id: 0,
        };
        let responses_node1_after_echo0 = rbc_node1
            .handle_message(&echo_msg_from_0)
            .unwrap();
        // Node 1 has 2 ECHOs (0, 1). F+1 = 2. It already sent its ECHO.
        // It should not send READY yet, needs N-F=3 ECHOs.
        assert!(responses_node1_after_echo0.is_empty());

        // Simulate Node 2 sending ECHO
        let mut rbc_node2 = ReliableBroadcast::new(create_test_config(n, f, 2), 0, EPOCH_NUM, 2);
        let echo2_responses = rbc_node2
            .handle_message(&val_msg_from_0)
            .unwrap();
        let echo_msg_from_2 = echo2_responses
            .into_iter()
            .find(|m| matches!(m.pub_content, RbcMessageContent::Echo(_)))
            .unwrap();

        // Node 0 receives ECHO from Node 2
        let responses_node0_after_echo2 = rbc_node0
            .handle_message(&echo_msg_from_2)
            .unwrap();
        // Node 0 now has 3 ECHOs (0, 1, 2). N-F = 3. It should send READY.
        assert_eq!(responses_node0_after_echo2.len(), 1);
        let ready_msg_from_node0 = responses_node0_after_echo2[0].clone(); // Clone for later use
        assert!(matches!(ready_msg_from_node0.pub_content, RbcMessageContent::Ready(_)));
        assert!(!rbc_node0.is_decided()); // Sending own READY doesn't mean decided yet. Needs N-F total received READYs.

        // Node 1 receives ECHO from Node 2
        let responses_node1_after_echo2 = rbc_node1
            .handle_message(&echo_msg_from_2)
            .unwrap();
        // Node 1 now has 3 ECHOs (0, 1, 2). N-F = 3. It should send READY.
        assert_eq!(responses_node1_after_echo2.len(), 1);
        assert!(matches!(responses_node1_after_echo2[0].pub_content, RbcMessageContent::Ready(_)));

        // Node 1 receives READY from Node 0
        let responses_node1_after_ready0 = rbc_node1
            .handle_message(&ready_msg_from_node0)
            .unwrap();
        // Node 1 has 2 READYs (0, 1). F+1 = 2. It already sent its READY.
        // It should not decide yet, needs N-F=3 READYs.
        assert!(responses_node1_after_ready0.is_empty());
        assert!(!rbc_node1.is_decided());

        // Simulate Node 2 sending READY (after it got N-F ECHOs)
        let mut rbc_node2_ready_state =
            ReliableBroadcast::new(create_test_config(n, f, 2), 0, EPOCH_NUM, 2);
        rbc_node2_ready_state
            .handle_message(&val_msg_from_0)
            .unwrap(); // VAL
        rbc_node2_ready_state
            .handle_message(&echo_msg_from_0)
            .unwrap(); // ECHO from 0
        rbc_node2_ready_state
            .handle_message(&echo_msg_from_1)
            .unwrap(); // ECHO from 1
        // At this point rbc_node2 has 3 ECHOs (0,1,2) and should have sent READY.
        assert!(
            rbc_node2_ready_state
                .ready_sent_for_hash
                .is_some()
        );
        let ready_msg_from_2 = RbcMessage {
            pub_proposer_id: 0,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Ready(batch_hash),
            pub_sender_id: 2,
        };

        // Node 1 receives READY from Node 2
        let responses_node1_after_ready2 = rbc_node1
            .handle_message(&ready_msg_from_2)
            .unwrap();
        // Node 1 now has 3 READYs (0, 1, 2). N-F = 3. It should decide.
        assert!(responses_node1_after_ready2.is_empty()); // No new messages to send
        assert!(rbc_node1.is_decided());
        assert_eq!(rbc_node1.output(), Some(batch));
    }

    #[test]
    fn rbc_idempotency() {
        let config = create_test_config(4, 1, 1);
        let mut rbc = ReliableBroadcast::new(Arc::clone(&config), PROPOSER_ID, EPOCH_NUM, 1);
        let batch = create_batch(1);
        let val_message = RbcMessage {
            pub_proposer_id: PROPOSER_ID,
            pub_epoch: EPOCH_NUM,
            pub_content: RbcMessageContent::Val(batch.clone()),
            pub_sender_id: PROPOSER_ID,
        };

        let responses1 = rbc
            .handle_message(&val_message)
            .unwrap();
        assert_eq!(responses1.len(), 1); // Sends ECHO

        let responses2 = rbc
            .handle_message(&val_message)
            .unwrap(); // Same VAL again
        assert!(responses2.is_empty()); // Should not send ECHO again

        let echo_message = responses1[0].clone();
        let responses3 = rbc
            .handle_message(&echo_message)
            .unwrap(); // Process its own ECHO (no-op)
        assert!(responses3.is_empty());
    }
}
