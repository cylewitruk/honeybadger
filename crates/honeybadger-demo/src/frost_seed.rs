//! Demonstrates a `CommonCoin` implementation using the "Signature Seed" method
//! with FROST (frost-secp256k1) for the `honeybadger` BFT library.
//!
//! ## Method:
//! 1. A one-time, full FROST signature ceremony is conducted to generate a
//!    SEED. This involves standard FROST Round 1 (commitment exchange) and
//!    Round 2 (share exchange) for this single seed signature.
//! 2. The SEED is HASH(FROST_Signature_On_Fixed_Message).
//! 3. Subsequent common coins for HBBFT (epoch, round) are derived as HASH(SEED
//!    || epoch || round).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use frost_secp256k1::keys::{KeyPackage, PublicKeyPackage};
use frost_secp256k1::{
    self as frost, Identifier as FrostIdentifier, Signature, SigningPackage, round1, round2,
};
use honeybadger::{
    AbaRound, CoinFactory, CoinOutput, CoinShare, CommonCoin, EpochNum, HbbftError,
    HoneyBadgerConfig, HoneyBadgerConfigBuilder, NodeIdentifier,
};
use rand_chacha::ChaCha20Rng;
use rand_core::{OsRng, SeedableRng as _};
use sha2::{Digest, Sha256};

/// Module for testing utilities, kept internal to this demonstration library.
mod testing_utils {
    use std::collections::BTreeSet;

    use honeybadger::CoinFactory;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct DemoNodeId(pub usize);
    impl std::fmt::Display for DemoNodeId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl NodeIdentifier for DemoNodeId {}

    pub fn create_test_config_with_node_ids(
        num_faulty_nodes: usize,
        all_node_ids_vec: Vec<DemoNodeId>,
        current_node_id: DemoNodeId,
        coin_factory: CoinFactory<DemoNodeId>,
    ) -> Arc<HoneyBadgerConfig<DemoNodeId>> {
        let all_node_ids_set: BTreeSet<DemoNodeId> = all_node_ids_vec.into_iter().collect();
        Arc::new(
            HoneyBadgerConfigBuilder::new(
                current_node_id,
                all_node_ids_set,
                num_faulty_nodes,
                coin_factory,
            )
            .build()
            .unwrap(),
        )
    }
}
use testing_utils::*;

const SEED_GENERATION_MESSAGE: &[u8] = b"sbtc_hbbft_common_coin_seed_generation_message_v1";

/// Represents the state of the one-time SEED generation ceremony.
#[derive(Debug, Clone)]
enum SeedGenerationState {
    NotStarted,
    AwaitingSeedCommitments {
        // Own nonces and commitments for the seed signature
        own_seed_nonces: Box<round1::SigningNonces>,
        own_seed_commitments: Box<round1::SigningCommitments>,
        // Received commitments from other participants for the seed signature
        // This map will store commitments from those who have responded.
        received_seed_commitments: BTreeMap<FrostIdentifier, round1::SigningCommitments>,
    },
    AwaitingSeedShares {
        // Signing package for the seed signature (derived after all commitments are in)
        seed_signing_package: SigningPackage,
        // The actual participants whose commitments formed this signing package.
        actual_seed_package_participants: Vec<FrostIdentifier>,
        // Own nonces (needed for round2::sign)
        own_seed_nonces: Box<round1::SigningNonces>,
        // Own share for the seed signature (already computed)
        own_seed_share_bytes: Option<Vec<u8>>, // To return to HBBFT if requested again
        // Received shares from other participants for the seed signature
        received_seed_shares: BTreeMap<FrostIdentifier, round2::SignatureShare>,
    },
    SeedGenerated(Arc<[u8; 32]>), // The SEED (hash of the signature)
    SeedGenerationFailed,         // If the seed ceremony fails irrecoverably
}

/// A `CommonCoin` implementation using the "Signature Seed" method with FROST.
#[derive(Debug)]
pub struct SeedBasedFrostCoin<NodeId: NodeIdentifier> {
    hbbft_own_id: NodeId,

    frost_own_identifier: FrostIdentifier,
    key_package: KeyPackage, // Own key share
    group_public_key_package: PublicKeyPackage,

    // --- SEED Generation Specific Fields ---
    /// Min signers for the critical SEED generation (should be high, e.g., N-F
    /// or F+1 Robust). For this demo, we'll use HBBFT's low_threshold (F+1)
    /// for simplicity of testing, but in production, this should be a
    /// higher threshold for the seed.
    min_signers_for_seed: u16,
    /// All participants involved in generating the SEED. Typically all N nodes.
    seed_participants: Vec<FrostIdentifier>, /* Sorted list of FrostIdentifiers for
                                              * deterministic SigningPackage */
    seed_generation_state: SeedGenerationState,
    node_id_to_frost_id: Arc<HashMap<NodeId, FrostIdentifier>>,

    /// Cache for coins already derived from the SEED.
    reconstructed_derived_coins: HashMap<(EpochNum, AbaRound), bool>,
}

impl<NodeId: NodeIdentifier> SeedBasedFrostCoin<NodeId> {
    pub fn new(
        config: Arc<HoneyBadgerConfig<NodeId>>,
        hbbft_own_id: NodeId,
        frost_own_identifier: FrostIdentifier,
        key_package: KeyPackage,
        group_public_key_package: PublicKeyPackage,
        node_id_to_frost_id: Arc<HashMap<NodeId, FrostIdentifier>>,
        // All FROST identifiers of the N nodes, sorted.
        all_frost_participants_sorted: Vec<FrostIdentifier>,
    ) -> Self {
        // For SEED generation, we want a robust threshold.
        // Let's use config.num_total_nodes() - config.num_faulty_nodes() which is N-F
        // (high_threshold). Or for simpler testing with fewer nodes, just F+1
        // can be used, but acknowledge this.
        let min_signers_for_seed =
            u16::try_from(config.high_threshold()).expect("N-F threshold too large for u16");

        if min_signers_for_seed == 0 {
            panic!("min_signers_for_seed (N-F) cannot be zero");
        }

        // In a real scenario, seed_participants might be all N nodes.
        // For the demo, we could use the same subset logic as before for consistency
        // or specifically define them as all nodes. Let's use
        // all_frost_participants_sorted and ensure min_signers_for_seed
        // <= all_frost_participants_sorted.len()

        if min_signers_for_seed as usize > all_frost_participants_sorted.len() {
            panic!("Not enough participants for robust seed generation threshold.");
        }

        SeedBasedFrostCoin {
            hbbft_own_id,
            frost_own_identifier,
            key_package,
            group_public_key_package,
            min_signers_for_seed,
            seed_participants: all_frost_participants_sorted, // All nodes participate in seed
            seed_generation_state: SeedGenerationState::NotStarted,
            node_id_to_frost_id,
            reconstructed_derived_coins: HashMap::new(),
        }
    }

    /// Derives the coin value once the SEED is known.
    fn derive_coin_from_seed(seed: &Arc<[u8; 32]>, epoch: EpochNum, round: AbaRound) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(seed.as_ref());
        hasher.update(b"derived_coin"); // Domain separation
        hasher.update(epoch.to_le_bytes());
        hasher.update(round.to_le_bytes());
        let hash_result = hasher.finalize();
        hash_result[0] % 2 == 0 // Simple way to get a boolean
    }

    /// Generates this node's Round 1 commitments for the SEED signature.
    /// This is part of the one-time SEED generation.
    fn generate_own_seed_commitments(&self) -> (round1::SigningNonces, round1::SigningCommitments) {
        // For the SEED generation, we use *actual randomness* for nonces.
        // Each node generates its own truly random nonces.
        round1::commit(self.key_package.signing_share(), &mut OsRng)
    }

    /// Constructs the SigningPackage for the SEED message once all commitments
    /// are collected.
    /// The `commitments_map` should only contain commitments from the actual
    /// participants.
    fn get_seed_signing_package(
        commitments_map: &BTreeMap<FrostIdentifier, round1::SigningCommitments>,
    ) -> SigningPackage {
        SigningPackage::new(commitments_map.clone(), SEED_GENERATION_MESSAGE) // clone is on BTreeMap
    }

    /// Generates this node's Round 2 signature share for the SEED signature.
    fn generate_own_seed_share(
        &self,
        seed_signing_package: &SigningPackage,
        own_seed_nonces: &round1::SigningNonces,
    ) -> Result<round2::SignatureShare, HbbftError<NodeId>> {
        round2::sign(seed_signing_package, own_seed_nonces, &self.key_package)
            .map_err(|e| HbbftError::InternalError(format!("FROST sign failed for seed: {e:?}")))
    }

    /// Aggregates shares to produce the final SEED signature.
    fn aggregate_seed_signature(
        &self,
        seed_signing_package: &SigningPackage,
        received_seed_shares: &BTreeMap<FrostIdentifier, round2::SignatureShare>,
        // actual_package_participants: &[FrostIdentifier], // Used to validate shares are from
        // expected set
    ) -> Result<Signature, HbbftError<NodeId>> {
        // Build the intersection of actually received shares with the signing package
        // and the public key package. Do not stop at threshold; pass all valid shares.
        let package_commitments_map = seed_signing_package.signing_commitments();
        let pubkey_verifying_shares_map = self
            .group_public_key_package
            .verifying_shares();

        let mut valid_shares_for_aggregation = BTreeMap::new();
        for (id, share) in received_seed_shares {
            if package_commitments_map.contains_key(id)
                && pubkey_verifying_shares_map.contains_key(id)
            {
                valid_shares_for_aggregation.insert(*id, *share);
            }
        }

        let pkg_len = seed_signing_package
            .signing_commitments()
            .len();
        eprintln!(
            "[DEBUG AGGREGATE SEED SIG] Package size: {}, shares used: {}",
            pkg_len,
            valid_shares_for_aggregation.len()
        );

        if pkg_len != self.min_signers_for_seed as usize {
            return Err(HbbftError::InternalError(format!(
                "SigningPackage size mismatch: got {}, expected {}. Package must be built from \
                 the threshold set.",
                pkg_len, self.min_signers_for_seed
            )));
        }

        if valid_shares_for_aggregation.len() < self.min_signers_for_seed as usize {
            return Err(HbbftError::InternalError(format!(
                "Not enough valid shares to aggregate seed sig: got {}, need {}",
                valid_shares_for_aggregation.len(),
                self.min_signers_for_seed
            )));
        }

        // Optional: keep the helpful diagnostics you added
        eprintln!("[DEBUG AGGREGATE SEED SIG] Node ID: {:?}", self.hbbft_own_id);
        eprintln!("[DEBUG AGGREGATE SEED SIG] Min signers for seed: {}", self.min_signers_for_seed);
        eprintln!(
            "[DEBUG AGGREGATE SEED SIG] Seed Signing Package Commitments ({}):",
            package_commitments_map.len()
        );
        for (id, _) in package_commitments_map.iter() {
            eprintln!(
                "  - Package Committer (Serialized): {:?}, (Debug): {:?}",
                id.serialize(),
                id
            );
        }
        eprintln!(
            "[DEBUG AGGREGATE SEED SIG] Group Public Key Package Verifying Shares ({}):",
            pubkey_verifying_shares_map.len()
        );
        for (id, _) in pubkey_verifying_shares_map.iter() {
            eprintln!(
                "  - PubKey Verifying Share for (Serialized): {:?}, (Debug): {:?}",
                id.serialize(),
                id
            );
        }
        eprintln!(
            "[DEBUG AGGREGATE SEED SIG] Valid Shares for Aggregation ({}):",
            valid_shares_for_aggregation.len()
        );
        for (id, _) in valid_shares_for_aggregation.iter() {
            eprintln!(
                "  - Share from (Serialized): {:?}, (Debug): {:?}\n    - In Package Commitments? \
                 : {}\n    - In PubKey Verifying Shares? : {}",
                id.serialize(),
                id,
                package_commitments_map.contains_key(id),
                pubkey_verifying_shares_map.contains_key(id),
            );
        }

        frost::aggregate(
            seed_signing_package,
            &valid_shares_for_aggregation,
            &self.group_public_key_package,
        )
        .map_err(|e| HbbftError::InternalError(format!("FROST aggregate failed for seed: {e:?}")))
    }
}

impl<NodeId: NodeIdentifier> CommonCoin for SeedBasedFrostCoin<NodeId> {
    type NodeId = NodeId;

    fn request_coin_value(&mut self, epoch: EpochNum, round: AbaRound) -> CoinOutput {
        let hbbft_epoch_round_key = (epoch, round);

        // Return cached decision (from fallback or prior seed-derived coin), if any
        if let Some(&value) = self
            .reconstructed_derived_coins
            .get(&hbbft_epoch_round_key)
        {
            return CoinOutput::with_value(value);
        }

        match &self.seed_generation_state {
            SeedGenerationState::SeedGenerated(seed) => {
                // SEED is already generated, just derive the coin.
                if let Some(&value) = self
                    .reconstructed_derived_coins
                    .get(&hbbft_epoch_round_key)
                {
                    return CoinOutput::with_value(value);
                }
                let value = Self::derive_coin_from_seed(seed, epoch, round);
                self.reconstructed_derived_coins
                    .insert(hbbft_epoch_round_key, value);
                CoinOutput::with_value(value)
            }
            SeedGenerationState::NotStarted => {
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::NotStarted. Generating seed commitments.",
                    self.hbbft_own_id
                );
                let (nonces, commitments) = self.generate_own_seed_commitments();
                let mut received_commitments = BTreeMap::new();
                received_commitments.insert(self.frost_own_identifier, commitments);

                self.seed_generation_state = SeedGenerationState::AwaitingSeedCommitments {
                    own_seed_nonces: Box::new(nonces),
                    own_seed_commitments: Box::new(commitments),
                    received_seed_commitments: received_commitments,
                };
                // Share to send is our FROST Round 1 commitment for the SEED.
                // Serialize hiding and binding commitments
                match (commitments.hiding().serialize(), commitments.binding().serialize()) {
                    (Ok(h_bytes), Ok(b_bytes)) => {
                        let combined_bytes: CoinShare = [h_bytes, b_bytes].concat();
                        CoinOutput::with_share(combined_bytes)
                    }
                    _ => {
                        eprintln!("[SeedCoin] Failed to serialize own initial seed commitments.");
                        self.seed_generation_state = SeedGenerationState::SeedGenerationFailed;
                        CoinOutput::empty()
                    }
                }
            }
            SeedGenerationState::AwaitingSeedCommitments {
                own_seed_commitments, // Already sent this one
                received_seed_commitments,
                ..
            } => {
                // We've sent our commitment, waiting for others for the SEED.
                // If enough commitments received, transition to AwaitingSeedShares.
                // This transition will actually happen in submit_coin_share.
                // HBBFT expects us to return our share if we have one. If we're just
                // awaiting, it means we already provided it.
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::AwaitingSeedCommitments ({} collected of {} \
                     needed from {} total participants for seed). Returning own commitment again \
                     if requested.",
                    self.hbbft_own_id,
                    received_seed_commitments.len(),
                    self.min_signers_for_seed,
                    self.seed_participants.len()
                );
                // Return our commitment share again if HBBFT asks during this phase.
                // Serialize hiding and binding commitments
                match (
                    own_seed_commitments
                        .hiding()
                        .serialize(),
                    own_seed_commitments
                        .binding()
                        .serialize(),
                ) {
                    (Ok(h_bytes), Ok(b_bytes)) => {
                        let combined_bytes: CoinShare = [h_bytes, b_bytes].concat();
                        CoinOutput::with_share(combined_bytes)
                    }
                    _ => {
                        eprintln!("[SeedCoin] Failed to re-serialize own seed commitments.");
                        // Don't change state here, just fail to provide the share this time.
                        // If it's a persistent serialization issue, SeedGenerationFailed will be
                        // hit eventually.
                        CoinOutput::empty()
                    }
                }
            }
            SeedGenerationState::AwaitingSeedShares {
                own_seed_share_bytes, // This is the FROST Round 2 share for the SEED
                received_seed_shares,
                ..
            } => {
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::AwaitingSeedShares ({} collected of {} needed). \
                     Returning own share if available.",
                    self.hbbft_own_id,
                    received_seed_shares.len(),
                    self.min_signers_for_seed
                );
                // We are in FROST Round 2 for the SEED. Return our own share.
                // The actual aggregation to get the SEED will happen in submit_coin_share.
                CoinOutput::with_share(own_seed_share_bytes.clone())
            }
            SeedGenerationState::SeedGenerationFailed => {
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::SeedGenerationFailed. Cannot proceed.",
                    self.hbbft_own_id
                );
                // Seed generation failed, can't produce coins cryptographically.
                // Fallback will be used by HBBFT if it calls get_coin_for_round_fallback.
                CoinOutput::empty()
            }
        }
    }

    fn submit_coin_share(
        &mut self,
        epoch: EpochNum, // HBBFT epoch for a derived coin
        round: AbaRound, // HBBFT round for a derived coin
        share_content: CoinShare,
        sender_hbbft_id: Self::NodeId,
    ) -> CoinOutput {
        let hbbft_epoch_round_key = (epoch, round);

        // If we already have a cached decision (fallback or derived), return it.
        if let Some(&value) = self
            .reconstructed_derived_coins
            .get(&hbbft_epoch_round_key)
        {
            return CoinOutput::with_value(value);
        }

        let sender_frost_id = match self
            .node_id_to_frost_id
            .get(&sender_hbbft_id)
        {
            Some(id) => *id,
            None => {
                eprintln!(
                    "[SeedCoin] Received share from unknown HBBFT NodeId for SEED gen: \
                     {sender_hbbft_id:?}",
                );
                return CoinOutput::empty();
            }
        };

        // If seed is already generated, just derive the coin for the HBBFT request
        if let SeedGenerationState::SeedGenerated(seed) = &self.seed_generation_state {
            if let Some(&value) = self
                .reconstructed_derived_coins
                .get(&hbbft_epoch_round_key)
            {
                return CoinOutput::with_value(value);
            }
            let value = Self::derive_coin_from_seed(seed, epoch, round);
            self.reconstructed_derived_coins
                .insert(hbbft_epoch_round_key, value);
            return CoinOutput::with_value(value);
        }
        if matches!(self.seed_generation_state, SeedGenerationState::SeedGenerationFailed) {
            return CoinOutput::empty(); // Fallback will be used by HBBFT
        }

        // --- Logic for SEED Generation Phases ---
        let current_state = self.seed_generation_state.clone(); // Clone to work on, then replace self.
        match current_state {
            SeedGenerationState::NotStarted => {
                // This shouldn't happen if request_coin_value was called first by this node.
                // If another node's share arrives before this node calls request_coin_value.
                eprintln!(
                    "[SeedCoin] Node {}: Received share while in SeedGen::NotStarted. This is \
                     unexpected but attempting to process as commitment.",
                    self.hbbft_own_id
                );
                // Treat it as a commitment and try to kickstart our own process.
                // This part is tricky to handle perfectly without a more explicit state machine
                // initiator. For now, let's assume request_coin_value is called
                // first on self.
                self.seed_generation_state = SeedGenerationState::SeedGenerationFailed; // Mark as failed due to odd state
                return CoinOutput::empty();
            }
            SeedGenerationState::AwaitingSeedCommitments {
                own_seed_nonces,
                own_seed_commitments,
                mut received_seed_commitments,
            } => {
                if !self
                    .seed_participants
                    .contains(&sender_frost_id)
                {
                    eprintln!(
                        "[SeedCoin] Received SEED commitment from non-participant FROST ID: \
                         {sender_frost_id:?}",
                    );
                    return CoinOutput::empty(); // Do not update state based on non-participant
                }

                // For secp256k1, a compressed point (NonceCommitment) is 33 bytes.
                const COMMITMENT_LEN: usize = 33;

                // This arm expects a commitment share (Round 1 for SEED)
                if share_content.len() != 2 * COMMITMENT_LEN {
                    eprintln!(
                        "[SeedCoin] State AwaitingSeedCommitments: Received share with unexpected \
                         length {} for a commitment (expected {}). Sender: {:?}",
                        share_content.len(),
                        2 * COMMITMENT_LEN,
                        sender_frost_id
                    );
                    return CoinOutput::empty();
                }
                // Deserialize commitment
                if share_content.len() != 2 * COMMITMENT_LEN {
                    eprintln!(
                        "[SeedCoin] Invalid SEED commitment length: got {}, expected {}",
                        share_content.len(),
                        2 * COMMITMENT_LEN
                    );
                    return CoinOutput::empty();
                }
                let hiding_bytes: [u8; COMMITMENT_LEN] = share_content[0..COMMITMENT_LEN]
                    .try_into()
                    .map_err(|_| {
                        eprintln!(
                            "[SeedCoin] Failed to slice hiding commitment bytes from \
                             share_content (len {})",
                            share_content.len()
                        );
                    })
                    .unwrap_or_else(|_| {
                        // This panic is bad for library code, should return CoinOutput::empty()
                        // For now, keeping it to highlight the error during testing.
                        panic!("Slice error for hiding commitment");
                    });
                let binding_bytes: [u8; COMMITMENT_LEN] = share_content[COMMITMENT_LEN..]
                    .try_into()
                    .map_err(|_| {
                        eprintln!(
                            "[SeedCoin] Failed to slice binding commitment bytes from \
                             share_content (len {})",
                            share_content.len()
                        );
                    })
                    .unwrap_or_else(|_| {
                        panic!("Slice error for binding commitment");
                    });

                let hiding_commitment = match round1::NonceCommitment::deserialize(&hiding_bytes) {
                    Ok(hc) => hc,
                    Err(e) => {
                        eprintln!("[SeedCoin] Failed to deserialize hiding commitment: {e:?}");
                        // Optionally, could mark sender as misbehaving.
                        return CoinOutput::empty();
                    }
                };
                let binding_commitment = match round1::NonceCommitment::deserialize(&binding_bytes)
                {
                    Ok(bc) => bc,
                    Err(e) => {
                        eprintln!("[SeedCoin] Failed to deserialize binding commitment: {e:?}");
                        // Optionally, could mark sender as misbehaving.
                        return CoinOutput::empty();
                    }
                };
                let commitment =
                    round1::SigningCommitments::new(hiding_commitment, binding_commitment);

                received_seed_commitments.insert(sender_frost_id, commitment);
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::AwaitingSeedCommitments received from {:?}. \
                     Total commitments: {} of {} needed from {} total participants for seed.",
                    self.hbbft_own_id,
                    sender_frost_id,
                    received_seed_commitments.len(),
                    self.min_signers_for_seed,
                    self.seed_participants.len()
                );

                // Ensure our own commitment is recorded:
                received_seed_commitments
                    .entry(self.frost_own_identifier)
                    .or_insert(*own_seed_commitments);

                // Canonical signer set: first `t` in the globally sorted participant list.
                let t = self.min_signers_for_seed as usize;
                let canonical_signers: Vec<FrostIdentifier> = self
                    .seed_participants
                    .iter()
                    .copied()
                    .take(t)
                    .collect();

                // Only proceed when we have commitments from all canonical signers.
                let have_all_canonical = canonical_signers
                    .iter()
                    .all(|pid| received_seed_commitments.contains_key(pid));

                if have_all_canonical {
                    // Build package from canonical set only (identical on all nodes).
                    let mut valid_commitments_for_package = BTreeMap::new();
                    for pid in &canonical_signers {
                        valid_commitments_for_package.insert(
                            *pid,
                            *received_seed_commitments
                                .get(pid)
                                .expect("commitment present"),
                        );
                    }

                    eprintln!(
                        "[SeedCoin] Node {}: Forming SEED SigningPackage with canonical \
                         participants: {:?}",
                        self.hbbft_own_id, canonical_signers
                    );

                    let seed_signing_package =
                        Self::get_seed_signing_package(&valid_commitments_for_package);

                    if seed_signing_package
                        .signing_commitments()
                        .len()
                        != t
                    {
                        eprintln!(
                            "[SeedCoin] Node {}: SigningPackage has unexpected number of \
                             commitments: got {}, expected {}",
                            self.hbbft_own_id,
                            seed_signing_package
                                .signing_commitments()
                                .len(),
                            t
                        );
                        self.seed_generation_state = SeedGenerationState::SeedGenerationFailed;
                        return CoinOutput::empty();
                    }

                    // If we are in the canonical signer set, generate our Round 2 share.
                    let mut own_seed_share_bytes: Option<Vec<u8>> = None;
                    let mut received_seed_shares = BTreeMap::new();

                    if canonical_signers.contains(&self.frost_own_identifier) {
                        match self.generate_own_seed_share(&seed_signing_package, &own_seed_nonces)
                        {
                            Ok(own_share) => {
                                own_seed_share_bytes = Some(own_share.serialize().to_vec());
                                received_seed_shares.insert(self.frost_own_identifier, own_share);
                            }
                            Err(e) => {
                                eprintln!(
                                    "[SeedCoin] Node {}: Failed to generate own seed share: {:?}. \
                                     Failing seed gen.",
                                    self.hbbft_own_id, e
                                );
                                self.seed_generation_state =
                                    SeedGenerationState::SeedGenerationFailed;
                                return CoinOutput::empty();
                            }
                        }
                    } else {
                        eprintln!(
                            "[SeedCoin] Node {}: Not in canonical signer set; will not produce a \
                             share but can aggregate.",
                            self.hbbft_own_id
                        );
                    }

                    self.seed_generation_state = SeedGenerationState::AwaitingSeedShares {
                        seed_signing_package,
                        actual_seed_package_participants: canonical_signers,
                        own_seed_nonces,
                        own_seed_share_bytes,
                        received_seed_shares,
                    };

                    if let SeedGenerationState::AwaitingSeedShares {
                        own_seed_share_bytes: Some(share_bytes),
                        ..
                    } = &self.seed_generation_state
                    {
                        return CoinOutput::with_share(share_bytes.clone());
                    } else {
                        return CoinOutput::empty();
                    }
                } else {
                    // Not enough canonical commitments yet; keep waiting.
                    self.seed_generation_state = SeedGenerationState::AwaitingSeedCommitments {
                        own_seed_nonces,
                        own_seed_commitments,
                        received_seed_commitments,
                    };
                }
                return CoinOutput::empty();
            }
            SeedGenerationState::AwaitingSeedShares {
                seed_signing_package,
                actual_seed_package_participants,
                own_seed_nonces,
                own_seed_share_bytes,
                mut received_seed_shares,
            } => {
                // Only accept shares from the selected participants in this signing package.
                if !actual_seed_package_participants.contains(&sender_frost_id) {
                    eprintln!(
                        "[SeedCoin] Ignoring SEED share from {:?}: not in active signing set {:?}",
                        sender_frost_id, actual_seed_package_participants
                    );
                    return CoinOutput::empty();
                }

                // This arm expects a signature share (Round 2 for SEED)
                // For secp256k1, a signature share (scalar) is 32 bytes.
                if share_content.len() != 32 {
                    eprintln!(
                        "[SeedCoin] State AwaitingSeedShares: Received share with unexpected \
                         length {} for a signature share (expected 32). Sender: {:?}",
                        share_content.len(),
                        sender_frost_id
                    );
                    return CoinOutput::empty();
                }

                let sig_share = match round2::SignatureShare::deserialize(&share_content) {
                    Ok(s) => s, // share_content is Vec<u8>, deserialize expects &[u8]
                    Err(e) => {
                        eprintln!("[SeedCoin] Failed to deserialize SEED share: {e:?}");
                        return CoinOutput::empty();
                    }
                };
                received_seed_shares.insert(sender_frost_id, sig_share);
                eprintln!(
                    "[SeedCoin] Node {}: SeedGen::AwaitingSeedShares received from \
                     {sender_frost_id:?}. Total shares: {} of {} needed.",
                    self.hbbft_own_id,
                    received_seed_shares.len(),
                    self.min_signers_for_seed
                );

                if received_seed_shares.len() >= self.min_signers_for_seed as usize {
                    eprintln!(
                        "[SeedCoin] Node {}: Enough SEED shares received. Attempting to aggregate \
                         SEED signature.",
                        self.hbbft_own_id
                    );
                    match self.aggregate_seed_signature(
                        &seed_signing_package,
                        &received_seed_shares, // , &actual_seed_package_participants
                    ) {
                        Ok(seed_signature) => {
                            let serialized_seed_sig = match seed_signature.serialize() {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    eprintln!(
                                        "[SeedCoin] Failed to serialize final SEED signature: \
                                         {e:?}"
                                    );
                                    self.seed_generation_state =
                                        SeedGenerationState::SeedGenerationFailed;
                                    return CoinOutput::empty();
                                }
                            };
                            let mut hasher = Sha256::new();
                            hasher.update(&serialized_seed_sig);
                            let seed_value: [u8; 32] = hasher.finalize().into();
                            let shared_seed = Arc::new(seed_value);
                            eprintln!(
                                "[SeedCoin] Node {}: SEED Generated: {shared_seed:?}",
                                self.hbbft_own_id,
                            );
                            self.seed_generation_state =
                                SeedGenerationState::SeedGenerated(Arc::clone(&shared_seed));

                            // Now that SEED is generated, derive the coin for the original HBBFT
                            // request
                            let derived_coin_value =
                                Self::derive_coin_from_seed(&shared_seed, epoch, round);
                            self.reconstructed_derived_coins
                                .insert(hbbft_epoch_round_key, derived_coin_value);
                            return CoinOutput::with_value(derived_coin_value);
                        }
                        Err(e) => {
                            eprintln!(
                                "[SeedCoin] Node {}: SEED aggregation failed: {e:?}. Failing seed \
                                 gen.",
                                self.hbbft_own_id
                            );
                            self.seed_generation_state = SeedGenerationState::SeedGenerationFailed;
                            return CoinOutput::empty();
                        }
                    }
                } else {
                    // Not enough shares for seed yet
                    self.seed_generation_state = SeedGenerationState::AwaitingSeedShares {
                        seed_signing_package,
                        actual_seed_package_participants,
                        own_seed_nonces,
                        own_seed_share_bytes,
                        received_seed_shares,
                    };
                }
                return CoinOutput::empty(); // No coin decided yet
            }
            _ => { /* SeedGenerated or SeedGenerationFailed, already handled at top */ }
        }
        CoinOutput::empty()
    }
}

type FrostSeedCoinSetup = (
    BTreeMap<FrostIdentifier, KeyPackage>, // All key packages for FROST participants
    PublicKeyPackage,                      // Group public key package
    Arc<HashMap<DemoNodeId, FrostIdentifier>>, // Map from HBBFT NodeId to FROST Identifier
    Vec<FrostIdentifier>,                  // Sorted list of all FROST participant identifiers
);

/// Helper to setup FROST participants, adapted for Arc sharing and full
/// participant list.
pub fn setup_frost_for_seed_coin(
    num_total_nodes: u16, // N
    seed_threshold: u16,  // t_seed (e.g., N-F+1 or F+1 Robust (N-F for us))
) -> FrostSeedCoinSetup {
    let mut rng = ChaCha20Rng::from_seed([0u8; 32]);
    let mut hbbft_node_id_to_frost_id_map = HashMap::new();
    let frost_identifiers_sorted: Vec<FrostIdentifier> = (1..=num_total_nodes)
        .map(|i| {
            let frost_id = FrostIdentifier::try_from(i).unwrap();
            // Assuming DemoNodeId(0) maps to FrostIdentifier(1), etc.
            hbbft_node_id_to_frost_id_map.insert(DemoNodeId((i - 1) as usize), frost_id);
            frost_id
        })
        .collect();

    let (shares, group_pubkey_pkg) = frost::keys::generate_with_dealer(
        num_total_nodes,
        seed_threshold, // Use the specific threshold for seed generation
        frost::keys::IdentifierList::Custom(&frost_identifiers_sorted), // Provide all N identifiers
        &mut rng,
    )
    .unwrap();

    let key_packages: BTreeMap<FrostIdentifier, KeyPackage> = shares
        .into_iter()
        .map(|(id, secret_share)| {
            let verifying_share_val = *group_pubkey_pkg
                .verifying_shares()
                .get(&id)
                .unwrap();
            (
                id,
                frost::keys::KeyPackage::new(
                    id,
                    *secret_share.signing_share(),
                    verifying_share_val,
                    *group_pubkey_pkg.verifying_key(),
                    seed_threshold, // Store the threshold it was generated with
                ),
            )
        })
        .collect();
    (
        key_packages,
        group_pubkey_pkg,
        Arc::new(hbbft_node_id_to_frost_id_map),
        frost_identifiers_sorted,
    )
}

// --- Scenarios (Illustrative - will need adaptation for new Coin type) ---

/// Scenario: "seeded-integration"
///
/// End-to-end demonstration of the SEED-based coin:
/// - Round 1: all N nodes broadcast FROST commitments for the fixed SEED
///   message
/// - Canonical signer set (size t = N - F) is selected deterministically
/// - Round 2: only canonical signers produce signature shares; others aggregate
/// - SEED is produced once any node collects t valid shares
/// - Subsequent (epoch, round) coins are derived from SEED without more signing
///
/// Run:
/// - `cargo run -- seeded-integration`
/// - `just demo seeded-integration`
///
/// Expected:
/// - Nodes log formation of the canonical signing package
/// - Non-canonical shares are ignored with a clear log
/// - SEED bytes identical on all nodes; derived coin matches across nodes
/// - Later derived coins require no new shares
pub fn run_seed_coin_integration_scenario() {
    println!("\n--- Running Seed-Based Coin Integration Scenario ---");
    let n_hbbft: usize = 4;
    let f_hbbft: usize = 1;
    // For SEED generation, use a robust threshold, e.g., N-F. Here, 4-1 = 3
    // HBBFT's "low_threshold" (F+1) is used for ABA round decisions, not related to
    // seed strength directly.
    let seed_generation_threshold: u16 = (n_hbbft - f_hbbft) as u16;
    if seed_generation_threshold < 2 {
        // FROST needs at least 2
        panic!(
            "Seed generation threshold too low for FROST (needs >=2). N-F resulted in \
             {seed_generation_threshold}",
        );
    }

    let hbbft_node_ids: Vec<DemoNodeId> = (0..n_hbbft).map(DemoNodeId).collect();

    let (
        all_key_packages_frost,
        group_pk_pkg_frost,
        hbbft_to_frost_map_arc,
        all_frost_participants_sorted, // Needed for SeedBasedFrostCoin::new
    ) = setup_frost_for_seed_coin(n_hbbft as u16, seed_generation_threshold);

    // Canonical signer set (first t FROST ids, identical on all nodes)
    let canonical_signers: Vec<FrostIdentifier> = all_frost_participants_sorted
        .iter()
        .copied()
        .take(seed_generation_threshold as usize)
        .collect();
    let canonical_signers_set: std::collections::BTreeSet<FrostIdentifier> = canonical_signers
        .iter()
        .copied()
        .collect();

    // Share these structures among coin instances via Arc
    let shared_all_key_packages = Arc::new(all_key_packages_frost);
    let shared_group_pk = Arc::new(group_pk_pkg_frost);
    // hbbft_to_frost_map_arc is already Arc
    let shared_all_frost_participants = Arc::new(all_frost_participants_sorted);

    let shared_group_pk_clone = Arc::clone(&shared_group_pk);
    let shared_all_frost_participants_clone = Arc::clone(&shared_all_frost_participants);
    let hbbft_to_frost_map_clone = Arc::clone(&hbbft_to_frost_map_arc);
    let shared_all_key_packages_clone = Arc::clone(&shared_all_key_packages);
    let hbbft_to_frost_map_arc_clone = Arc::clone(&hbbft_to_frost_map_arc);
    let coin_factory_fn = Arc::new(
        move |hbbft_node_id: DemoNodeId,
              _epoch: EpochNum,
              config_for_coin: Arc<HoneyBadgerConfig<DemoNodeId>>| {
            let local_frost_id = *hbbft_to_frost_map_clone
                .get(&hbbft_node_id)
                .expect("HBBFT ID not in FROST map for factory (seed coin)");
            let local_key_pkg = shared_all_key_packages_clone
                .get(&local_frost_id)
                .expect("FROST ID not in key packages for factory (seed coin)")
                .clone();

            Box::new(SeedBasedFrostCoin::new(
                config_for_coin,
                hbbft_node_id,
                local_frost_id,
                local_key_pkg,
                (*shared_group_pk_clone).clone(),
                hbbft_to_frost_map_arc_clone.clone(),
                (*shared_all_frost_participants_clone).clone(),
            )) as Box<dyn CommonCoin<NodeId = DemoNodeId>>
        },
    );
    let seed_coin_factory = CoinFactory::new(coin_factory_fn);

    // Create HBBFT configurations
    let mut hbbft_configs = Vec::new();
    for node_id in &hbbft_node_ids {
        hbbft_configs.push(create_test_config_with_node_ids(
            f_hbbft,
            hbbft_node_ids.clone(),
            *node_id,
            seed_coin_factory.clone(),
        ));
    }

    // Create Coin Instances directly for interaction
    // We need 'N' coin instances for the full seed generation ceremony.
    let mut coins: Vec<SeedBasedFrostCoin<DemoNodeId>> = Vec::new();
    for i in 0..n_hbbft {
        let node_id = hbbft_node_ids[i];
        let frost_id = *hbbft_to_frost_map_arc
            .get(&node_id)
            .unwrap();
        let key_pkg = shared_all_key_packages
            .get(&frost_id)
            .unwrap()
            .clone();
        coins.push(SeedBasedFrostCoin::new(
            hbbft_configs[i].clone(),
            node_id,
            frost_id,
            key_pkg,
            (*shared_group_pk).clone(),
            hbbft_to_frost_map_arc.clone(),
            (*shared_all_frost_participants).clone(),
        ));
    }

    let epoch_seed_gen: EpochNum = 0; // Epoch for HBBFT when seed gen happens
    let round_seed_gen: AbaRound = 0; // Round for HBBFT when seed gen happens

    println!("--- Starting SEED Generation Phase ---");
    // Round 1: All N nodes request, producing their commitments for the SEED
    let mut seed_commitments_shares: Vec<CoinShare> = Vec::new();
    for (i, coin) in coins
        .iter_mut()
        .enumerate()
        .take(n_hbbft)
    {
        println!("[Scenario] Node {i} requesting (triggers seed commitment)...");
        let output = coin.request_coin_value(epoch_seed_gen, round_seed_gen);
        assert!(
            output.share_to_broadcast.is_some(),
            "Node {i} should produce a SEED commitment share"
        );
        assert!(output.decided_value.is_none(), "SEED should not be generated yet by Node {i}");
        seed_commitments_shares.push(output.share_to_broadcast.unwrap());
    }
    println!("[Scenario] All nodes produced SEED commitment shares.");

    // Simulate broadcast of commitments: each node submits every other node's
    // commitment
    for (i, coin) in coins
        .iter_mut()
        .enumerate()
        .take(n_hbbft)
    {
        let mut round_1_output_for_node_i = CoinOutput::empty();
        // Submitting to coin[i]
        for j in 0..n_hbbft {
            // Share from coin[j]
            if i == j {
                continue;
            } // Don't submit its own share to itself via this
            println!("[Scenario] Node {i} submitting Node {j}'s SEED commitment share...");
            let output = coin.submit_coin_share(
                epoch_seed_gen,
                round_seed_gen, // These params are for the derived coin HBBFT wants
                seed_commitments_shares[j].clone(),
                hbbft_node_ids[j],
            );
            // If submitting a commitment caused this node to generate its Round 2 share,
            // capture it for the next phase of the test.
            if output.share_to_broadcast.is_some() {
                // This node has now transitioned to AwaitingSeedShares and produced its R2
                // share
                round_1_output_for_node_i = output;
            }
        }
        // If after receiving all other commitments, this node produced its R2 share,
        // store it.
        if let Some(_r2_share) = round_1_output_for_node_i.share_to_broadcast {
            // This is node i's own R2 share, update seed_signature_shares[i]
            // This assumes the test's seed_signature_shares was pre-filled or
            // can be updated. For simplicity, let's assume the
            // request_coin_value in the next block will get it.
        }
    }
    println!("[Scenario] All SEED commitments exchanged.");
    // At this point, after all commitments are submitted, each coin instance that
    // is a seed_participant should have transitioned to AwaitingSeedShares and
    // generated its own Round 2 share for the SEED.

    // Round 2: All N nodes request again, this time they should produce their
    // *signature shares* for the SEED
    // This call is crucial as it allows nodes that transitioned to
    // AwaitingSeedShares to output their R2 share.
    let mut seed_signature_shares: Vec<CoinShare> = Vec::new();
    for (i, coin) in coins
        .iter_mut()
        .enumerate()
        .take(n_hbbft)
    {
        println!("[Scenario] Node {i} requesting (triggers seed signature share)...");
        let output = coin.request_coin_value(epoch_seed_gen, round_seed_gen);

        // Only canonical signers must produce a 32-byte share
        let frost_id_i = *hbbft_to_frost_map_arc
            .get(&hbbft_node_ids[i])
            .unwrap();
        if canonical_signers_set.contains(&frost_id_i) {
            assert!(
                output.share_to_broadcast.is_some(),
                "Node {i} (signer) should produce a SEED signature share"
            );
            let share = output
                .share_to_broadcast
                .clone()
                .unwrap();
            assert_eq!(share.len(), 32, "Share from signer node {i} should be 32 bytes");
            seed_signature_shares.push(share);
        } else {
            // Non-signers may not produce a share
            assert!(
                output.share_to_broadcast.is_none(),
                "Node {i} (non-signer) should not produce a SEED signature share"
            );
        }
        assert!(
            output.decided_value.is_none(),
            "SEED should not be generated yet by Node {i} after only its own (or no) share"
        );
    }
    // Verify that all shares collected now are indeed signature shares (32 bytes)
    for (idx, share) in seed_signature_shares.iter().enumerate() {
        assert_eq!(
            share.len(),
            32,
            "Share from node {} in R2 should be 32 bytes (signature share), got {}",
            idx,
            share.len()
        );
    }

    println!("[Scenario] All nodes produced SEED signature shares.");

    // Simulate broadcast of signature shares: each node submits other N-1 shares
    // We only need seed_generation_threshold shares to complete.
    // The first node to receive enough should generate the seed.
    let mut final_seed_derived_coin: Option<bool> = None;

    for (i, coin) in coins
        .iter_mut()
        .enumerate()
        .take(n_hbbft)
    {
        // Submitting to coin[i]
        // Submit enough shares to coin[i] to potentially complete the SEED
        let mut shares_submitted_count = 0;
        for (j, peer_id) in hbbft_node_ids
            .iter()
            .enumerate()
            .take(n_hbbft)
        {
            if i == j {
                continue;
            }
            // Only broadcast shares from canonical signers
            let frost_id_j = *hbbft_to_frost_map_arc
                .get(peer_id)
                .unwrap();
            if !canonical_signers_set.contains(&frost_id_j) {
                continue;
            }
            println!("[Scenario] Node {i} submitting Node {j}'s SEED signature share...");
            // The index into seed_signature_shares corresponds to signer order in
            // canonical_signers
            let signer_pos = canonical_signers
                .iter()
                .position(|id| *id == frost_id_j)
                .expect("signer frost id must be in canonical set");
            let share = seed_signature_shares[signer_pos].clone();

            let output = coin.submit_coin_share(epoch_seed_gen, round_seed_gen, share, *peer_id);
            shares_submitted_count += 1;

            if let Some(val) = output.decided_value {
                println!("[Scenario] Node {i} generated SEED and derived coin value: {val}!");
                if final_seed_derived_coin.is_none() {
                    final_seed_derived_coin = Some(val);
                } else {
                    assert_eq!(
                        final_seed_derived_coin,
                        Some(val),
                        "All nodes should derive the same coin"
                    );
                }
            }
            if shares_submitted_count >= seed_generation_threshold as usize
                && final_seed_derived_coin.is_some()
            {
                // If this node decided earlier, or threshold met, potentially
                // break if we only want one decider test
            }
        }
        // After node i received all other shares, if it hasn't decided, it should now.
        // Or if it decided earlier, this call should just return the cached derived
        // coin.
        let output = coin.request_coin_value(epoch_seed_gen, round_seed_gen);
        if let Some(val) = output.decided_value {
            if final_seed_derived_coin.is_none() {
                // Should not happen if threshold logic correct
                final_seed_derived_coin = Some(val);
                println!(
                    "[Scenario] Node {i} (after all shares submitted to it) derived coin value: \
                     {val}!"
                );
            }
            assert_eq!(final_seed_derived_coin, Some(val), "Node {i} final derived coin mismatch",);
        } else {
            // This would be an error if we expect all nodes to eventually have the seed
            // if seed_generation_threshold shares were broadcast.
            panic!("Node {i} did not decide on a coin value after all shares exchanged!");
        }
    }
    assert!(
        final_seed_derived_coin.is_some(),
        "SEED generation should have completed and a coin derived."
    );
    println!(
        "--- SEED Generation Phase Complete. Final derived coin for \
         ({epoch_seed_gen},{round_seed_gen}): {} ---",
        final_seed_derived_coin.unwrap()
    );

    println!("\n--- Testing Derived Coin (epoch 10, round 5) AFTER SEED ---");
    let epoch_derived: EpochNum = 10;
    let round_derived: AbaRound = 5;
    let mut derived_coins_map = HashMap::new();

    for i in 0..n_hbbft {
        let output = coins[i].request_coin_value(epoch_derived, round_derived);
        assert!(
            output.share_to_broadcast.is_none(),
            "No share should be produced for derived coin by Node {i}"
        );
        assert!(output.decided_value.is_some(), "Derived coin should be available for Node {i}");
        derived_coins_map.insert(hbbft_node_ids[i], output.decided_value.unwrap());
        println!(
            "[Scenario] Node {i} derived coin for ({epoch_derived},{round_derived}): {}",
            output.decided_value.unwrap()
        );
    }

    let first_derived_val = derived_coins_map[&hbbft_node_ids[0]];
    for i in 1..n_hbbft {
        assert_eq!(
            derived_coins_map[&hbbft_node_ids[i]], first_derived_val,
            "All nodes must derive the same coin value from the SEED"
        );
    }
    println!(
        "SUCCESS: All nodes derived the same coin value for ({epoch_derived},{round_derived}): \
         {first_derived_val}",
    );
}
