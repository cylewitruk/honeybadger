//! Demonstrates a `CommonCoin` implementation using FROST (frost-secp256k1)
//! for the `honeybadger` BFT library.
//!
//! ## Important Simplification for this Demonstration:
//!
//! FROST is typically a two-round protocol. This example simplifies this by
//! assuming deterministic commitments for FROST's round 1, avoiding explicit
//! network exchange of these commitments within the `CommonCoin` flow.
//!
//! ## Running the Demonstration:
//!
//! If `honeybadger-demo` is a binary crate with a `src/main.rs` that calls
//! `honeybadger_demo::run_cli_scenarios()`, you can run it via:
//!
//! ```bash
//! cargo run -- [SCENARIO_NAME]
//! ```
//!
//! Available scenarios:
//! - `integration`: Demonstrates the basic request, submit, and aggregation
//!   flow.
//! - `fallback`: Demonstrates the coin fallback logic when not enough shares
//!   are received.

mod frost_seed;

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Arc;

// --- FROST and related imports ---
use frost_secp256k1::{
    self as frost,
    Identifier as FrostIdentifier, // Corrected import
    Signature,
    SigningPackage,
    keys::KeyPackage,
    keys::PublicKeyPackage,
    keys::SigningShare,
    rand_core::SeedableRng as _,
    // VerifyingShare, // Not directly used in FrostThresholdCoin struct fields
    round1,
    round2,
};
use honeybadger::{
    AbaRound, CoinOutput, CoinShare, EpochNum, HbbftError, HoneyBadgerConfig,
    HoneyBadgerConfigBuilder, NodeIdentifier,
};
// Assuming the main honeybadger library is a dependency
use honeybadger::{CoinFactory, CommonCoin};
use rand_chacha::ChaCha20Rng;
use sha2::{Digest, Sha256};
// No direct `secp256k1` crate import needed

/// Module for testing utilities, kept internal to this demonstration library.
mod testing_utils {
    use std::collections::BTreeSet;

    use honeybadger::CoinFactory;

    use super::*; // To get NodeIdentifier, HoneyBadgerConfigBuilder etc.

    /// A simple NodeId type for testing and demonstration.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    pub struct DemoNodeId(pub usize);

    impl std::fmt::Display for DemoNodeId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Node({})", self.0)
        }
    }
    impl NodeIdentifier for DemoNodeId {}

    /// Creates a test configuration for the HoneyBadger library.
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
use testing_utils::*; // Make testing utils available in this lib's scope

/// An example `CommonCoin` implementation using FROST (frost-secp256k1).
#[derive(Debug)]
pub struct FrostThresholdCoin<NodeId: NodeIdentifier> {
    #[allow(unused)] // Example
    config: Arc<HoneyBadgerConfig<NodeId>>,
    #[allow(unused)] // Example
    hbbft_own_id: NodeId,

    frost_own_identifier: FrostIdentifier,
    key_package: KeyPackage,
    group_public_key_package: PublicKeyPackage,

    min_signers: u16,
    all_signing_shares: BTreeMap<FrostIdentifier, SigningShare>,
    coin_participants: Vec<FrostIdentifier>,

    received_sig_shares:
        HashMap<(EpochNum, AbaRound), BTreeMap<FrostIdentifier, frost::round2::SignatureShare>>,
    reconstructed_coins: HashMap<(EpochNum, AbaRound), bool>,
    own_signing_nonces: HashMap<(EpochNum, AbaRound), round1::SigningNonces>,
    node_id_to_frost_id: HashMap<NodeId, FrostIdentifier>,
}

fn derive_deterministic_seed(
    purpose: &str,
    epoch: EpochNum,
    round: AbaRound,
    id: &FrostIdentifier,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(purpose.as_bytes());
    hasher.update(epoch.to_le_bytes());
    hasher.update(round.to_le_bytes());
    hasher.update(id.serialize());
    hasher.finalize().into()
}

impl<NodeId: NodeIdentifier> FrostThresholdCoin<NodeId> {
    /// Creates a new `FrostThresholdCoin`.
    pub fn new(
        config: Arc<HoneyBadgerConfig<NodeId>>,
        hbbft_own_id: NodeId,
        frost_own_identifier: FrostIdentifier,
        key_package: KeyPackage,
        group_public_key_package: PublicKeyPackage,
        all_participants_key_packages: BTreeMap<FrostIdentifier, KeyPackage>,
        node_id_to_frost_id: HashMap<NodeId, FrostIdentifier>,
    ) -> Self {
        let min_signers =
            u16::try_from(config.low_threshold()).expect("F+1 threshold too large for u16");
        if min_signers == 0 {
            panic!("min_signers (F+1) cannot be zero for FROST");
        }

        let mut sorted_frost_ids: Vec<FrostIdentifier> = all_participants_key_packages
            .keys()
            .cloned()
            .collect();
        sorted_frost_ids.sort();
        let coin_participants: Vec<FrostIdentifier> = sorted_frost_ids
            .into_iter()
            .take(min_signers as usize)
            .collect();

        if coin_participants.len() < min_signers as usize {
            panic!(
                "Not enough total participants ({}) to select {min_signers} coin participants.",
                all_participants_key_packages.len(),
            );
        }

        let all_signing_shares = all_participants_key_packages
            .into_iter()
            .map(|(id, kp)| (id, *kp.signing_share()))
            .collect();

        FrostThresholdCoin {
            config,
            hbbft_own_id,
            frost_own_identifier,
            key_package,
            group_public_key_package,
            min_signers,
            all_signing_shares,
            coin_participants,
            received_sig_shares: HashMap::new(),
            reconstructed_coins: HashMap::new(),
            own_signing_nonces: HashMap::new(),
            node_id_to_frost_id,
        }
    }

    fn get_message_for_coin(epoch: EpochNum, round: AbaRound) -> Vec<u8> {
        format!("hbbft-coin-{epoch}-{round}").into_bytes()
    }

    fn get_deterministic_commitments(
        &self,
        participant_id: &FrostIdentifier,
        epoch: EpochNum,
        round: AbaRound,
    ) -> Result<round1::SigningCommitments, HbbftError<NodeId>> {
        let signing_share = self
            .all_signing_shares
            .get(participant_id)
            .ok_or_else(|| {
                HbbftError::InternalError(
                    "Missing signing share for FROST ID (display might be limited)".to_string(),
                )
            })?;
        // We use the signing_share from all_signing_shares, which corresponds to the
        // participant_id
        let seed = derive_deterministic_seed("frost_commitment", epoch, round, participant_id);
        let mut rng = ChaCha20Rng::from_seed(seed);
        let (_nonces, commitments) = round1::commit(signing_share, &mut rng);
        Ok(commitments)
    }

    fn get_signing_package_for_coin(
        &self,
        epoch: EpochNum,
        round: AbaRound,
    ) -> Result<SigningPackage, HbbftError<NodeId>> {
        let mut commitments_map = BTreeMap::new();
        for p_id in &self.coin_participants {
            let p_commitments = self.get_deterministic_commitments(p_id, epoch, round)?;
            commitments_map.insert(*p_id, p_commitments);
        }
        let message = Self::get_message_for_coin(epoch, round);
        Ok(SigningPackage::new(commitments_map, &message))
    }

    fn derive_bool_from_signature(signature: &Signature) -> bool {
        let sig_bytes = signature.serialize().unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&sig_bytes);
        let hash_result = hasher.finalize();
        hash_result[0] % 2 == 0
    }
}

impl<NodeId: NodeIdentifier> CommonCoin for FrostThresholdCoin<NodeId> {
    type NodeId = NodeId;

    fn request_coin_value(&mut self, epoch: EpochNum, round: AbaRound) -> CoinOutput {
        let coin_key = (epoch, round);

        if let Some(&value) = self.reconstructed_coins.get(&coin_key) {
            return CoinOutput::with_value(value);
        }

        if !self
            .coin_participants
            .contains(&self.frost_own_identifier)
        {
            // If this node is not a designated participant, it doesn't produce a share.
            // It can only return a decided coin if it has already received enough shares.
            if let Some(shares_for_coin_map) = self.received_sig_shares.get(&coin_key)
                && shares_for_coin_map.len() >= self.min_signers as usize
            {
                // Attempt to aggregate only if enough shares are present
                match self.get_signing_package_for_coin(epoch, round) {
                    // Immutable borrow of self
                    Ok(signing_package) => {
                        match frost::aggregate(
                            // Immutable borrow of self for group_public_key_package
                            &signing_package,
                            shares_for_coin_map, // Use the immutable map
                            &self.group_public_key_package,
                        ) {
                            Ok(group_signature) => {
                                let coin_value = Self::derive_bool_from_signature(&group_signature);
                                // This mutable borrow is fine as other borrows for this
                                // operation have ended.
                                self.reconstructed_coins
                                    .insert(coin_key, coin_value);
                                return CoinOutput::with_value(coin_value);
                            }
                            Err(e) => {
                                eprintln!(
                                    "[FrostCoin] Aggregation failed for non-participant: {e:?}"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "[FrostCoin] Failed to get signing package for non-participant: {e:?}"
                        );
                    }
                }
            }
            return CoinOutput::empty(); // No share to broadcast, coin undecided
        }

        let own_nonces_val = self
            .own_signing_nonces
            .entry(coin_key)
            .or_insert_with(|| {
                let seed = derive_deterministic_seed(
                    "frost_commitment", /* Use the same purpose as for commitments in
                                         * SigningPackage */
                    epoch,
                    round,
                    &self.frost_own_identifier,
                );
                let mut rng_nonce = ChaCha20Rng::from_seed(seed);
                let (nonces, _commitments) =
                    round1::commit(self.key_package.signing_share(), &mut rng_nonce);
                nonces
            })
            .clone(); // Clone the nonces to release the mutable borrow on self.own_signing_nonces

        let signature_share_result = {
            let signing_package = match self.get_signing_package_for_coin(epoch, round) {
                Ok(sp) => sp,
                Err(_) => {
                    return CoinOutput::empty(); // No share to broadcast, coin undecided;
                }
            };
            round2::sign(
                &signing_package,
                &own_nonces_val,   // Pass the cloned, owned nonces by reference
                &self.key_package, // This immutable borrow is now fine
            ) // Message is part of signing_package
        };

        let own_sig_share = match signature_share_result {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[FrostCoin] Error generating own signature share: {e:?}");
                return CoinOutput::empty(); // No share to broadcast, coin undecided;
            }
        };

        let own_share_bytes: CoinShare = own_sig_share.serialize().to_vec();
        // The previous debug print for own_share_bytes.len() confirmed it's 32.
        eprintln!(
            "[FrostCoin DEBUG] request_coin_value for FROST ID {:?}: own_share_bytes.len() = {}",
            self.frost_own_identifier,
            own_share_bytes.len()
        );

        // Perform the mutable insert operation and let the borrow end.
        self.received_sig_shares
            .entry(coin_key)
            .or_default()
            .insert(self.frost_own_identifier, own_sig_share);

        let mut coin_value_now: Option<bool> = None;
        // Now, access self.received_sig_shares immutably.
        if let Some(current_shares_map) = self.received_sig_shares.get(&coin_key)
            && current_shares_map.len() >= self.min_signers as usize
        {
            match self.get_signing_package_for_coin(epoch, round) {
                Ok(signing_package) => {
                    match frost::aggregate(
                        &signing_package,
                        current_shares_map, // Pass the immutable reference
                        &self.group_public_key_package,
                    ) {
                        Ok(group_signature) => {
                            let value = Self::derive_bool_from_signature(&group_signature);
                            // This mutable borrow is fine as other borrows for this operation
                            // have ended.
                            self.reconstructed_coins
                                .insert(coin_key, value);
                            coin_value_now = Some(value);
                        }
                        Err(e) => {
                            eprintln!("[FrostCoin] Aggregation failed: {e:?}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[FrostCoin] Error getting signing package for aggregation: {e:?}");
                }
            }
        }
        CoinOutput::new(Some(own_share_bytes), coin_value_now)
    }

    fn submit_coin_share(
        &mut self,
        epoch: EpochNum,
        round: AbaRound,
        share_content: CoinShare,
        sender_hbbft_id: Self::NodeId,
    ) -> CoinOutput {
        let coin_key = (epoch, round);

        if let Some(&value) = self.reconstructed_coins.get(&coin_key) {
            return CoinOutput::with_value(value);
        }

        let sender_frost_id = match self
            .node_id_to_frost_id
            .get(&sender_hbbft_id)
        {
            Some(id) => *id,
            None => {
                eprintln!(
                    "[FrostCoin] Received share from unknown HBBFT NodeId: {sender_hbbft_id:?}"
                );
                return CoinOutput::empty();
            }
        };

        if !self
            .coin_participants
            .contains(&sender_frost_id)
        {
            eprintln!(
                "[FrostCoin] Received share from non-participant FROST ID: {sender_frost_id:?}"
            );
            return CoinOutput::empty();
        }

        // A frost_secp256k1::SignatureShare serializes to just its Scalar component.
        eprintln!(
            "[FrostCoin DEBUG] submit_coin_share from HBBFT ID {sender_hbbft_id:?}, FROST ID \
             {sender_frost_id:?}: share_content.len() = {}",
            share_content.len()
        );
        let sig_share_array: [u8; 32] = match share_content.try_into() {
            // Explicitly use 32 bytes for secp256k1 scalar
            Ok(arr) => arr,
            Err(_) => {
                eprintln!("[FrostCoin] Invalid share content length");
                return CoinOutput::empty();
            }
        };

        let sig_share = match round2::SignatureShare::deserialize(&sig_share_array) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[FrostCoin] Failed to deserialize signature share: {e:?}");
                return CoinOutput::empty();
            }
        };

        // Perform the mutable operation (insert) and let the borrow end.
        self.received_sig_shares
            .entry(coin_key)
            .or_default()
            .insert(sender_frost_id, sig_share);

        // Now, access self.received_sig_shares immutably.
        if let Some(current_shares_map) = self.received_sig_shares.get(&coin_key)
            && current_shares_map.len() >= self.min_signers as usize
        {
            // Immutable borrows of self for get_signing_package_for_coin and
            // group_public_key_package are now safe.
            match self.get_signing_package_for_coin(epoch, round) {
                Ok(signing_package) => {
                    match frost::aggregate(
                        &signing_package,
                        current_shares_map, // Pass the immutable reference
                        &self.group_public_key_package,
                    ) {
                        Ok(group_signature) => {
                            let value = Self::derive_bool_from_signature(&group_signature);
                            // This mutable borrow is fine as other borrows for this operation
                            // have ended.
                            self.reconstructed_coins
                                .insert(coin_key, value);
                            return CoinOutput::with_value(value);
                        }
                        Err(e) => {
                            eprintln!("[FrostCoin] Aggregation failed after submit: {e:?}");
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[FrostCoin] Error getting signing package for aggregation after submit: \
                         {e:?}"
                    );
                }
            }
        }
        CoinOutput::empty()
    }

    fn get_coin_for_round_fallback(&mut self, epoch: EpochNum, round: AbaRound) -> bool {
        let coin_key = (epoch, round);
        if let Some(&value) = self.reconstructed_coins.get(&coin_key) {
            return value;
        }

        let mut hasher = Sha256::new();
        hasher.update(b"hbbft-coin-fallback-salt");
        hasher.update(epoch.to_le_bytes());
        hasher.update(round.to_le_bytes());
        let hash_result = hasher.finalize();
        let fallback_value = hash_result[0] % 2 == 0;

        self.reconstructed_coins
            .insert(coin_key, fallback_value);
        eprintln!(
            "[FrostCoin Fallback] epoch: {epoch}, round: {round}. Using fallback value: \
             {fallback_value}",
        );
        fallback_value
    }
}

/// Helper to setup FROST participants for scenarios. Made public for use in
/// examples/main.
pub fn setup_frost_participants(
    num_total_nodes: u16,
    min_signers: u16,
) -> (BTreeMap<FrostIdentifier, KeyPackage>, PublicKeyPackage, BTreeMap<DemoNodeId, FrostIdentifier>)
{
    let mut rng = ChaCha20Rng::from_seed([0u8; 32]); // Using StdRng

    let mut hbbft_node_id_to_frost_id = BTreeMap::new();
    let frost_identifiers: Vec<FrostIdentifier> = (1..=num_total_nodes)
        .map(|i| {
            let frost_id = FrostIdentifier::try_from(i).unwrap();
            hbbft_node_id_to_frost_id.insert(DemoNodeId((i - 1) as usize), frost_id);
            frost_id
        })
        .collect();

    let (shares, group_pubkey_pkg) = frost::keys::generate_with_dealer(
        num_total_nodes,
        min_signers,
        frost::keys::IdentifierList::Custom(&frost_identifiers),
        &mut rng,
    )
    .unwrap();

    let key_packages: BTreeMap<FrostIdentifier, KeyPackage> = shares
        .into_iter()
        .map(|(id, secret_share)| {
            // Error 1: Access verifying_shares map first, then get the specific share.
            let verifying_share_val = *group_pubkey_pkg
                .verifying_shares()
                .get(&id)
                .unwrap();
            (
                id,
                // Error 2: Call the KeyPackage::new wrapper from frost-secp256k1
                // using the `frost` alias to ensure correct method resolution.
                frost::keys::KeyPackage::new(
                    id,
                    *secret_share.signing_share(), // SigningShare is an alias for SecretShare
                    verifying_share_val,           /* This is frost_core::keys::VerifyingShare
                                                    * (aliased as
                                                    * frost_secp256k1::VerifyingShare) */
                    *group_pubkey_pkg.verifying_key(),
                    min_signers,
                ),
            )
        })
        .collect();
    (
        key_packages,
        group_pubkey_pkg, // Return the whole PublicKeyPackage
        hbbft_node_id_to_frost_id,
    )
}

pub fn run_integration_scenario() {
    println!("\n--- Running Integration Scenario ---");
    let n_hbbft: usize = 4; // Used for clarity, though create_test_config_with_node_ids infers from all_node_ids_vec
    let f_hbbft: usize = 1;
    let min_frost_signers: u16 = (f_hbbft + 1) as u16;

    let hbbft_node_ids: Vec<DemoNodeId> = (0..n_hbbft).map(DemoNodeId).collect();

    // 1. Setup FROST participants and initial data structures
    let (
        raw_all_key_packages_frost, // BTreeMap<FrostIdentifier, KeyPackage>
        raw_group_pk_pkg_frost,     // Changed to PublicKeyPackage
        raw_hbbft_to_frost_map_btreemap,
    ) = setup_frost_participants(n_hbbft as u16, min_frost_signers);

    // Convert BTreeMap to HashMap as expected by later code
    let raw_hbbft_to_frost_map: HashMap<DemoNodeId, FrostIdentifier> =
        raw_hbbft_to_frost_map_btreemap
            .into_iter()
            .collect();

    // 2. Wrap shared data in Arc for shared ownership
    let all_key_packages_frost_arc = Arc::new(raw_all_key_packages_frost);
    let group_pk_pkg_frost_arc = Arc::new(raw_group_pk_pkg_frost); // Changed variable name
    let hbbft_to_frost_map_arc = Arc::new(raw_hbbft_to_frost_map);

    // 3. Create CoinFactory: closure captures Arcs
    // Clone Arcs that will be moved into the closure
    let factory_all_key_packages_arc = Arc::clone(&all_key_packages_frost_arc);
    let factory_group_pk_pkg_arc = Arc::clone(&group_pk_pkg_frost_arc); // Changed variable name
    let factory_hbbft_map_arc = Arc::clone(&hbbft_to_frost_map_arc);

    let coin_factory_fn = Arc::new(
        move |hbbft_node_id: DemoNodeId,
              _epoch: EpochNum,
              config_for_coin: Arc<HoneyBadgerConfig<DemoNodeId>>| {
            let local_frost_id = *factory_hbbft_map_arc
                .get(&hbbft_node_id)
                .expect("HBBFT ID not in FROST map for factory");
            let local_key_pkg = factory_all_key_packages_arc
                .get(&local_frost_id)
                .expect("FROST ID not in key packages for factory")
                .clone(); // KeyPackage is Clone

            Box::new(FrostThresholdCoin::new(
                config_for_coin,
                hbbft_node_id,
                local_frost_id,
                local_key_pkg,
                (*factory_group_pk_pkg_arc).clone(), // Deref Arc and clone PublicKeyPackage
                (*factory_all_key_packages_arc).clone(), // Deref Arc and clone BTreeMap
                (*factory_hbbft_map_arc).clone(),    // Deref Arc and clone HashMap
            )) as Box<dyn CommonCoin<NodeId = DemoNodeId>>
        },
    );
    let demo_coin_factory = CoinFactory::new(coin_factory_fn);

    // 4. Create HBBFT configurations using the factory
    let config0_hbbft = create_test_config_with_node_ids(
        f_hbbft,
        hbbft_node_ids.clone(),
        hbbft_node_ids[0],
        demo_coin_factory.clone(),
    );

    let config1_hbbft = create_test_config_with_node_ids(
        f_hbbft,
        hbbft_node_ids.clone(),
        hbbft_node_ids[1],
        demo_coin_factory.clone(),
    );

    // 5. Create FrostThresholdCoin instances for direct testing, using original
    //    Arcs
    let frost_id0 = *hbbft_to_frost_map_arc
        .get(&hbbft_node_ids[0])
        .unwrap();
    let key_pkg0 = all_key_packages_frost_arc
        .get(&frost_id0)
        .unwrap()
        .clone();
    let mut coin0 = FrostThresholdCoin::new(
        Arc::clone(&config0_hbbft),
        hbbft_node_ids[0],
        frost_id0,
        key_pkg0,
        (*group_pk_pkg_frost_arc).clone(), // Use PublicKeyPackage
        (*all_key_packages_frost_arc).clone(),
        (*hbbft_to_frost_map_arc).clone(),
    );

    let frost_id1 = *hbbft_to_frost_map_arc
        .get(&hbbft_node_ids[1])
        .unwrap();
    let key_pkg1 = all_key_packages_frost_arc
        .get(&frost_id1)
        .unwrap()
        .clone();
    let mut coin1 = FrostThresholdCoin::new(
        Arc::clone(&config1_hbbft),
        hbbft_node_ids[1],
        frost_id1,
        key_pkg1,
        (*group_pk_pkg_frost_arc).clone(), // Use PublicKeyPackage
        (*all_key_packages_frost_arc).clone(),
        (*hbbft_to_frost_map_arc).clone(),
    );

    assert_eq!(coin0.coin_participants.len(), min_frost_signers as usize);
    assert!(
        coin0
            .coin_participants
            .contains(&frost_id0)
    );
    assert!(
        coin0
            .coin_participants
            .contains(&frost_id1)
    );

    let epoch: EpochNum = 1;
    let round: AbaRound = 0;

    println!("Node 0 requesting coin value...");
    let output0 = coin0.request_coin_value(epoch, round);
    assert!(output0.share_to_broadcast.is_some(), "Node 0 should produce a share");
    assert!(output0.decided_value.is_none(), "Coin should not be decided with one share yet");
    let share0_bytes = output0.share_to_broadcast.unwrap();
    println!("Node 0 produced share. Coin undecided.");

    println!("Node 1 requesting coin value...");
    let output1 = coin1.request_coin_value(epoch, round);
    assert!(output1.share_to_broadcast.is_some(), "Node 1 should produce a share");
    assert!(
        output1.decided_value.is_none(),
        "Coin should not be decided with one share yet (from its perspective)"
    );
    let share1_bytes = output1.share_to_broadcast.unwrap();
    println!("Node 1 produced share. Coin undecided.");

    println!("Node 0 submitting Node 1's share...");
    let CoinOutput {
        decided_value: val_at_node0_after_share1,
        ..
    } = coin0.submit_coin_share(epoch, round, share1_bytes.clone(), hbbft_node_ids[1]);
    assert!(
        val_at_node0_after_share1.is_some(),
        "Node 0 should decide coin after receiving Node 1's share"
    );
    let decided_value_at_0 = val_at_node0_after_share1.unwrap();
    println!("Node 0 decided coin value: {decided_value_at_0}");

    println!("Node 1 submitting Node 0's share...");
    let CoinOutput {
        decided_value: val_at_node1_after_share0,
        ..
    } = coin1.submit_coin_share(epoch, round, share0_bytes.clone(), hbbft_node_ids[0]);
    assert!(
        val_at_node1_after_share0.is_some(),
        "Node 1 should decide coin after receiving Node 0's share"
    );
    let decided_value_at_1 = val_at_node1_after_share0.unwrap();
    println!("Node 1 decided coin value: {decided_value_at_1}");

    assert_eq!(
        decided_value_at_0, decided_value_at_1,
        "Both nodes should decide on the same coin value"
    );
    println!("SUCCESS: Both nodes decided on the same value: {decided_value_at_0}");

    let output0_again = coin0.request_coin_value(epoch, round);
    assert!(
        output0_again
            .share_to_broadcast
            .is_none(),
        "Node 0 should not produce a new share if decided"
    );
    assert_eq!(output0_again.decided_value, Some(decided_value_at_0));
    println!("Node 0 subsequent request returns cached value.");
}

/// Runs the fallback scenario for the FROST common coin.
pub fn run_fallback_scenario() {
    println!("\n--- Running Fallback Scenario ---");
    let n_hbbft: usize = 4;
    let f_hbbft: usize = 1;
    let min_frost_signers: u16 = (f_hbbft + 1) as u16;
    let hbbft_node_ids: Vec<DemoNodeId> = (0..n_hbbft).map(DemoNodeId).collect();

    // 1. Setup FROST participants and initial data structures
    let (
        raw_all_key_packages_frost,
        raw_group_pk_pkg_frost, // Changed to PublicKeyPackage
        raw_hbbft_to_frost_map_btreemap,
    ) = setup_frost_participants(n_hbbft as u16, min_frost_signers);

    let raw_hbbft_to_frost_map: HashMap<DemoNodeId, FrostIdentifier> =
        raw_hbbft_to_frost_map_btreemap
            .into_iter()
            .collect();
    // 2. Wrap shared data in Arc
    let all_key_packages_frost_arc = Arc::new(raw_all_key_packages_frost);
    let group_pk_pkg_frost_arc = Arc::new(raw_group_pk_pkg_frost); // Changed variable name
    let hbbft_to_frost_map_arc = Arc::new(raw_hbbft_to_frost_map);

    // 3. Create CoinFactory: closure captures Arcs
    let factory_all_key_packages_arc = Arc::clone(&all_key_packages_frost_arc);
    let factory_group_pk_arc = Arc::clone(&group_pk_pkg_frost_arc);
    let factory_hbbft_map_arc = Arc::clone(&hbbft_to_frost_map_arc);

    let coin_factory_fn = Arc::new(
        move |hbbft_node_id: DemoNodeId,
              _epoch: EpochNum,
              config_for_coin: Arc<HoneyBadgerConfig<DemoNodeId>>| {
            let local_frost_id = *factory_hbbft_map_arc
                .get(&hbbft_node_id)
                .expect("HBBFT ID not in FROST map for factory");
            let local_key_pkg = factory_all_key_packages_arc
                .get(&local_frost_id)
                .expect("FROST ID not in key packages for factory")
                .clone();

            Box::new(FrostThresholdCoin::new(
                config_for_coin,
                hbbft_node_id,
                local_frost_id,
                local_key_pkg,
                (*factory_group_pk_arc).clone(), // Use PublicKeyPackage
                (*factory_all_key_packages_arc).clone(),
                (*factory_hbbft_map_arc).clone(),
            )) as Box<dyn CommonCoin<NodeId = DemoNodeId>>
        },
    );
    let demo_coin_factory = CoinFactory::new(coin_factory_fn);

    // 4. Create HBBFT configuration for node 0
    let config0_hbbft = create_test_config_with_node_ids(
        f_hbbft,
        hbbft_node_ids.clone(),
        hbbft_node_ids[0],
        demo_coin_factory.clone(),
    );

    // 5. Create FrostThresholdCoin instance for direct testing (coin0)
    let frost_id0 = *hbbft_to_frost_map_arc
        .get(&hbbft_node_ids[0])
        .unwrap();
    let key_pkg0 = all_key_packages_frost_arc
        .get(&frost_id0)
        .unwrap()
        .clone();
    let mut coin0 = FrostThresholdCoin::new(
        Arc::clone(&config0_hbbft),
        hbbft_node_ids[0],
        frost_id0,
        key_pkg0,
        (*group_pk_pkg_frost_arc).clone(), // Use PublicKeyPackage
        (*all_key_packages_frost_arc).clone(),
        (*hbbft_to_frost_map_arc).clone(),
    );

    let epoch: EpochNum = 5;
    let round: AbaRound = 3;

    println!("Node 0 requesting coin (epoch {epoch}, round {round})...");
    let output = coin0.request_coin_value(epoch, round);
    assert!(output.share_to_broadcast.is_some(), "Node 0 should produce a share for fallback test");
    assert!(output.decided_value.is_none(), "Coin should be undecided initially in fallback test");
    println!("Node 0 produced share. Coin undecided.");

    println!("Simulating ABA timeout, Node 0 calls get_coin_for_round_fallback...");
    let fallback_val = coin0.get_coin_for_round_fallback(epoch, round);
    println!("Node 0 fallback value: {fallback_val}");

    let output_after_fallback = coin0.request_coin_value(epoch, round);
    assert!(
        output_after_fallback
            .share_to_broadcast
            .is_none(),
        "No new share should be produced after fallback decision"
    );
    assert_eq!(
        output_after_fallback.decided_value,
        Some(fallback_val),
        "Subsequent request should return cached fallback value"
    );
    println!("Node 0 subsequent request returns cached fallback value.");

    // Create HBBFT configuration for node 1 (for coin1 instance)
    let config1_hbbft = create_test_config_with_node_ids(
        f_hbbft,
        hbbft_node_ids.clone(),
        hbbft_node_ids[1],
        demo_coin_factory.clone(),
    );

    // Create FrostThresholdCoin instance for direct testing (coin1)
    let frost_id1 = *hbbft_to_frost_map_arc
        .get(&hbbft_node_ids[1])
        .unwrap();
    let key_pkg1 = all_key_packages_frost_arc
        .get(&frost_id1)
        .unwrap()
        .clone();
    let mut coin1 = FrostThresholdCoin::new(
        Arc::clone(&config1_hbbft),
        hbbft_node_ids[1],
        frost_id1,
        key_pkg1,
        (*group_pk_pkg_frost_arc).clone(), // Use PublicKeyPackage
        (*all_key_packages_frost_arc).clone(),
        (*hbbft_to_frost_map_arc).clone(),
    );

    println!("Simulating ABA timeout for Node 1, calls get_coin_for_round_fallback...");
    let fallback_val_node1 = coin1.get_coin_for_round_fallback(epoch, round);
    println!("Node 1 fallback value: {fallback_val_node1}");
    assert_eq!(
        fallback_val_node1, fallback_val,
        "Fallback should be deterministic across instances"
    );
    println!("SUCCESS: Node 1 fallback value matches Node 0's fallback value.");
}

/// Parses command-line arguments and runs the selected scenario.
/// This function would typically be called from `src/main.rs`.
pub fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: cargo run -- [SCENARIO_NAME]"); // Adjusted for direct binary run
        println!("Available scenarios: integration, fallback");
        return;
    }

    match args[1].as_str() {
        "integration" => run_integration_scenario(),
        "fallback" => run_fallback_scenario(),
        "seeded-integration" => frost_seed::run_seed_coin_integration_scenario(),
        "seeded-fallback" => frost_seed::run_seed_coin_fallback_scenario(),
        _ => {
            println!("Unknown scenario: {}", args[1]);
            println!(
                "Available scenarios: integration, fallback, seeded-integration, seeded-fallback"
            );
        }
    }
}
