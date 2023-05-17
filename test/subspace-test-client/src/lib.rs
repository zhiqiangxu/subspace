// Copyright (C) 2021 Subspace Labs, Inc.
// SPDX-License-Identifier: GPL-3.0-or-later

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! Subspace test client only.

#![warn(missing_docs, unused_crate_dependencies)]

pub mod chain_spec;

use futures::executor::block_on;
use futures::StreamExt;
use sc_client_api::{BlockBackend, HeaderBackend};
use sc_consensus_subspace::notification::SubspaceNotificationStream;
use sc_consensus_subspace::{NewSlotNotification, RewardSigningNotification};
use sp_api::ProvideRuntimeApi;
use sp_consensus_subspace::{FarmerPublicKey, FarmerSignature, SubspaceApi};
use sp_core::{Decode, Encode};
use std::num::NonZeroUsize;
use std::sync::Arc;
use subspace_core_primitives::crypto::kzg::{embedded_kzg_settings, Kzg};
use subspace_core_primitives::objects::BlockObjectMapping;
use subspace_core_primitives::{HistorySize, PublicKey, Record, SegmentIndex, Solution};
use subspace_erasure_coding::ErasureCoding;
use subspace_farmer_components::auditing::audit_sector;
use subspace_farmer_components::plotting::{plot_sector, PieceGetterRetryPolicy, PlottedSector};
use subspace_farmer_components::sector::{sector_size, SectorMetadata};
use subspace_farmer_components::FarmerProtocolInfo;
use subspace_proof_of_space::Table;
use subspace_runtime_primitives::opaque::Block;
use subspace_service::tx_pre_validator::PrimaryChainTxPreValidator;
use subspace_service::{FullClient, NewFull};
use subspace_solving::REWARD_SIGNING_CONTEXT;
use subspace_transaction_pool::bundle_validator::BundleValidator;
use zeroize::Zeroizing;

// Smaller value for testing purposes
const MAX_PIECES_IN_SECTOR: u16 = 32;

/// Subspace native executor instance.
pub struct TestExecutorDispatch;

impl sc_executor::NativeExecutionDispatch for TestExecutorDispatch {
    /// Otherwise we only use the default Substrate host functions.
    type ExtendHostFunctions = sp_consensus_subspace::consensus::HostFunctions;

    fn dispatch(method: &str, data: &[u8]) -> Option<Vec<u8>> {
        subspace_test_runtime::api::dispatch(method, data)
    }

    fn native_version() -> sc_executor::NativeVersion {
        subspace_test_runtime::native_version()
    }
}

/// The client type being used by the test service.
pub type Client = FullClient<subspace_test_runtime::RuntimeApi, TestExecutorDispatch>;

/// The backend type being used by the test service.
pub type Backend = sc_service::TFullBackend<Block>;

/// The fraud proof verifier being used the test service.
pub type FraudProofVerifier =
    subspace_service::FraudProofVerifier<subspace_test_runtime::RuntimeApi, TestExecutorDispatch>;

type TxPreValidator =
    PrimaryChainTxPreValidator<Block, Client, FraudProofVerifier, BundleValidator<Block, Client>>;

/// Run a farmer.
pub fn start_farmer<PosTable>(new_full: &NewFull<Client, TxPreValidator>)
where
    PosTable: Table,
{
    let client = new_full.client.clone();
    let new_slot_notification_stream = new_full.new_slot_notification_stream.clone();
    let reward_signing_notification_stream = new_full.reward_signing_notification_stream.clone();

    let keypair = schnorrkel::Keypair::generate();
    let subspace_farming =
        start_farming::<PosTable, _>(keypair.clone(), client, new_slot_notification_stream);
    new_full
        .task_manager
        .spawn_essential_handle()
        .spawn_blocking("subspace-farmer", Some("farming"), subspace_farming);

    new_full
        .task_manager
        .spawn_essential_handle()
        .spawn_blocking("subspace-farmer", Some("block-signing"), async move {
            let substrate_ctx = schnorrkel::context::signing_context(REWARD_SIGNING_CONTEXT);
            let signing_pair: Zeroizing<schnorrkel::Keypair> = Zeroizing::new(keypair);

            let mut reward_signing_notification_stream =
                reward_signing_notification_stream.subscribe();

            while let Some(RewardSigningNotification {
                hash: header_hash,
                signature_sender,
                ..
            }) = reward_signing_notification_stream.next().await
            {
                let header_hash: [u8; 32] = header_hash.into();
                let signature: subspace_core_primitives::RewardSignature = signing_pair
                    .sign(substrate_ctx.bytes(&header_hash))
                    .to_bytes()
                    .into();
                signature_sender
                    .unbounded_send(
                        FarmerSignature::decode(&mut signature.encode().as_ref())
                            .expect("Failed to decode schnorrkel block signature"),
                    )
                    .unwrap();
            }
        });
}

async fn start_farming<PosTable, Client>(
    keypair: schnorrkel::Keypair,
    client: Arc<Client>,
    new_slot_notification_stream: SubspaceNotificationStream<NewSlotNotification>,
) where
    PosTable: Table,
    Client: ProvideRuntimeApi<Block>
        + BlockBackend<Block>
        + HeaderBackend<Block>
        + Send
        + Sync
        + 'static,
    Client::Api: SubspaceApi<Block, FarmerPublicKey>,
{
    let (plotting_result_sender, plotting_result_receiver) = futures::channel::oneshot::channel();

    let kzg = Kzg::new(embedded_kzg_settings());
    let erasure_coding = ErasureCoding::new(
        NonZeroUsize::new(Record::NUM_S_BUCKETS.next_power_of_two().ilog2() as usize).unwrap(),
    )
    .unwrap();

    std::thread::spawn({
        let keypair = keypair.clone();
        let erasure_coding = erasure_coding.clone();

        move || {
            let (sector, sector_metadata) = block_on(plot_one_segment::<PosTable, _>(
                client.as_ref(),
                &keypair,
                MAX_PIECES_IN_SECTOR,
                &erasure_coding,
            ));
            plotting_result_sender
                .send((sector, sector_metadata))
                .unwrap();
        }
    });

    let (sector, plotted_sector) = plotting_result_receiver.await.unwrap();
    let sector_index = 0;
    let public_key = PublicKey::from(keypair.public.to_bytes());

    let mut new_slot_notification_stream = new_slot_notification_stream.subscribe();

    while let Some(NewSlotNotification {
        new_slot_info,
        solution_sender,
    }) = new_slot_notification_stream.next().await
    {
        if u64::from(new_slot_info.slot) % 2 == 0 {
            let solution_candidates = audit_sector(
                &public_key,
                sector_index,
                &new_slot_info.global_challenge,
                new_slot_info.solution_range,
                &sector,
                &plotted_sector.sector_metadata,
            )
            .expect("With max solution range there must be a sector eligible; qed");

            let solution = solution_candidates
                .into_iter::<_, PosTable>(&public_key, &kzg, &erasure_coding)
                .unwrap()
                .next()
                .expect("With max solution range there must be a solution; qed")
                .unwrap();
            // Lazy conversion to a different type of public key and reward address
            let solution = Solution::<FarmerPublicKey, FarmerPublicKey>::decode(
                &mut solution.encode().as_slice(),
            )
            .unwrap();
            let _ = solution_sender.unbounded_send(solution);
        }
    }
}

async fn plot_one_segment<PosTable, Client>(
    client: &Client,
    keypair: &schnorrkel::Keypair,
    pieces_in_sector: u16,
    erasure_coding: &ErasureCoding,
) -> (Vec<u8>, PlottedSector)
where
    PosTable: Table,
    Client: BlockBackend<Block> + HeaderBackend<Block>,
{
    let kzg = Kzg::new(embedded_kzg_settings());
    let mut archiver = subspace_archiving::archiver::Archiver::new(kzg.clone())
        .expect("Incorrect parameters for archiver");

    let genesis_block = client.block(client.info().genesis_hash).unwrap().unwrap();
    let archived_segment = archiver
        .add_block(genesis_block.encode(), BlockObjectMapping::default())
        .into_iter()
        .next()
        .expect("First block is always producing one segment; qed");
    let history_size = HistorySize::from(SegmentIndex::ZERO);
    let mut sector = vec![0u8; sector_size(pieces_in_sector)];
    let mut sector_metadata = vec![0u8; SectorMetadata::encoded_size()];
    let sector_offset = 0;
    let sector_index = 0;
    let public_key = PublicKey::from(keypair.public.to_bytes());
    let farmer_protocol_info = FarmerProtocolInfo {
        history_size,
        max_pieces_in_sector: pieces_in_sector,
        sector_expiration: SegmentIndex::from(100),
    };

    let plotted_sector = plot_sector::<_, PosTable>(
        &public_key,
        sector_offset,
        sector_index,
        &archived_segment.pieces,
        PieceGetterRetryPolicy::default(),
        &farmer_protocol_info,
        &kzg,
        erasure_coding,
        pieces_in_sector,
        &mut sector,
        &mut sector_metadata,
        Default::default(),
    )
    .await
    .expect("Plotting one sector in memory must not fail");

    (sector, plotted_sector)
}
