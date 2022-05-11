use actix;
use borealis_types::types::BorealisMessage;
use clap::Parser;
use configs::{
    init_logging, AwaitSynced, Error, MsgFormat, Opts, RunArgs, SubCommand, SyncMode,
    VerbosityLevel, CompressionMode,
};
use core::sync::atomic::{AtomicUsize, Ordering};
use nats;
use near_indexer;
use serde_cbor as cbor;
use serde_json;
use tokio::runtime::{Runtime, Builder};
use tokio::signal::{ctrl_c, unix::{signal, SignalKind}};
use tokio::sync::{mpsc, watch};
use tracing:: {info, error, debug};

pub mod configs;

static SIGNAL: AtomicUsize = AtomicUsize::new(0);

async fn kill_switch_usr1() -> Result<(), Error> {
    let mut kill_signal_stream = signal(SignalKind::from_raw(10))?;
    info!(
        target: "borealis_indexer",
        "Kill signal (USR1) handler installed\n"
    );
    while let Some(()) = kill_signal_stream.recv().await {
        info!(
            target: "borealis_indexer",
            "Kill signal (USR1) handler triggered\n"
        );
        SIGNAL.store(10, Ordering::SeqCst);
        actix::System::current().stop();
    }
    Ok(())
}

async fn kill_switch_usr2() -> Result<(), Error> {
    let mut kill_signal_stream = signal(SignalKind::from_raw(12))?;
    info!(
        target: "borealis_indexer",
        "Kill signal (USR2) handler installed\n"
    );
    while let Some(()) = kill_signal_stream.recv().await {
        info!(
            target: "borealis_indexer",
            "Kill signal (USR2) handler triggered\n"
        );
        SIGNAL.store(12, Ordering::SeqCst);
        actix::System::current().stop();
    }
    Ok(())
}

async fn term_switch() -> Result<(), Error> {
    let mut term_signal_stream = signal(SignalKind::terminate())?;
    info!(
        target: "borealis_indexer",
        "Terminate signal handler installed\n"
    );
    while let Some(()) = term_signal_stream.recv().await {
        info!(
            target: "borealis_indexer",
            "Terminate signal handler triggered\n"
        );
        SIGNAL.store(15, Ordering::SeqCst);
        actix::System::current().stop();
    }
    Ok(())
}

async fn hup_switch() -> Result<(), Error> {
    let mut hup_signal_stream = signal(SignalKind::hangup())?;
    info!(
        target: "borealis_indexer",
        "Hangup signal handler installed\n"
    );
    while let Some(()) = hup_signal_stream.recv().await {
        info!(
            target: "borealis_indexer",
            "Hangup signal handler triggered\n"
        );
        SIGNAL.store(1, Ordering::SeqCst);
    }
    Ok(())
}

async fn key_switch() -> Result<(), Error> {
    info!(
        target: "borealis_indexer",
        "Ctrl-C key sequence handler installed\n"
    );
    while let Ok(()) = ctrl_c().await {
        info!(
            target: "borealis_indexer",
            "Ctrl-C key sequence handler triggered\n"
        );
        SIGNAL.store(602437500, Ordering::SeqCst);
        actix::System::current().stop();
    }
    Ok(())
}

async fn message_producer(
    mut events_stream: mpsc::Receiver<near_indexer::StreamerMessage>,
    actual_connection_rx: watch::Receiver<NATSConnection>,
    connection_event_tx: mpsc::Sender<ConnectionEvent>,
    context: RunArgs,
    verbosity_level: Option<VerbosityLevel>,
) {
    info!(
        target: "borealis_indexer",
        "Message producer loop started: listening for new messages\n"
    );

    while let Some(streamer_message) = events_stream.recv().await {
        /*
            Example of `StreamerMessage` with all data fields (filled with synthetic data, as an example):

            Note that `outcomes` for a given transaction won't be included into the same block.
            Execution outcomes are included into the blocks after the transaction or receipt
            are recorded on a chain; in most cases, it is the next block after the one that has
            the transaction or receipt.

            StreamerMessage {
                block: BlockView {
                    author: "test.near",
                    header: BlockHeaderView {
                        height: 63596,
                        epoch_id: `Bk7pvZWUTfHRRZtfgTDjnQ6y5cV8yG2h3orCqJvUbiym`,
                        next_epoch_id: `3JuBZ4Gz5Eauf7PzQegfqSEDyvws3eKJYPbfGHAYmeR5`,
                        hash: `5X37niQWWcihDGQjsvDMHYKLCurNJyQLxCeLgneDb8mk`,
                        prev_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
                        prev_state_root: `GkdxSBf4Kfq8V16N4Kqn3YdcThG1f5KG1KLBmXpMzP1k`,
                        chunk_receipts_root: `9ETNjrt6MkwTgSVMMbpukfxRshSD1avBUUa4R4NuqwHv`,
                        chunk_headers_root: `C7dVr9KdXYKt31yF2BkeAu115fpo79zYTqeU3FzqbFak`,
                        chunk_tx_root: `7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t`,
                        outcome_root: `7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t`,
                        chunks_included: 1,
                        challenges_root: `11111111111111111111111111111111`,
                        timestamp: 1618558205803345000,
                        timestamp_nanosec: 1618558205803345000,
                        random_value: `3cAa93XmoLaKAJQgWz3K7SiKwnA3uaxi8MGgLM78HTNS`,
                        validator_proposals: [],
                        chunk_mask: [
                            true,
                        ],
                        gas_price: 1000000000,
                        rent_paid: 0,
                        validator_reward: 0,
                        total_supply: 2050206401403887985811862247311434,
                        challenges_result: [],
                        last_final_block: `DCkMmXYHqibzcMjgFjRXJP7eckAMLrA4ijggSApMNwKu`,
                        last_ds_final_block: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
                        next_bp_hash: `4DJWnxRbUhRrsXK6EBkx4nFeXHKgJWqteDnJ7Hv4MZ6M`,
                        block_merkle_root: `Bvn5K89fJ3uPNsj3324Ls9TXAGUVteHPpfKwKqL1La6W`,
                        approvals: [
                            Some(
                                ed25519:F816hgJod7nPfD2qQz5yhaKDMn1JXmvzj2iXegsJpsmPNnYYZpKYJXgyuVTVJ4TKQbcJ2Q3USCGZF6fX2TcwBBv,
                            ),
                        ],
                        signature: ed25519:239NbE4BuJaxneQA3AEsPrsGY7v3wBgaezbgg56HER69zPrBoc3a4fbyVWPXeoKE3LvgGma1g6pSHk9QHkmETCZY,
                        latest_protocol_version: 43,
                    },
                    chunks: [
                        ChunkHeaderView {
                            chunk_hash: `2M2oeNFBbUUnHfkU1UuBr8EKBCLMH9xr2vfsGRpyiBmA`,
                            prev_block_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
                            outcome_root: `11111111111111111111111111111111`,
                            prev_state_root: `3gZPPijaumgMRCvMuuZZM1Ab2LoHTSfYigMKwLqZ67m6`,
                            encoded_merkle_root: `79Bt7ivt9Qhp3c6dJYnueaTyPVweYxZRpQHASRRAiyuy`,
                            encoded_length: 8,
                            height_created: 63596,
                            height_included: 63596,
                            shard_id: 0,
                            gas_used: 0,
                            gas_limit: 1000000000000000,
                            rent_paid: 0,
                            validator_reward: 0,
                            balance_burnt: 0,
                            outgoing_receipts_root: `H4Rd6SGeEBTbxkitsCdzfu9xL9HtZ2eHoPCQXUeZ6bW4`,
                            tx_root: `11111111111111111111111111111111`,
                            validator_proposals: [],
                            signature: ed25519:2vWNayBzEoW5DRc7gTdhxdLbkKuK6ACQ78p3JGpKSAZZCarnLroeoALPAFwpr9ZNPxBqdVYh9QLBe7WHZebsS17Z,
                        },
                    ],
                },
                shards: [
                    IndexerShard {
                        shard_id: 0,
                        chunk: Some(
                            IndexerChunkView {
                                author: "test.near",
                                header: ChunkHeaderView {
                                    chunk_hash: `2M2oeNFBbUUnHfkU1UuBr8EKBCLMH9xr2vfsGRpyiBmA`,
                                    prev_block_hash: `2vJNJca72pBiq2eETq2xvuoc6caKDaUkdRgtdefyutbA`,
                                    outcome_root: `11111111111111111111111111111111`,
                                    prev_state_root: `3gZPPijaumgMRCvMuuZZM1Ab2LoHTSfYigMKwLqZ67m6`,
                                    encoded_merkle_root: `79Bt7ivt9Qhp3c6dJYnueaTyPVweYxZRpQHASRRAiyuy`,
                                    encoded_length: 8,
                                    height_created: 63596,
                                    height_included: 0,
                                    shard_id: 0,
                                    gas_used: 0,
                                    gas_limit: 1000000000000000,
                                    rent_paid: 0,
                                    validator_reward: 0,
                                    balance_burnt: 0,
                                    outgoing_receipts_root: `H4Rd6SGeEBTbxkitsCdzfu9xL9HtZ2eHoPCQXUeZ6bW4`,
                                    tx_root: `11111111111111111111111111111111`,
                                    validator_proposals: [],
                                    signature: ed25519:2vWNayBzEoW5DRc7gTdhxdLbkKuK6ACQ78p3JGpKSAZZCarnLroeoALPAFwpr9ZNPxBqdVYh9QLBe7WHZebsS17Z,
                                },
                                transactions: [
                                    IndexerTransactionWithOutcome {
                                        transaction: SignedTransactionView {
                                            signer_id: "test.near",
                                            public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
                                            nonce: 1,
                                            receiver_id: "some.test.near",
                                            actions: [
                                                CreateAccount,
                                                Transfer {
                                                    deposit: 40000000000000000000000000,
                                                },
                                                AddKey {
                                                    public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
                                                    access_key: AccessKeyView {
                                                        nonce: 0,
                                                        permission: FullAccess,
                                                    },
                                                },
                                            ],
                                            signature: ed25519:Qniuu7exnr6xbe6gKafV5vDhuwM1jt9Bn7sCTF6cHfPpYWVJ4Q6kq8RAxKSeLoxbCreVp1XzMMJmXt8YcUqmMYw,
                                            hash: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
                                        },
                                        outcome: IndexerExecutionOutcomeWithReceipt {
                                            execution_outcome: ExecutionOutcomeWithIdView {
                                                proof: [],
                                                block_hash: `G9v6Fsv94xaa7BRY2N5PFF5PJwT7ec6DPzQK73Yf3CZ6`,
                                                id: `8dNv9S8rAFwso9fLwfDQXmw5yv5zscDjQpta96pMF6Bi`,
                                                outcome: ExecutionOutcomeView {
                                                    logs: [],
                                                    receipt_ids: [
                                                        `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
                                                    ],
                                                    gas_burnt: 424555062500,
                                                    tokens_burnt: 424555062500000000000,
                                                    executor_id: "test.near",
                                                    status: SuccessReceiptId(CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz),
                                                },
                                            },
                                            receipt: None,
                                        },
                                    },
                                ],
                                receipts: [
                                    ReceiptView {
                                        predecessor_id: "test.near",
                                        receiver_id: "some.test.near",
                                        receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
                                        receipt: Action {
                                            signer_id: "test.near",
                                            signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
                                            gas_price: 1030000000,
                                            output_data_receivers: [],
                                            input_data_ids: [],
                                            actions: [
                                                CreateAccount,
                                                Transfer {
                                                    deposit: 40000000000000000000000000,
                                                },
                                                AddKey {
                                                    public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
                                                    access_key: AccessKeyView {
                                                        nonce: 0,
                                                        permission: FullAccess,
                                                    },
                                                },
                                            ],
                                        },
                                    },
                                ],
                            },
                        ),
                        receipt_execution_outcomes: [
                            IndexerExecutionOutcomeWithReceipt {
                                execution_outcome: ExecutionOutcomeWithIdView {
                                    proof: [],
                                    block_hash: `BXPB6DQGmBrjARvcgYwS8qKLkyto6dk9NfawGSmfjE9Q`,
                                    id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
                                    outcome: ExecutionOutcomeView {
                                        logs: [],
                                        receipt_ids: [
                                            `8vJ1QWM4pffRDnW3c5CxFFV5cMx8wiqxsAqmZTitHvfh`,
                                        ],
                                        gas_burnt: 424555062500,
                                        tokens_burnt: 424555062500000000000,
                                        executor_id: "some.test.near",
                                        status: SuccessValue(``),
                                    },
                                },
                                receipt: ReceiptView {
                                    predecessor_id: "test.near",
                                    receiver_id: "some.test.near",
                                    receipt_id: `CbWu7WYYbYbn3kThs5gcxANrxy7AKLcMcBLxLw8Zq1Fz`,
                                    receipt: Action {
                                        signer_id: "test.near",
                                        signer_public_key: ed25519:8NA7mh6TAWzy2qz68bHp62QHTEQ6nJLfiYeKDRwEbU3X,
                                        gas_price: 1030000000,
                                        output_data_receivers: [],
                                        input_data_ids: [],
                                        actions: [
                                            CreateAccount,
                                            Transfer {
                                                deposit: 40000000000000000000000000,
                                            },
                                            AddKey {
                                                public_key: ed25519:2syGhqwJ8ba2nUGmP9tkZn9m1DYZPYYobpufiERVnug8,
                                                access_key: AccessKeyView {
                                                    nonce: 0,
                                                    permission: FullAccess,
                                                },
                                            },
                                        ],
                                    },
                                },
                            },
                        ],
                        state_changes: [
                            StateChangeWithCauseView {
                                cause: ValidatorAccountsUpdate,
                                value: AccountUpdate {
                                    account_id: "test.near",
                                    account: AccountView {
                                        amount: 1000000000000000000000000000000000,
                                        locked: 50000000000000000000000000000000,
                                        code_hash: `11111111111111111111111111111111`,
                                        storage_usage: 182,
                                        storage_paid_at: 0,
                                    },
                                },
                            },
                        ],
                    },
                ],
            }
        */

        info!(
            target: "borealis_indexer",
            "Message producer loop executed: message received\n"
        );

        // Stream message to NATS
        match context.msg_format {
            MsgFormat::Cbor => loop {
                let nats_connection = actual_connection_rx.borrow().clone();
                debug!(target: "borealis_indexer", "Message Producer [CBOR bytes vector]: Current Connection: NATS Connection: {:?}", &nats_connection);

                let result = nats_connection.connection.as_ref().unwrap()
                    .publish(
                        context.subject.as_str(),
                        BorealisMessage::new(streamer_message.block.header.height,
                            if let Some(compression_mode) = context.payload_compression {
                                let payload_bytes = serde_json::to_vec(&streamer_message).unwrap();
                                let (payload_bytes_compressed, _payload_len) = match compression_mode {
                                    CompressionMode::Lz4f => BorealisMessage::<Vec<u8>>::payload_compress_lz4(&payload_bytes).unwrap(),
                                    CompressionMode::Zstd => BorealisMessage::<Vec<u8>>::payload_compress_zstd(&payload_bytes).unwrap(),
                                };
                                payload_bytes_compressed
                            } else {
                                serde_json::to_vec(&streamer_message).unwrap()
                            }
                        )
                        .to_cbor()
                        .unwrap(),
                    );

                match &result {
                    Ok(()) => {
                        debug!(target: "borealis_indexer", "Message Producer [CBOR bytes vector]: Actual Connection: NATS Connection: {:?}", &nats_connection);
                        drop(result);
                        drop(nats_connection);
                        break;
                    }
                    Err(error) => {
                        error!(target: "borealis_indexer", "Message Producer [CBOR bytes vector]: Message passing error, NATS connection error or wrong credentials: {:?}", error);
                        connection_event_tx
                            .send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                            .await
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Message Producer [CBOR bytes vector]: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
                            );
                        drop(error);
                        drop(result);
                        drop(nats_connection);
                        tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                        continue;
                    }
                }
            },
            MsgFormat::Json => loop {
                let nats_connection = actual_connection_rx.borrow().clone();
                debug!(target: "borealis_indexer", "Message Producer [JSON bytes vector]: Current Connection: NATS Connection: {:?}", &nats_connection);

                let result = nats_connection.connection.as_ref().unwrap()
                    .publish(
                        context.subject.as_str(),
                        BorealisMessage::new(
                            streamer_message.block.header.height,
                            if let Some(compression_mode) = context.payload_compression {
                                let payload_bytes = serde_json::to_vec(&streamer_message).unwrap();
                                let (payload_bytes_compressed, _payload_len) = match compression_mode {
                                    CompressionMode::Lz4f => BorealisMessage::<Vec<u8>>::payload_compress_lz4(&payload_bytes).unwrap(),
                                    CompressionMode::Zstd => BorealisMessage::<Vec<u8>>::payload_compress_zstd(&payload_bytes).unwrap(),
                                };
                                payload_bytes_compressed
                            } else { 
                                serde_json::to_vec(&streamer_message).unwrap()
                            }
                        )
                        .to_json_bytes()
                        .unwrap(),
                    );

                match &result {
                    Ok(()) => {
                        debug!(target: "borealis_indexer", "Message Producer [JSON bytes vector]: Actual Connection: NATS Connection: {:?}", &nats_connection);
                        drop(result);
                        drop(nats_connection);
                        break;
                    }
                    Err(error) => {
                        error!(target: "borealis_indexer", "Message Producer [JSON bytes vector]: Message passing error, NATS connection error or wrong credentials: {:?}", error);
                        connection_event_tx
                            .send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                            .await
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Message Producer [JSON bytes vector]: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
                            );
                        drop(error);
                        drop(result);
                        drop(nats_connection);
                        tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                        continue;
                    }
                }
            },
        }

        // Data handling from `StreamerMessage` data structure. For custom filtering purposes.
        // Same as: jq '{block_height: .block.header.height, block_hash: .block.header.hash, block_header_chunk: .block.chunks[0], shard_chunk_header: .shards[0].chunk.header, transactions: .shards[0].chunk.transactions, receipts: .shards[0].chunk.receipts, receipt_execution_outcomes: .shards[0].receipt_execution_outcomes, state_changes: .state_changes}'

        info!(
            target: "borealis_indexer",
            "block_height: #{}, block_hash: {}\n",
            &streamer_message.block.header.height,
            &streamer_message.block.header.hash
        );

        if let Some(VerbosityLevel::WithStreamerMessageDump)
        | Some(VerbosityLevel::WithStreamerMessageParse) = verbosity_level
        {
            println!(
                "streamer_message: {}\n",
                serde_json::to_string_pretty(&streamer_message).unwrap()
            );
            println!(
                "streamer_message: {}\n",
                serde_json::to_string(&streamer_message).unwrap()
            );
        };

        if let Some(VerbosityLevel::WithStreamerMessageParse) = verbosity_level {
            println!(
                "streamer_message: {}\n",
                serde_json::to_value(&streamer_message).unwrap()
            );
            println!(
                "streamer_message: {:?}\n",
                cbor::to_vec(&streamer_message).unwrap()
            );

            println!(
                "block_header: {}\n",
                serde_json::to_value(&streamer_message.block.header).unwrap()
            );
            println!(
                "block_header: {:?}\n",
                cbor::to_vec(&streamer_message.block.header).unwrap()
            );

            println!(
                "block_header_chunks#: {}\n",
                streamer_message.block.chunks.len()
            );
            streamer_message.block.chunks.iter().for_each(|chunk| {
                println!(
                    "block_header_chunk: {}\n",
                    serde_json::to_value(&chunk).unwrap()
                );
                println!("block_header_chunk: {:?}\n", cbor::to_vec(&chunk).unwrap());
            });

            println!("shards#: {}\n", streamer_message.shards.len());
            streamer_message.shards.iter().for_each(|shard| {
                if let Some(chunk) = &shard.chunk {
                    println!(
                        "shard_chunk_header: {}\n",
                        serde_json::to_value(&chunk.header).unwrap()
                    );
                    println!(
                        "shard_chunk_header: {:?}\n",
                        cbor::to_vec(&chunk.header).unwrap()
                    );

                    println!("shard_chunk_transactions#: {}\n", chunk.transactions.len());
                    println!(
                        "shard_chunk_transactions: {}\n",
                        serde_json::to_value(&chunk.transactions).unwrap()
                    );
                    println!(
                        "shard_chunk_transactions: {:?}\n",
                        cbor::to_vec(&chunk.transactions).unwrap()
                    );

                    println!("shard_chunk_receipts#: {}\n", chunk.receipts.len());
                    println!(
                        "shard_chunk_receipts: {}\n",
                        serde_json::to_value(&chunk.receipts).unwrap()
                    );
                    println!(
                        "shard_chunk_receipts: {:?}\n",
                        cbor::to_vec(&chunk.receipts).unwrap()
                    );
                } else {
                    println!("shard_chunk_header: None\n");

                    println!("shard_chunk_transactions#: None\n");
                    println!("shard_chunk_transactions: None\n");

                    println!("shard_chunk_receipts#: None\n");
                    println!("shard_chunk_receipts: None\n");
                };

                println!(
                    "shard_receipt_execution_outcomes#: {}\n",
                    shard.receipt_execution_outcomes.len()
                );
                println!(
                    "shard_receipt_execution_outcomes: {}\n",
                    serde_json::to_value(&shard.receipt_execution_outcomes).unwrap()
                );
                println!(
                    "shard_receipt_execution_outcomes: {:?}\n",
                    cbor::to_vec(&shard.receipt_execution_outcomes).unwrap()
                );

                println!("StateChanges#: {}\n", shard.state_changes.len());
                shard.state_changes.iter().for_each(|state_change| {
                    println!(
                        "StateChange: {}\n",
                        serde_json::to_value(&state_change).unwrap()
                    );
                    println!("StateChange: {:?}\n", cbor::to_vec(&state_change).unwrap());
                });
            });
        };
    }
}

#[derive(Debug, Clone, Copy)]
enum ConnectionEvent
where
    Self: Send + Sync,
{
    NewConnectionRequest(usize),
    ConnectionReestablished(usize),
    ConnectionLost(usize),
    ConnectionClosed(usize),
}

impl ConnectionEvent
where
    Self: Send + Sync,
{
    async fn events_processing(
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
        mut connection_event_rx: mpsc::Receiver<ConnectionEvent>,
        actual_connection_tx: watch::Sender<NATSConnection>,
        actual_connection_rx: watch::Receiver<NATSConnection>,
        connect_args: RunArgs,
    ) {
        while let Some(event) = connection_event_rx.recv().await {
            match event {
                ConnectionEvent::NewConnectionRequest(cid) => {
                    info!(target: "borealis_indexer", "New connection has been requested, creation of new connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_indexer", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_indexer", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
                                    drop(error);
                                    drop(result);
                                    drop(nats_connection);
                                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                                    continue;
                                }
                            }
                        } else {
                            drop(nats_connection);
                            break;
                        }
                    }
                }
                ConnectionEvent::ConnectionReestablished(cid) => {
                    info!(target: "borealis_indexer", "Connection has been reestablished, checking current connection is active and workable, otherwise creation of new connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_indexer", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_indexer", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
                                    drop(error);
                                    drop(result);
                                    drop(nats_connection);
                                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                                    continue;
                                }
                            }
                        } else {
                            drop(nats_connection);
                            break;
                        }
                    }
                }
                ConnectionEvent::ConnectionLost(cid) => {
                    info!(target: "borealis_indexer", "Connection has been lost, retrieving connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_indexer", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_indexer", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
                                    drop(error);
                                    drop(result);
                                    drop(nats_connection);
                                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                                    continue;
                                }
                            }
                        } else {
                            drop(nats_connection);
                            break;
                        }
                    }
                }
                ConnectionEvent::ConnectionClosed(cid) => {
                    info!(target: "borealis_indexer", "Connection has been closed, retrieving connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_indexer", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_indexer", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_indexer", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
                                    drop(error);
                                    drop(result);
                                    drop(nats_connection);
                                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                                    continue;
                                }
                            }
                        } else {
                            drop(nats_connection);
                            break;
                        }
                    }
                }
            }
        }
    }

    fn events_processing_check(
        actual_connection_receiver: watch::Receiver<NATSConnection>,
        connection_event_sender: mpsc::Sender<ConnectionEvent>,
    ) {
        loop {
            let nats_connection = actual_connection_receiver.borrow().clone();
            debug!(target: "borealis_indexer", "Events Processing Check: Current Connection: NATS Connection: {:?}", &nats_connection);
            let result = nats_connection.nats_check_connection();
            match &result {
                Ok(()) => {
                    debug!(target: "borealis_indexer", "Events Processing Check: Actual Connection: NATS Connection: {:?}", &nats_connection);
                    drop(result);
                    drop(nats_connection);
                    break;
                }
                Err(error) => {
                    error!(target: "borealis_indexer", "Events Processing Check: NATS connection error or wrong credentials: {:?}", error);
                    connection_event_sender
                        .blocking_send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Events Processing Check: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
                        );
                    drop(error);
                    drop(result);
                    drop(nats_connection);
                    std::thread::sleep(core::time::Duration::from_millis(500));
                    continue;
                }
            }
        }
    }
}

static CID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
struct NATSConnection
where
    Self: Send + Sync,
{
    cid: usize,
    connection: Option<nats::Connection>,
}

impl NATSConnection
where
    Self: Send + Sync,
{
    fn new() -> NATSConnection {
        let cid = CID.load(Ordering::SeqCst);

        NATSConnection {
            cid,
            connection: None,
        }
    }

    /// Create options for connection to Borealis NATS Bus
    fn options(
        cid: usize,
        connect_args: RunArgs,
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> nats::Options {

        let connection_reestablished_event = connection_event_tx.clone();
        let connection_lost_event = connection_event_tx.clone();
        let connection_closed_event = connection_event_tx.clone();

        let creds_path = connect_args
            .creds_path
            .unwrap_or(std::path::PathBuf::from("./.nats/seed/nats.creds"));

        let options = match (
            connect_args.root_cert_path,
            connect_args.client_cert_path,
            connect_args.client_private_key,
        ) {
            (Some(root_cert_path), None, None) => nats::Options::with_credentials(creds_path)
                .with_name("Borealis Indexer [TLS, Server Auth]")
                .tls_required(true)
                .add_root_certificate(root_cert_path)
                .reconnect_buffer_size(256 * 1024 * 1024)
                .max_reconnects(1)
                .reconnect_delay_callback(|reconnect_try| {
                    let reconnect_attempt = {
                        if reconnect_try == 0 {
                            1_usize
                        } else {
                            reconnect_try
                        }
                    };
                    let delay = core::time::Duration::from_millis(std::cmp::min(
                        (reconnect_attempt
                            * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                            as u64,
                        1000,
                    ));
                    info!(
                        target: "borealis_indexer",
                        "Reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .reconnect_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been reestablished...");
                    connection_reestablished_event
                        .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .disconnect_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been lost...");
                    connection_lost_event
                        .blocking_send(ConnectionEvent::ConnectionLost(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .close_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been closed...");
                    connection_closed_event
                        .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                }),
            (Some(root_cert_path), Some(client_cert_path), Some(client_private_key)) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Indexer [TLS, Server Auth, Client Auth]")
                    .tls_required(true)
                    .add_root_certificate(root_cert_path)
                    .client_cert(client_cert_path, client_private_key)
                    .reconnect_buffer_size(256 * 1024 * 1024)
                    .max_reconnects(1)
                    .reconnect_delay_callback(|reconnect_try| {
                        let reconnect_attempt = {
                            if reconnect_try == 0 {
                                1_usize
                            } else {
                                reconnect_try
                            }
                        };
                        let delay = core::time::Duration::from_millis(std::cmp::min(
                            (reconnect_attempt
                                * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                                as u64,
                            1000,
                        ));
                        info!(
                            target: "borealis_indexer",
                            "Reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .reconnect_callback(move || {
                        info!(target: "borealis_indexer", "Connection has been reestablished...");
                        connection_reestablished_event
                            .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
                    .disconnect_callback(move || {
                        info!(target: "borealis_indexer", "Connection has been lost...");
                        connection_lost_event
                            .blocking_send(ConnectionEvent::ConnectionLost(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
                    .close_callback(move || {
                        info!(target: "borealis_indexer", "Connection has been closed...");
                        connection_closed_event
                            .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
            },
            _ => nats::Options::with_credentials(creds_path)
                .with_name("Borealis Indexer [NATS Connection, without TLS]")
                .reconnect_buffer_size(256 * 1024 * 1024)
                .max_reconnects(1)
                .reconnect_delay_callback(|reconnect_try| {
                    let reconnect_attempt = {
                        if reconnect_try == 0 {
                            1_usize
                        } else {
                            reconnect_try
                        }
                    };
                    let delay = core::time::Duration::from_millis(std::cmp::min(
                        (reconnect_attempt
                            * rand::Rng::gen_range(&mut rand::thread_rng(), 100..1000))
                            as u64,
                        1000,
                    ));
                    info!(
                        target: "borealis_indexer",
                        "Reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .reconnect_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been reestablished...");
                    connection_reestablished_event
                        .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .disconnect_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been lost...");
                    connection_lost_event
                        .blocking_send(ConnectionEvent::ConnectionLost(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .close_callback(move || {
                    info!(target: "borealis_indexer", "Connection has been closed...");
                    connection_closed_event
                        .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_indexer", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                }),
        };
        options
    }

    /// Create connection to Borealis NATS Bus
    fn connect(
        connect_args: RunArgs,
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<Self, Error> {
        let connection_id = CID.fetch_add(1, Ordering::SeqCst);
        let cid = CID.load(Ordering::SeqCst);

        let connection_options =
            NATSConnection::options(cid, connect_args.to_owned(), connection_event_tx);

        let result = connection_options.connect(connect_args.nats_server.as_str());

        match result {
            Ok(nats_connection) => {
                match nats_connection.flush_timeout(core::time::Duration::from_millis(10000)) {
                    Ok(()) => {
                        debug!(target: "borealis_indexer", "Connect: CID: {}, {}; NATS Connection: {:?}", connection_id, cid, &nats_connection);
                        Ok(Self {
                            cid,
                            connection: Some(nats_connection),
                        })
                    }
                    Err(error) => {
                        error!(target: "borealis_indexer", "Connect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {}; NATS Connection: {:?}", error, connection_id, cid, &nats_connection);
                        nats_connection.close();
                        Err(format!("Connect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {};", error, connection_id, cid).into())
                    }
                }
            }
            Err(error) => {
                error!(target: "borealis_indexer", "Connect: NATS connection error or wrong credentials: {:?}", error);
                Err(format!("Connect: NATS connection error or wrong credentials: {:?}", error).into())
            }
        }
    }

    /// Use already existed connection to Borealis NATS Bus or recreate new connection to prevent connection issues
    fn try_connect(
        &self,
        connect_args: RunArgs,
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<Self, Error> {
        if let Ok(()) = self.connection.as_ref().unwrap().flush_timeout(core::time::Duration::from_millis(10000)) {
            debug!(target: "borealis_indexer", "Reconnect: NATS Connection: {:?}", self.clone());
            Ok(self.clone())
        } else {
            let connection_id = CID.fetch_add(1, Ordering::SeqCst);
            let cid = CID.load(Ordering::SeqCst);

            let connection_options =
                NATSConnection::options(cid, connect_args.to_owned(), connection_event_tx);

            let result = connection_options.connect(connect_args.nats_server.as_str());

            match result {
                Ok(nats_connection) => {
                    match nats_connection.flush_timeout(core::time::Duration::from_millis(10000)) {
                        Ok(()) => {
                            debug!(target: "borealis_indexer", "Reconnect: CID: {}, {}; NATS Connection: {:?}", connection_id, cid, &nats_connection);
                            Ok(Self {
                                cid,
                                connection: Some(nats_connection),
                            })
                        }
                        Err(error) => {
                            error!(target: "borealis_indexer", "Reconnect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {}; NATS Connection: {:?}", error, connection_id, cid, &nats_connection);
                            nats_connection.close();
                            Err(format!("Reconnect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {};", error, connection_id, cid).into())
                        }
                    }
                }
                Err(error) => {
                    error!(target: "borealis_indexer", "Reconnect: NATS connection error or wrong credentials: {:?}", error);
                    Err(format!("Reconnect: NATS connection error or wrong credentials: {:?}", error).into())
                }
            }
        }
    }

    /// Check connection to Borealis NATS Bus
    fn nats_check_connection(&self) -> Result<(), Error> {
        let nats_connection = self.connection.as_ref().unwrap();
        debug!(target: "borealis_indexer", "Check Connection: NATS Connection: {:?}", self.clone());
        let result = nats_connection.flush_timeout(core::time::Duration::from_millis(10000));
        match result {
            Ok(()) => {
                info!(target: "borealis_indexer", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
                info!(target: "borealis_indexer", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
                info!(target: "borealis_indexer", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
                info!(target: "borealis_indexer", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
                Ok(())
            }
            Err(error) => {
                error!(target: "borealis_indexer", "Check Connection: NATS connection error or wrong credentials: {:?}", error);
                Err(format!("Check Connection: NATS connection error or wrong credentials: {:?}", error).into())
            }
        }
    }
}

static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

fn events_processing_rt(verbosity_level: Option<VerbosityLevel>) -> Result<Runtime, Error> {
    let events_processing_rt = {
        if let Some(VerbosityLevel::WithRuntimeThreadsDump) = verbosity_level {
            let events_processing_rt = Builder::new_multi_thread()
                .enable_all()
                .thread_name_fn( || {
                    let thread_id = THREAD_ID.fetch_add(1, Ordering::SeqCst);
                    format!("connection-events-processing-{}", thread_id)
                })
                .on_thread_start( || {
                    debug!(target: "borealis_indexer", "NATS connection events processing runtime: thread starting");
                })
                .on_thread_stop( || {
                    debug!(target: "borealis_indexer", "NATS connection events processing runtime: thread stopping");
                })
                .on_thread_park( || {
                    debug!(target: "borealis_indexer", "NATS connection events processing runtime: thread parking and going idle");
                })
                .on_thread_unpark( || {
                    debug!(target: "borealis_indexer", "NATS connection events processing runtime: thread unparked and starts executing tasks");
                })
                .build()?;

            events_processing_rt
        } else {
            let events_processing_rt = Builder::new_multi_thread()
                .enable_all()
                .thread_name_fn(|| {
                    let thread_id = THREAD_ID.fetch_add(1, Ordering::SeqCst);
                    format!("connection-events-processing-{}", thread_id)
                })
                .build()?;

            events_processing_rt
        }
    };
    Ok(events_processing_rt)
}

fn messages_processing_rt(verbosity_level: Option<VerbosityLevel>) -> Result<Runtime, Error> {
    let messages_processing_rt = {
        if let Some(VerbosityLevel::WithRuntimeThreadsDump) = verbosity_level {
            let messages_processing_rt = Builder::new_multi_thread()
                .enable_all()
                .thread_name_fn( || {
                    let thread_id = THREAD_ID.fetch_add(1, Ordering::SeqCst);
                    format!("streamer-messages-processing-{}", thread_id)
                })
                .on_thread_start( || {
                    debug!(target: "borealis_indexer", "Streamer Messages processing runtime: thread starting");
                })
                .on_thread_stop( || {
                    debug!(target: "borealis_indexer", "Streamer Messages processing runtime: thread stopping");
                })
                .on_thread_park( || {
                    debug!(target: "borealis_indexer", "Streamer Messages processing runtime: thread parking and going idle");
                })
                .on_thread_unpark( || {
                    debug!(target: "borealis_indexer", "Streamer Messages processing runtime: thread unparked and starts executing tasks");
                })
                .build()?;

            messages_processing_rt
        } else {
            let messages_processing_rt = Builder::new_multi_thread()
                .enable_all()
                .thread_name_fn(|| {
                    let thread_id = THREAD_ID.fetch_add(1, Ordering::SeqCst);
                    format!("streamer-messages-processing-{}", thread_id)
                })
                .build()?;

            messages_processing_rt
        }
    };
    Ok(messages_processing_rt)
}

fn main() -> Result<(), Error> {
    // restart of system in case of stop or error returned, due to run-time panic in a thread
    loop {
        // Search for the root certificates to perform HTTPS/TLS calls
        // for downloading genesis and config files
        openssl_probe::init_ssl_cert_env_vars();

        // Initialize logging
        init_logging();

        // Parse CLI options
        let opts: Opts = Opts::parse();

        // let home_dir = opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
        let home_dir = opts
            .home_dir
            .unwrap_or(std::path::PathBuf::from("./.borealis-indexer"));

        let (connection_event_tx, connection_event_rx) = 
            mpsc::channel::<ConnectionEvent>(1000);
        let (actual_connection_tx, actual_connection_rx) =
            watch::channel::<NATSConnection>(NATSConnection::new());

        let connection_event_sender = connection_event_tx.clone();
        let actual_connection_receiver = actual_connection_tx.subscribe();

        if let SubCommand::Check(run_args) | SubCommand::Run(run_args) = opts.subcmd.clone() {
            loop {
                let result = NATSConnection::connect(run_args.to_owned(), connection_event_tx.clone());
                match &result {
                    Ok(nats_connection) => {
                        debug!(target: "borealis_indexer", "Main(): Connect with extended options: NATS Connection: {:?}", nats_connection);
                        actual_connection_tx.send(nats_connection.clone())
                            .unwrap_or_else(|error|
                                error!(target: "borealis_indexer", "Main(): Connect with extended options: NATS Connection with CID {} send error: {:?}", nats_connection.cid, error)
                            );
                        drop(nats_connection);
                        drop(result);
                        break;
                    }
                    Err(error) => {
                        error!(target: "borealis_indexer", "Main(): Connect with extended options: NATS connection error or wrong credentials: {:?}", error);
                        drop(error);
                        drop(result);
                        std::thread::sleep(core::time::Duration::from_millis(500));
                        continue;
                    }
                }
            }
        };

        match opts.subcmd {
            SubCommand::Check(run_args) => {
                let events_processing_rt = actix::System::with_tokio_rt(||
                    events_processing_rt(opts.verbose.clone())
                    .expect("Main(): Check(): Run-time error returned while creating Indexer's custom Tokio run-time for Actix")
                );

                events_processing_rt.block_on(async move {
                    // Unix signals and key sequence handlers
                    actix::spawn(async move {
                        key_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        hup_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        term_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        kill_switch_usr1().await.unwrap();
                    });
                    actix::spawn(async move {
                        kill_switch_usr2().await.unwrap();
                    });

                    actix::spawn(async move {
                        ConnectionEvent::events_processing(
                            connection_event_tx,
                            connection_event_rx,
                            actual_connection_tx,
                            actual_connection_rx,
                            run_args,
                        )
                        .await;
                    });

                    ConnectionEvent::events_processing_check(
                        actual_connection_receiver.clone(),
                        connection_event_sender.clone(),
                    );

                });
                events_processing_rt.run()
                    .unwrap_or_else(|error|
                        error!(target: "borealis_indexer", "Main(): Check(): Indexer's connection checking events processing loop returned run-time error: {:?}", error)
                    );
            }
            SubCommand::Init(config_args) => {
                near_indexer::indexer_init_configs(&home_dir, config_args.into())
                    .expect("Main(): Init(): Error while creating Indexer's initial configuration files");
                break;
            }
            SubCommand::Run(run_args) => {
                let indexer_config = near_indexer::IndexerConfig {
                    home_dir,
                    // recover and continue message streaming from latest synced block (real-time), or from interruption, or from exact block height
                    sync_mode: match run_args.sync_mode {
                        SyncMode::LatestSynced => near_indexer::SyncModeEnum::LatestSynced,
                        SyncMode::FromInterruption => near_indexer::SyncModeEnum::FromInterruption,
                        SyncMode::BlockHeight => {
                            near_indexer::SyncModeEnum::BlockHeight(run_args.block_height.unwrap_or(0))
                        }
                    },
                    // waiting for full sync or stream messages while syncing
                    await_for_node_synced: match run_args.await_synced {
                        AwaitSynced::WaitForFullSync => {
                            near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
                        }
                        AwaitSynced::StreamWhileSyncing => {
                            near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing
                        }
                    },
                };

                let connect_args = run_args.clone();

                let messages_processing_rt = actix::System::with_tokio_rt(||
                    messages_processing_rt(opts.verbose.clone())
                    .expect("Main(): Run(): Run-time error returned while creating Indexer's custom Tokio run-time for Actix")
                );

                messages_processing_rt.block_on(async move {
                    // Unix signals and key sequence handlers
                    actix::spawn(async move {
                        key_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        hup_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        term_switch().await.unwrap();
                    });
                    actix::spawn(async move {
                        kill_switch_usr1().await.unwrap();
                    });
                    actix::spawn(async move {
                        kill_switch_usr2().await.unwrap();
                    });

                    actix::spawn(async move {
                        ConnectionEvent::events_processing(
                            connection_event_tx,
                            connection_event_rx,
                            actual_connection_tx,
                            actual_connection_rx,
                            connect_args,
                        )
                        .await;
                    });

                    ConnectionEvent::events_processing_check(
                        actual_connection_receiver.clone(),
                        connection_event_sender.clone(),
                    );

                    let indexer = near_indexer::Indexer::new(indexer_config)
                        .expect("Main(): Run(): Error while creating Indexer's instance");

                    let events_stream = indexer.streamer();

                    actix::spawn(async move {
                        message_producer(
                            events_stream,
                            actual_connection_receiver.clone(),
                            connection_event_sender.clone(),
                            run_args.clone(),
                            opts.verbose,
                        )
                        .await;
                    });

                });
                messages_processing_rt.run()
                    .unwrap_or_else(|error|
                        error!(target: "borealis_indexer", "Main(): Run(): Indexer's messages processing loop returned run-time error: {:?}", error)
                    );
            }
        };
        if let 602437500 | 15 | 12 | 10 = SIGNAL.load(Ordering::SeqCst) {
            break;
        }
    } // restart of system in case of stop or error returned, due to run-time panic in a thread
//  Graceful shutdown for all tasks (futures, green threads) currently executed on existed run-time thread-pools
    info!(target: "borealis_indexer", "Shutdown process within 10 seconds...");
//  messages_processing_rt.shutdown_timeout(core::time::Duration::from_secs(10));
//  events_processing_rt.shutdown_timeout(core::time::Duration::from_secs(10));
    std::thread::sleep(core::time::Duration::from_millis(10000));
    Ok(())
}
