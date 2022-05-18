use actix;
use clap::Parser;
use cli::{
    init_logging, Error, Opts, Context, SubCommand, VerbosityLevel,
};
use core::sync::atomic::{AtomicUsize, Ordering};
use nats;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::runtime::{Runtime, Builder};
use tokio::signal::{ctrl_c, unix::{signal, SignalKind}};
use tokio::sync::{mpsc, watch};
use tracing:: {info, error, debug};

pub mod cli;

static SIGNAL: AtomicUsize = AtomicUsize::new(0);

async fn kill_switch_usr1() -> Result<(), Error> {
    let mut kill_signal_stream = signal(SignalKind::from_raw(10))?;
    info!(
        target: "borealis_banhammer_run_time",
        "Kill signal (USR1) handler installed\n"
    );
    while let Some(()) = kill_signal_stream.recv().await {
        info!(
            target: "borealis_banhammer_run_time",
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
        target: "borealis_banhammer_run_time",
        "Kill signal (USR2) handler installed\n"
    );
    while let Some(()) = kill_signal_stream.recv().await {
        info!(
            target: "borealis_banhammer_run_time",
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
        target: "borealis_banhammer_run_time",
        "Terminate signal handler installed\n"
    );
    while let Some(()) = term_signal_stream.recv().await {
        info!(
            target: "borealis_banhammer_run_time",
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
        target: "borealis_banhammer_run_time",
        "Hangup signal handler installed\n"
    );
    while let Some(()) = hup_signal_stream.recv().await {
        info!(
            target: "borealis_banhammer_run_time",
            "Hangup signal handler triggered\n"
        );
        SIGNAL.store(1, Ordering::SeqCst);
    }
    Ok(())
}

async fn key_switch() -> Result<(), Error> {
    info!(
        target: "borealis_banhammer_run_time",
        "Ctrl-C key sequence handler installed\n"
    );
    while let Ok(()) = ctrl_c().await {
        info!(
            target: "borealis_banhammer_run_time",
            "Ctrl-C key sequence handler triggered\n"
        );
        SIGNAL.store(602437500, Ordering::SeqCst);
        actix::System::current().stop();
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BanhammerBanEventMessage;
#[derive(Debug, Serialize, Deserialize, Clone)]
struct BanhammerConfigMessage;
#[derive(Debug, Serialize, Deserialize, Clone)]
struct RelayerMessage;
#[derive(Debug, Serialize, Deserialize, Clone)]
struct EthCallMessage;

async fn message_producer(
    mut events_stream: mpsc::Receiver<BanhammerBanEventMessage>,
    actual_connection_rx: watch::Receiver<NATSConnection>,
    connection_event_tx: mpsc::Sender<ConnectionEvent>,
    context: Context,
    verbosity_level: Option<VerbosityLevel>,
) {
    info!(
        target: "borealis_banhammer_nats",
        "Message producer loop starting: producing and streaming new ban event messages\n"
    );

    while let Some(ban_event_message) = events_stream.recv().await {
        info!(
            target: "borealis_banhammer_nats",
            "Message producer loop executed: ban event message received and will be transmitted to bus\n"
        );

        // Stream/transmit ban event message to NATS bus
        loop {
            let nats_connection = actual_connection_rx.borrow().clone();
            debug!(target: "borealis_banhammer_nats", "Message Producer [JSON bytes vector]: Current Connection: NATS Connection: {:?}", &nats_connection);

            let result = nats_connection.connection.as_ref().unwrap()
                .publish(
                    context.tx_subject.as_str(),
                    serde_json::to_vec(&ban_event_message).unwrap()
                );

            match &result {
                Ok(()) => {
                    debug!(target: "borealis_banhammer_nats", "Message Producer [JSON bytes vector]: Actual Connection: NATS Connection: {:?}", &nats_connection);
                    drop(result);
                    drop(nats_connection);
                    break;
                }
                Err(error) => {
                    error!(target: "borealis_banhammer_nats", "Message Producer [JSON bytes vector]: Message passing error, NATS connection error or wrong credentials: {:?}", error);
                    connection_event_tx
                        .send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                        .await
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Message Producer [JSON bytes vector]: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
                        );
                    drop(error);
                    drop(result);
                    drop(nats_connection);
                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                    continue;
                }
            }
        };

        // Print `BanhammerBanEventMessage` data structure for debug purposes.
        if let Some(VerbosityLevel::WithNATSMessagesDump) = verbosity_level {
            debug!(
                target: "borealis_banhammer_nats",
                "Banhammer ban event message: {}\n",
                serde_json::to_string_pretty(&ban_event_message).unwrap()
            );
        };
    }
}

async fn relayer_message_consumer(
    relayer_message_stream_tx: mpsc::Sender<RelayerMessage>,
    actual_connection_rx: watch::Receiver<NATSConnection>,
    connection_event_tx: mpsc::Sender<ConnectionEvent>,
    context: Context,
    verbosity_level: Option<VerbosityLevel>,
) {
    let mut error_rate = 0;
    loop {
        let nats_connection = actual_connection_rx.borrow().clone();
        debug!(target: "borealis_banhammer_nats", "Message Consumer [Relayer Message]: Current Connection: NATS Connection: {:?}", &nats_connection);

        let subscription = nats_connection.connection.as_ref().unwrap()
            .subscribe(
                context.relayer_rx_subject.as_str(),
            );

        match &subscription {
            Ok(subscription) => {
                debug!(target: "borealis_banhammer_nats", "Message Consumer [Relayer Message]: Actual Connection: NATS Connection: {:?}", &nats_connection);
                loop {
                    info!(
                        target: "borealis_banhammer_nats",
                        "Message consumer loop started: listening for new relayer messages\n"
                    );

                    let message = subscription.next_timeout(std::time::Duration::from_millis(3000));

                    match message {
                        Ok(msg) => {
                            info!(target: "borealis_banhammer_nats", "Received message:\n{}", &msg);
                            let relayer_message = serde_json::from_slice::<RelayerMessage>(msg.data.as_ref()).unwrap();
                            // Print `RelayerMessage` data structure for debug purposes.
                            if let Some(VerbosityLevel::WithNATSMessagesDump) = verbosity_level {
                                debug!(
                                    target: "borealis_banhammer_nats",
                                    "Received relayer message: {}\n",
                                    serde_json::to_string_pretty(&relayer_message).unwrap()
                                );
                            };
                            relayer_message_stream_tx
                                .send(relayer_message)
                                .await
                                .unwrap_or_else(|error|
                                    error!(target: "borealis_banhammer_nats", "Message Consumer [Relayer Message]: Realyer message send error: {:?}", error)
                                );
                        },
                        Err(error) => {
                            error!(
                                target: "borealis_banhammer_nats",
                                "Message wasn't received within 3s timeframe: Error occured due to waiting timeout for message receiving was elapsed: {:?}",
                                error
                            );
                            error_rate+=1;
                            if error_rate < 10 {
                                continue;
                            } else {
                                error_rate = 0;
                                break;
                            };
                        },
                    };
                }
            },
            Err(error) => {
                error!(target: "borealis_banhammer_nats", "Message Consumer [Relayer Messages]: Subscription error: maybe wrong or nonexistent `--subject` name: {:?}", error);
                error_rate+=1;
                if error_rate < 10 {
                    continue;
                } else {
                    connection_event_tx
                        .send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                        .await
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Message Consumer [Relayer Messages]: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
                        );
                    error_rate = 0;
                    drop(error);
                    drop(subscription);
                    drop(nats_connection);
                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                    continue;
                };
            },
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
        connect_args: Context,
    ) {
        while let Some(event) = connection_event_rx.recv().await {
            match event {
                ConnectionEvent::NewConnectionRequest(cid) => {
                    info!(target: "borealis_banhammer_nats", "New connection has been requested, creation of new connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_banhammer_nats", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_banhammer_nats", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
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
                    info!(target: "borealis_banhammer_nats", "Connection has been reestablished, checking current connection is active and workable, otherwise creation of new connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_banhammer_nats", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_banhammer_nats", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
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
                    info!(target: "borealis_banhammer_nats", "Connection has been lost, retrieving connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_banhammer_nats", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_banhammer_nats", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
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
                    info!(target: "borealis_banhammer_nats", "Connection has been closed, retrieving connection...");
                    loop {
                        let nats_connection = actual_connection_rx.borrow().clone();
                        debug!(target: "borealis_banhammer_nats", "Events Processing: Current Connection: NATS Connection: {:?}", &nats_connection);
                        if cid == nats_connection.cid {
                            let result = nats_connection.try_connect(connect_args.to_owned(), connection_event_tx.clone());
                            match &result {
                                Ok(nats_connection_actual) => {
                                    debug!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection: {:?}", nats_connection_actual);
                                    actual_connection_tx.send(nats_connection_actual.clone())
                                        .unwrap_or_else(|error|
                                            error!(target: "borealis_banhammer_nats", "Events Processing: Actual Connection: NATS Connection with CID {} send error: {:?}", nats_connection_actual.cid, error)
                                        );
                                    drop(nats_connection_actual);
                                    drop(result);
                                    drop(nats_connection);
                                    break;
                                }
                                Err(error) => {
                                    error!(target: "borealis_banhammer_nats", "Events Processing: NATS connection error or wrong credentials: {:?}", error);
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
            debug!(target: "borealis_banhammer_nats", "Events Processing Check: Current Connection: NATS Connection: {:?}", &nats_connection);
            let result = nats_connection.nats_check_connection();
            match &result {
                Ok(()) => {
                    debug!(target: "borealis_banhammer_nats", "Events Processing Check: Actual Connection: NATS Connection: {:?}", &nats_connection);
                    drop(result);
                    drop(nats_connection);
                    break;
                }
                Err(error) => {
                    error!(target: "borealis_banhammer_nats", "Events Processing Check: NATS connection error or wrong credentials: {:?}", error);
                    connection_event_sender
                        .blocking_send(ConnectionEvent::NewConnectionRequest(nats_connection.cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Events Processing Check: New Connection Request: NATS Connection with CID {} event send error: {:?}", nats_connection.cid, error)
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
        connect_args: Context,
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
                .with_name("Borealis Banhammer [TLS, Server Auth]")
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
                        target: "borealis_banhammer_nats",
                        "Reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .reconnect_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been reestablished...");
                    connection_reestablished_event
                        .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .disconnect_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been lost...");
                    connection_lost_event
                        .blocking_send(ConnectionEvent::ConnectionLost(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .close_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been closed...");
                    connection_closed_event
                        .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                }),
            (Some(root_cert_path), Some(client_cert_path), Some(client_private_key)) => {
                nats::Options::with_credentials(creds_path)
                    .with_name("Borealis Banhammer [TLS, Server Auth, Client Auth]")
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
                            target: "borealis_banhammer_nats",
                            "Reconnection attempt #{} within delay of {:?} ...",
                            reconnect_attempt, delay
                        );
                        delay
                    })
                    .reconnect_callback(move || {
                        info!(target: "borealis_banhammer_nats", "Connection has been reestablished...");
                        connection_reestablished_event
                            .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
                    .disconnect_callback(move || {
                        info!(target: "borealis_banhammer_nats", "Connection has been lost...");
                        connection_lost_event
                            .blocking_send(ConnectionEvent::ConnectionLost(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
                    .close_callback(move || {
                        info!(target: "borealis_banhammer_nats", "Connection has been closed...");
                        connection_closed_event
                            .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                            .unwrap_or_else(|error|
                                error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                            );
                    })
            },
            _ => nats::Options::with_credentials(creds_path)
                .with_name("Borealis Banhammer [NATS Connection, without TLS]")
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
                        target: "borealis_banhammer_nats",
                        "Reconnection attempt #{} within delay of {:?} ...",
                        reconnect_attempt, delay
                    );
                    delay
                })
                .reconnect_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been reestablished...");
                    connection_reestablished_event
                        .blocking_send(ConnectionEvent::ConnectionReestablished(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .disconnect_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been lost...");
                    connection_lost_event
                        .blocking_send(ConnectionEvent::ConnectionLost(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                })
                .close_callback(move || {
                    info!(target: "borealis_banhammer_nats", "Connection has been closed...");
                    connection_closed_event
                        .blocking_send(ConnectionEvent::ConnectionClosed(cid))
                        .unwrap_or_else(|error|
                            error!(target: "borealis_banhammer_nats", "Connection with CID {} event send error: {:?}", cid, error)
                        );
                }),
        };
        options
    }

    /// Create connection to Borealis NATS Bus
    fn connect(
        connect_args: Context,
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
                        debug!(target: "borealis_banhammer_nats", "Connect: CID: {}, {}; NATS Connection: {:?}", connection_id, cid, &nats_connection);
                        Ok(Self {
                            cid,
                            connection: Some(nats_connection),
                        })
                    }
                    Err(error) => {
                        error!(target: "borealis_banhammer_nats", "Connect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {}; NATS Connection: {:?}", error, connection_id, cid, &nats_connection);
                        nats_connection.close();
                        Err(format!("Connect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {};", error, connection_id, cid).into())
                    }
                }
            }
            Err(error) => {
                error!(target: "borealis_banhammer_nats", "Connect: NATS connection error or wrong credentials: {:?}", error);
                Err(format!("Connect: NATS connection error or wrong credentials: {:?}", error).into())
            }
        }
    }

    /// Use already existed connection to Borealis NATS Bus or recreate new connection to prevent connection issues
    fn try_connect(
        &self,
        connect_args: Context,
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<Self, Error> {
        if let Ok(()) = self.connection.as_ref().unwrap().flush_timeout(core::time::Duration::from_millis(10000)) {
            debug!(target: "borealis_banhammer_nats", "Reconnect: NATS Connection: {:?}", self.clone());
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
                            debug!(target: "borealis_banhammer_nats", "Reconnect: CID: {}, {}; NATS Connection: {:?}", connection_id, cid, &nats_connection);
                            Ok(Self {
                                cid,
                                connection: Some(nats_connection),
                            })
                        }
                        Err(error) => {
                            error!(target: "borealis_banhammer_nats", "Reconnect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {}; NATS Connection: {:?}", error, connection_id, cid, &nats_connection);
                            nats_connection.close();
                            Err(format!("Reconnect: NATS connection error or connection waiting timeout elapsed: {:?}; CID: {}, {};", error, connection_id, cid).into())
                        }
                    }
                }
                Err(error) => {
                    error!(target: "borealis_banhammer_nats", "Reconnect: NATS connection error or wrong credentials: {:?}", error);
                    Err(format!("Reconnect: NATS connection error or wrong credentials: {:?}", error).into())
                }
            }
        }
    }

    /// Check connection to Borealis NATS Bus
    fn nats_check_connection(&self) -> Result<(), Error> {
        let nats_connection = self.connection.as_ref().unwrap();
        debug!(target: "borealis_banhammer_nats", "Check Connection: NATS Connection: {:?}", self.clone());
        let result = nats_connection.flush_timeout(core::time::Duration::from_millis(10000));
        match result {
            Ok(()) => {
                info!(target: "borealis_banhammer_nats", "round trip time (rtt) between this client and the current NATS server: {:?}", nats_connection.rtt());
                info!(target: "borealis_banhammer_nats", "this client IP address, as known by the current NATS server: {:?}", nats_connection.client_ip());
                info!(target: "borealis_banhammer_nats", "this client ID, as known by the current NATS server: {:?}", nats_connection.client_id());
                info!(target: "borealis_banhammer_nats", "maximum payload size the current NATS server will accept: {:?}", nats_connection.max_payload());
                Ok(())
            }
            Err(error) => {
                error!(target: "borealis_banhammer_nats", "Check Connection: NATS connection error or wrong credentials: {:?}", error);
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
                    debug!(target: "borealis_banhammer_run_time", "NATS connection events processing runtime: thread starting");
                })
                .on_thread_stop( || {
                    debug!(target: "borealis_banhammer_run_time", "NATS connection events processing runtime: thread stopping");
                })
                .on_thread_park( || {
                    debug!(target: "borealis_banhammer_run_time", "NATS connection events processing runtime: thread parking and going idle");
                })
                .on_thread_unpark( || {
                    debug!(target: "borealis_banhammer_run_time", "NATS connection events processing runtime: thread unparked and starts executing tasks");
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
                    debug!(target: "borealis_banhammer_run_time", "Streamer Messages processing runtime: thread starting");
                })
                .on_thread_stop( || {
                    debug!(target: "borealis_banhammer_run_time", "Streamer Messages processing runtime: thread stopping");
                })
                .on_thread_park( || {
                    debug!(target: "borealis_banhammer_run_time", "Streamer Messages processing runtime: thread parking and going idle");
                })
                .on_thread_unpark( || {
                    debug!(target: "borealis_banhammer_run_time", "Streamer Messages processing runtime: thread unparked and starts executing tasks");
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
        openssl_probe::init_ssl_cert_env_vars();

        // Initialize logging
        init_logging();

        // Parse CLI options
        let opts: Opts = Opts::parse();

        let home_dir = opts
            .home_dir
            .unwrap_or(std::path::PathBuf::from("./.borealis-banhammer"));

        // Channels for receiving relayer, buckets configuration and eth_call messages,
        // and channel for transmitting the Banhammer's ban event messages,
        // to/from NATS subjects
        let (ban_event_stream_tx, ban_event_stream_rx) = 
            mpsc::channel::<BanhammerBanEventMessage>(1000);
        let (config_message_stream_tx, config_message_stream_rx) = 
            mpsc::channel::<BanhammerConfigMessage>(1000);
        let (relayer_message_stream_tx, relayer_message_stream_rx) = 
            mpsc::channel::<RelayerMessage>(1000);
        let (eth_call_message_stream_tx, eth_call_message_stream_rx) = 
            mpsc::channel::<EthCallMessage>(1000);

        // Channels for sending/receiving NATS connection's events
        // and channel for sending/receiving actual, current or updated, NATS connection
        let (connection_event_tx, connection_event_rx) = 
            mpsc::channel::<ConnectionEvent>(1000);
        let (actual_connection_tx, actual_connection_rx) =
            watch::channel::<NATSConnection>(NATSConnection::new());

        // Channel rx/tx pair clones for checking of NATS connection events processing
        let connection_event_sender = connection_event_tx.clone();
        let actual_connection_receiver = actual_connection_tx.subscribe();

        // Channel rx/tx pair clones for NATS connection events sent to event processing,
        // and receiving actual NATS connection, in a Banhammer's ban event messages producer
        let connection_event_tx_for_bh_msg_pub = connection_event_tx.clone();
        let actual_connection_rx_for_bh_msg_pub = actual_connection_tx.subscribe();

        // Channel rx/tx pair clones for NATS connection events sent to event processing,
        // and receiving actual NATS connection, in a Banhammer's buckets configuration messages consumer
        let connection_event_tx_for_bh_conf_msg_sub = connection_event_tx.clone();
        let actual_connection_rx_for_bh_conf_msg_sub = actual_connection_tx.subscribe();

        // Channel rx/tx pair clones for NATS connection events sent to event processing,
        // and receiving actual NATS connection, in a Relayer's messages consumer
        let connection_event_tx_for_rlr_msg_sub = connection_event_tx.clone();
        let actual_connection_rx_for_rlr_msg_sub = actual_connection_tx.subscribe();

        // Channel rx/tx pair clones for NATS connection events sent to event processing,
        // and receiving actual NATS connection, in a eth_call messages consumer
        let connection_event_tx_for_ethcall_msg_sub = connection_event_tx.clone();
        let actual_connection_rx_for_ethcall_msg_sub = actual_connection_tx.subscribe();

        match opts.subcmd.clone() {
            SubCommand::Check(context) | SubCommand::Run(context) => {
                loop {
                    let result = NATSConnection::connect(context.clone(), connection_event_tx.clone());
                    match &result {
                        Ok(nats_connection) => {
                            debug!(target: "borealis_banhammer_nats", "Main(): Connect with extended options: NATS Connection: {:?}", nats_connection);
                            actual_connection_tx.send(nats_connection.clone())
                                .unwrap_or_else(|error|
                                    error!(target: "borealis_banhammer_nats", "Main(): Connect with extended options: NATS Connection with CID {} send error: {:?}", nats_connection.cid, error)
                                );
                            drop(nats_connection);
                            drop(result);
                            break;
                        }
                        Err(error) => {
                            error!(target: "borealis_banhammer_nats", "Main(): Connect with extended options: NATS connection error or wrong credentials: {:?}", error);
                            drop(error);
                            drop(result);
                            std::thread::sleep(core::time::Duration::from_millis(500));
                            continue;
                        }
                    }
                }
            }
        };

        match opts.subcmd {
            SubCommand::Check(context) => {

                let events_processing_rt = actix::System::with_tokio_rt(||
                    events_processing_rt(opts.verbose.clone())
                    .expect("Main(): Check(): Run-time error returned while creating Banhammer's custom Tokio run-time for Actix")
                );

                // NATS connection events processing run-time tasks
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

                    // NATS connection events processing
                    actix::spawn(async move {
                        ConnectionEvent::events_processing(
                            connection_event_tx,
                            connection_event_rx,
                            actual_connection_tx,
                            actual_connection_rx,
                            context.clone(),
                        )
                        .await;
                    });

                    // Checking of NATS connection events processing
                    ConnectionEvent::events_processing_check(
                        actual_connection_receiver.clone(),
                        connection_event_sender.clone(),
                    );

                });

                // Run NATS connection events processing run-time
                events_processing_rt.run()
                    .unwrap_or_else(|error|
                        error!(target: "borealis_banhammer_run_time", "Main(): Check(): Banhammer's connection checking events processing loop returned run-time error: {:?}", error)
                    );
            }
            SubCommand::Run(context) => {

                let messages_processing_rt = actix::System::with_tokio_rt(||
                    messages_processing_rt(opts.verbose.clone())
                    .expect("Main(): Run(): Run-time error returned while creating Banhammer's custom Tokio run-time for Actix")
                );

                let events_processing_context = context.clone();
                let message_producer_context = context.clone();
                let relayer_message_consumer_context = context.clone();

                // NATS messages processing run-time tasks
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

                    // NATS connection events processing
                    actix::spawn(async move {
                        ConnectionEvent::events_processing(
                            connection_event_tx,
                            connection_event_rx,
                            actual_connection_tx,
                            actual_connection_rx,
                            events_processing_context,
                        )
                        .await;
                    });

                    // Checking of NATS connection events processing
                    ConnectionEvent::events_processing_check(
                        actual_connection_receiver.clone(),
                        connection_event_sender.clone(),
                    );

                    // Banhammer's ban event messages producer
                    actix::spawn(async move {
                        message_producer(
                            ban_event_stream_rx,
                            actual_connection_rx_for_bh_msg_pub,
                            connection_event_tx_for_bh_msg_pub,
                            message_producer_context,
                            opts.verbose,
                        )
                        .await;
                    });

                    // Relayer messages consumer
                    actix::spawn(async move {
                        relayer_message_consumer(
                            relayer_message_stream_tx,
                            actual_connection_rx_for_rlr_msg_sub,
                            connection_event_tx_for_rlr_msg_sub,
                            relayer_message_consumer_context,
                            opts.verbose,
                        )
                        .await;
                    });

                });

                // Run NATS messages processing run-time
                messages_processing_rt.run()
                    .unwrap_or_else(|error|
                        error!(target: "borealis_banhammer_run_time", "Main(): Run(): Banhammer's messages processing loop returned run-time error: {:?}", error)
                    );
            }
        };
        if let 602437500 | 15 | 12 | 10 = SIGNAL.load(Ordering::SeqCst) {
            break;
        }
    } // restart of system in case of stop or error returned, due to run-time panic in a thread
//  Graceful shutdown for all tasks (futures, green threads) currently executed on existed run-time thread-pools
    info!(target: "borealis_banhammer", "Shutdown process within 10 seconds...");
//  messages_processing_rt.shutdown_timeout(core::time::Duration::from_secs(10));
//  events_processing_rt.shutdown_timeout(core::time::Duration::from_secs(10));
    std::thread::sleep(core::time::Duration::from_millis(10000));
    Ok(())
}
