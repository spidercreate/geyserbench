pub use {
    arpc::{
        arpc_service_client::ArpcServiceClient, SubscribeRequest as ArpcSubscribeRequest,
        SubscribeResponse as ArpcSubscribeResponse,
    },
    bs58,
    bytes::Bytes,
    futures_util::{sink::SinkExt, stream::StreamExt},
    serde::{Deserialize, Serialize},
    std::{
        collections::HashMap,
        env,
        fs::{self, OpenOptions},
        io::Write,
        path::Path,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    },
    tokio::{signal::ctrl_c, sync::broadcast, task},
    tokio_stream::Stream,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
        },
        prelude::{SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions},
        tonic::transport::ClientTlsConfig,
    },
};

pub mod arpc {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/arpc.rs"));

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("arpc_descriptor");
}

mod config;
use {config::*, std::collections::BTreeMap};

#[derive(Debug, Clone)]
pub struct TransactionData {
    timestamp: f64,

    signature: String,
    start_time: f64,
}

#[derive(Debug, Clone, Default)]
pub struct Comparator {
    pub data: HashMap<String, HashMap<String, TransactionData>>,
    worker_count: usize,
}

impl Comparator {
    pub fn add(&mut self, from: String, data: TransactionData) {
        self.data
            .entry(data.signature.clone())
            .or_insert_with(HashMap::new)
            .insert(from.clone(), data.clone());
        log::info!(
            "{}/{} total valid",
            self.get_valid_count(),
            CONFIG.config.transactions
        );
    }

    pub fn get_valid_count(&self) -> usize {
        self.data
            .values()
            .filter(|v| v.len() == self.worker_count)
            .count()
    }

    pub fn cleanup(&mut self) {
        self.data.retain(|_, v| v.len() == self.worker_count);
    }
}

lazy_static::lazy_static! {
    static ref CONFIG: ConfigToml = toml::from_str(&fs::read_to_string("bencher_config.toml").expect("Failed to read config.toml")).expect("Failed to deserialize bencher_config.toml");
    static ref COMPARATOR: Mutex<Comparator> = Mutex::new(Comparator {
        data: HashMap::new(),
        worker_count: CONFIG.endpoint.len(),
    });
}

#[derive(Default)]
pub struct EndpointStats {
    pub first_detections: usize,
    pub total_valid_transactions: usize,
    pub delays: Vec<f64>,
    pub old_transactions: usize,
}

fn get_current_timestamp() -> f64 {
    let start = SystemTime::now();
    let since_epoch = start.duration_since(UNIX_EPOCH).expect("Err");

    since_epoch.as_secs_f64()
}

fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

fn analyze_delays() {
    COMPARATOR.lock().unwrap().cleanup();
    let all_signatures: HashMap<String, HashMap<String, TransactionData>> =
        COMPARATOR.lock().unwrap().data.clone();
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();

    for endpoint_name in CONFIG.endpoint.iter().map(|e| e.name.clone()) {
        endpoint_stats.insert(endpoint_name.clone(), EndpointStats::default());
    }

    let mut fastest_endpoint = None;
    let mut highest_first_detection_rate = 0.0;

    for sig_data in all_signatures.values() {
        let mut is_historical = false;
        for tx_data in sig_data.values() {
            if tx_data.timestamp < tx_data.start_time {
                is_historical = true;
                break;
            }
        }

        if is_historical {
            for (endpoint, _) in sig_data {
                if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                    stats.old_transactions += 1;
                }
            }
            continue;
        }

        if let Some((first_endpoint, first_tx)) = sig_data
            .iter()
            .min_by(|a, b| a.1.timestamp.partial_cmp(&b.1.timestamp).unwrap())
        {
            if let Some(stats) = endpoint_stats.get_mut(first_endpoint) {
                stats.first_detections += 1;
                stats.total_valid_transactions += 1;
            }

            for (endpoint, tx) in sig_data {
                if endpoint != first_endpoint {
                    if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                        stats
                            .delays
                            .push((tx.timestamp - first_tx.timestamp) * 1000.0);
                        stats.total_valid_transactions += 1;
                    }
                }
            }
        }
    }

    for (endpoint, stats) in &endpoint_stats {
        if stats.total_valid_transactions > 0 {
            let detection_rate =
                stats.first_detections as f64 / stats.total_valid_transactions as f64;
            if detection_rate > highest_first_detection_rate {
                highest_first_detection_rate = detection_rate;
                fastest_endpoint = Some(endpoint.clone());
            }
        }
    }

    println!("\nFinished test results");
    println!("--------------------------------------------");

    if let Some(fastest) = fastest_endpoint.as_ref() {
        let fastest_stats = &endpoint_stats[fastest];
        let win_rate = (fastest_stats.first_detections as f64
            / fastest_stats.total_valid_transactions as f64)
            * 100.0;
        println!(
            "{}: Win rate {:.2}%, avg delay 0.00ms (fastest)",
            fastest, win_rate
        );

        for (endpoint, stats) in &endpoint_stats {
            if endpoint != fastest && stats.total_valid_transactions > 0 {
                let win_rate =
                    (stats.first_detections as f64 / stats.total_valid_transactions as f64) * 100.0;
                let avg_delay = if stats.delays.is_empty() {
                    0.0
                } else {
                    stats.delays.iter().sum::<f64>() / stats.delays.len() as f64
                };

                println!(
                    "{}: Win rate {:.2}%, avg delay {:.2}ms",
                    endpoint, win_rate, avg_delay
                );
            }
        }
    } else {
        println!("Not enough data");
    }

    println!("\nDetailed test results");
    println!("--------------------------------------------");

    if let Some(fastest) = fastest_endpoint {
        println!("\nFastest Endpoint: {}", fastest);
        let fastest_stats = &endpoint_stats[&fastest];
        println!(
            "  First detections: {} out of {} valid transactions ({:.2}%)",
            fastest_stats.first_detections,
            fastest_stats.total_valid_transactions,
            (fastest_stats.first_detections as f64 / fastest_stats.total_valid_transactions as f64)
                * 100.0
        );
        if fastest_stats.old_transactions > 0 {
            println!(
                "  Historical transactions detected: {}",
                fastest_stats.old_transactions
            );
        }

        println!("\nDelays relative to fastest endpoint:");
        for (endpoint, stats) in &endpoint_stats {
            if endpoint != &fastest && !stats.delays.is_empty() {
                let avg_delay = stats.delays.iter().sum::<f64>() / stats.delays.len() as f64;
                let max_delay = stats
                    .delays
                    .iter()
                    .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                let min_delay = stats.delays.iter().fold(f64::INFINITY, |a, &b| a.min(b));

                let mut sorted_delays = stats.delays.clone();
                sorted_delays.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p50 = percentile(&sorted_delays, 0.5);
                let p95 = percentile(&sorted_delays, 0.95);

                println!("\n{}:", endpoint);
                println!("  Average delay: {:.2} ms", avg_delay);
                println!("  Median delay: {:.2} ms", p50);
                println!("  95th percentile: {:.2} ms", p95);
                println!("  Min/Max delay: {:.2}/{:.2} ms", min_delay, max_delay);
                println!("  Valid transactions: {}", stats.total_valid_transactions);
                if stats.old_transactions > 0 {
                    println!(
                        "  Historical transactions detected: {}",
                        stats.old_transactions
                    );
                }
            }
        }
    } else {
        println!("Not enough data");
    }

    // THIS IS KINDA BROKEN
    // println!("\nComically Detailed Test Results");
    // println!("--------------------------------------------\n");

    // for sig_data in all_signatures.values() {
    //     let mut fstr = format!(
    //         "{}: ",
    //         sig_data
    //             .get(sig_data.keys().next().unwrap())
    //             .unwrap()
    //             .signature
    //     );

    //     let mut datagrams: Vec<(&String, &TransactionData)> = sig_data.iter().collect();
    //     datagrams.sort_by_key(|(_, tx)| tx.timestamp as u64);

    //     fstr.push_str(&format!("{}", datagrams.get(0).unwrap().0));
    //     let mut last = datagrams.get(0).unwrap().1.timestamp;

    //     for (_, (endpoint, tx)) in datagrams.iter().skip(1).enumerate() {
    //         fstr.push_str(&format!(
    //             " < {} (+{:.4}ms)",
    //             endpoint,
    //             (tx.timestamp - last) * 1000.0
    //         ));
    //         last = tx.timestamp;
    //     }

    //     print!("{}\n", fstr);
    // }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut transaction_count = 0;

    let log_filename = format!("transaction_log_{}.txt", endpoint.name);
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
        .expect("Failed to open log file");

    log::info!(
        "[{}] Connecting to endpoint: {}",
        endpoint.name,
        endpoint.url
    );

    let mut client = GeyserGrpcClient::build_from_shared(endpoint.url)?
        .x_token(Some(endpoint.x_token))?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    log::info!("[{}] Connected successfully", endpoint.name);

    let (mut subscribe_tx, mut stream) = client.subscribe().await?;
    let commitment: CommitmentLevel = config.commitment.into();

    let mut transactions = HashMap::new();
    transactions.insert(
        "account".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![config.account.clone()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    subscribe_tx
        .send(SubscribeRequest {
            slots: HashMap::default(),
            accounts: HashMap::default(),
            transactions,
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        })
        .await?;

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                if let Some(tx) = tx_msg.transaction {
                                    let accounts = tx.transaction.clone().unwrap().message.unwrap().account_keys
                                        .iter()
                                        .map(|key| bs58::encode(key).into_string())
                                        .collect::<Vec<String>>();

                                    if accounts.contains(&config.account) {
                                        let timestamp = get_current_timestamp();
                                        let signature = bs58::encode(&tx.transaction.unwrap().signatures[0]).into_string();

                                        let log_entry = format!(
                                            "[{:.3}] [{}] {}\n",
                                            timestamp,
                                            endpoint.name,
                                            signature
                                        );

                                        log_file.write_all(log_entry.as_bytes())
                                            .expect("Failed to write to log file");

                                        let mut comp = COMPARATOR.lock().unwrap();

                                        comp.add(
                                            endpoint.name.clone(),
                                            TransactionData {
                                                timestamp,

                                                signature: signature.clone(),
                                                start_time,
                                            },
                                        );
                                        if comp.get_valid_count() == config.transactions as usize {
                                            log::info!("Endpoint {} shutting down after {} transactions seen and {} by all workers", endpoint.name, transaction_count, config.transactions);
                                            shutdown_tx.send(()).unwrap();
                                            break 'ploop;
                                        }

                                        log::info!("{}", log_entry.trim());
                                        transaction_count += 1;
                                    }
                                }
                            },
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            },
                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        log::error!("[{}] Error receiving message: {:?}", endpoint.name, e);
                        break;
                    },
                    None => {
                        log::info!("[{}] Stream closed", endpoint.name);
                        break;
                    }
                }
            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}

async fn process_arpc_endpoint(
    endpoint: Endpoint,
    config: Config,
    shutdown_tx: broadcast::Sender<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut transaction_count = 0;

    let log_filename = format!("transaction_log_{}.txt", endpoint.name);
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
        .expect("Failed to open log file");

    log::info!(
        "[{}] Connecting to endpoint: {}",
        endpoint.name,
        endpoint.url
    );

    let mut client = ArpcServiceClient::connect(endpoint.url).await?;
    log::info!("[{}] Connected successfully", endpoint.name);

    fn reqstream(account: String) -> impl tokio_stream::Stream<Item = ArpcSubscribeRequest> {
        let mut transactions = HashMap::new();
        transactions.insert(
            String::from("account"),
            arpc::SubscribeRequestFilterTransactions {
                account_include: vec![account],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        tokio_stream::iter(vec![ArpcSubscribeRequest {
            transactions,
            ping_id: Some(0),
        }])
    }

    let in_stream = reqstream(config.account.clone());

    let mut stream = client.subscribe(in_stream).await?.into_inner();

    'ploop: loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                if let Some(Ok(msg)) = message {
                    if let Some(tx) = msg.transaction {
                        let accounts = tx.account_keys
                            .iter()
                            .map(|key| bs58::encode(key).into_string())
                            .collect::<Vec<String>>();

                        if accounts.contains(&config.account) {
                            let timestamp = get_current_timestamp();
                            let signature = bs58::encode(&tx.signatures[0]).into_string();

                            let log_entry = format!(
                                "[{:.3}] [{}] {}\n",
                                timestamp,
                                endpoint.name,
                                signature
                            );

                            log_file.write_all(log_entry.as_bytes())
                                .expect("Failed to write to log file");

                                let mut comp = COMPARATOR.lock().unwrap();

                                comp.add(
                                    endpoint.name.clone(),
                                    TransactionData {
                                        timestamp,

                                        signature: signature.clone(),
                                        start_time,
                                    },
                                );
                                if comp.get_valid_count() == config.transactions as usize {
                                    log::info!("Endpoint {} shutting down after {} transactions seen and {} by all workers", endpoint.name, transaction_count, config.transactions);
                                    shutdown_tx.send(()).unwrap();
                                    break 'ploop;
                                }


                            log::info!("{}", log_entry.trim());
                            transaction_count += 1;
                        }
                    }
                }

            }
        }
    }

    log::info!("[{}] Stream closed", endpoint.name);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let (shutdown_tx, _) = broadcast::channel(1);

    let mut handles = vec![];
    let start_time = get_current_timestamp();

    for endpoint in CONFIG.endpoint.clone() {
        let config = CONFIG.config.clone();
        let stx = shutdown_tx.clone();
        let shutdown_rx = shutdown_tx.subscribe();

        match endpoint.kind {
            EndpointKind::Yellowstone => handles.push(task::spawn(async move {
                if let Err(e) = process_yellowstone_endpoint(
                    endpoint.clone(),
                    config,
                    stx,
                    shutdown_rx,
                    start_time,
                )
                .await
                {
                    log::error!("[{}] Error processing endpoint: {:?}", endpoint.name, e);
                }
            })),
            EndpointKind::Arpc => handles.push(task::spawn(async move {
                if let Err(e) =
                    process_arpc_endpoint(endpoint.clone(), config, stx, shutdown_rx, start_time)
                        .await
                {
                    log::error!("[{}] Error processing endpoint: {:?}", endpoint.name, e);
                }
            })),
        }
    }

    tokio::spawn(async move {
        if let Ok(_) = ctrl_c().await {
            println!("\nReceived Ctrl+C signal. Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    for handle in handles {
        handle.await?;
    }

    analyze_delays();

    Ok(())
}
