use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, fs, path::Path};
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::task;
use tokio::signal::ctrl_c;
use toml;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    geyser::{CommitmentLevel, SubscribeRequest},
    tonic::transport::ClientTlsConfig,
};
use bytes::Bytes;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeRequestPing;
use yellowstone_grpc_proto::prelude::{SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions};
use bs58;
use tokio::sync::broadcast;

#[derive(Debug, Deserialize, Serialize)]
struct ConfigToml {
    config: Config,
    endpoint: Vec<Endpoint>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Config {
    transactions: i32,
    account: String,
    commitment: ArgsCommitment,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Endpoint {
    name: String,
    url: String,
    x_token: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone)]
struct TransactionData {
    timestamp: f64,
    slot: u64,
    signature: String,
    start_time: f64,
}

struct EndpointStats {
    first_detections: usize,
    total_valid_transactions: usize,
    delays: Vec<f64>,
    old_transactions: usize,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

fn get_current_timestamp() -> f64 {
    let start = SystemTime::now();
    let since_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Err");

    since_epoch.as_secs_f64()
}

fn create_default_config() -> Result<ConfigToml, Box<dyn std::error::Error>> {
    let default_config = ConfigToml {
        config: Config {
            transactions: 25,
            account: "7dGrdJRYtsNR8UYxZ3TnifXGjGc9eRYLq9sELwYpuuUu".to_string(),
            commitment: ArgsCommitment::Processed,
        },
        endpoint: vec![
            Endpoint {
                name: "endpoint 1".to_string(),
                url: "https://api.mainnet-beta.solana.com:10000".to_string(),
                x_token: "YOUR_TOKEN_HERE".to_string(),
            },
            Endpoint {
                name: "endpoint 2".to_string(),
                url: "http://frankfurt.omeganetworks.io:10000".to_string(),
                x_token: "".to_string(),
            },
            Endpoint {
                name: "endpoint 3".to_string(),
                url: "http://newyork.omeganetworks.io:10000".to_string(),
                x_token: "YOUR_TOKEN_HERE".to_string(),
            },
        ],
    };

    let toml_string = toml::to_string_pretty(&default_config)?;
    fs::write("config.toml", toml_string)?;

    log::info!("Created default config.toml file. Please edit it with your endpoints before running again.");

    Ok(default_config)
}


fn percentile(sorted_data: &[f64], p: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }
    let index = (p * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index]
}

fn analyze_delays(data: &HashMap<String, Vec<TransactionData>>) {
    let mut all_signatures: HashMap<String, HashMap<String, TransactionData>> = HashMap::new();
    let mut endpoint_stats: HashMap<String, EndpointStats> = HashMap::new();

    for endpoint_name in data.keys() {
        endpoint_stats.insert(endpoint_name.clone(), EndpointStats {
            first_detections: 0,
            total_valid_transactions: 0,
            delays: Vec::new(),
            old_transactions: 0,
        });
    }

    for (endpoint_name, transactions) in data {
        for tx in transactions {
            all_signatures
                .entry(tx.signature.clone())
                .or_insert_with(HashMap::new)
                .insert(endpoint_name.clone(), tx.clone());
        }
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
            .min_by(|a, b| a.1.timestamp.partial_cmp(&b.1.timestamp).unwrap()) {

            if let Some(stats) = endpoint_stats.get_mut(first_endpoint) {
                stats.first_detections += 1;
                stats.total_valid_transactions += 1;
            }

            for (endpoint, tx) in sig_data {
                if endpoint != first_endpoint {
                    if let Some(stats) = endpoint_stats.get_mut(endpoint) {
                        stats.delays.push((tx.timestamp - first_tx.timestamp) * 1000.0);
                        stats.total_valid_transactions += 1;
                    }
                }
            }
        }
    }

    for (endpoint, stats) in &endpoint_stats {
        if stats.total_valid_transactions > 0 {
            let detection_rate = stats.first_detections as f64 / stats.total_valid_transactions as f64;
            if detection_rate > highest_first_detection_rate {
                highest_first_detection_rate = detection_rate;
                fastest_endpoint = Some(endpoint.clone());
            }
        }
    }

    println!("\nFinished test results");

    if let Some(fastest) = fastest_endpoint.as_ref() {
        let fastest_stats = &endpoint_stats[fastest];
        let win_rate = (fastest_stats.first_detections as f64 / fastest_stats.total_valid_transactions as f64) * 100.0;
        println!("{}: Win rate {:.2}%, avg delay 0.00ms (fastest)",
                 fastest,
                 win_rate);

        for (endpoint, stats) in &endpoint_stats {
            if endpoint != fastest && stats.total_valid_transactions > 0 {
                let win_rate = (stats.first_detections as f64 / stats.total_valid_transactions as f64) * 100.0;
                let avg_delay = if stats.delays.is_empty() {
                    0.0
                } else {
                    stats.delays.iter().sum::<f64>() / stats.delays.len() as f64
                };

                println!("{}: Win rate {:.2}%, avg delay {:.2}ms",
                         endpoint,
                         win_rate,
                         avg_delay);
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
        println!("  First detections: {} out of {} valid transactions ({:.2}%)",
                 fastest_stats.first_detections,
                 fastest_stats.total_valid_transactions,
                 (fastest_stats.first_detections as f64 / fastest_stats.total_valid_transactions as f64) * 100.0
        );
        if fastest_stats.old_transactions > 0 {
            println!("  Historical transactions detected: {}", fastest_stats.old_transactions);
        }

        println!("\nDelays relative to fastest endpoint:");
        for (endpoint, stats) in &endpoint_stats {
            if endpoint != &fastest && !stats.delays.is_empty() {
                let avg_delay = stats.delays.iter().sum::<f64>() / stats.delays.len() as f64;
                let max_delay = stats.delays.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
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
                    println!("  Historical transactions detected: {}", stats.old_transactions);
                }
            }
        }
    } else {
        println!("Not enough data");
    }
}
async fn process_endpoint(
    endpoint: Endpoint,
    config: Config,
    shared_data: Arc<Mutex<HashMap<String, Vec<TransactionData>>>>,
    mut shutdown_rx: broadcast::Receiver<()>,
    start_time: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let max_transactions = config.transactions;
    let mut transaction_count = 0;

    let log_filename = format!("transaction_log_{}.txt", endpoint.name);
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_filename)
        .expect("Failed to open log file");

    log::info!("[{}] Connecting to endpoint: {}", endpoint.name, endpoint.url);

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

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                log::info!("[{}] Received stop signal...", endpoint.name);
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        if transaction_count >= max_transactions {
                            log::info!("[{}] Reached maximum number of transactions ({}), stopping...",
                                endpoint.name, max_transactions);
                            break;
                        }

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
                                            "[{:.3}] [{}] Transaction: slot={}, signature={}\n",
                                            timestamp,
                                            endpoint.name,
                                            tx_msg.slot,
                                            signature
                                        );

                                        log_file.write_all(log_entry.as_bytes())
                                            .expect("Failed to write to log file");

                                        let mut data = shared_data.lock().unwrap();
                                        data.entry(endpoint.name.clone())
                                            .or_insert_with(Vec::new)
                                            .push(TransactionData {
                                                timestamp,
                                                slot: tx_msg.slot,
                                                signature: signature.clone(),
                                                start_time,
                                            });

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


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let settings: ConfigToml = if Path::new("config.toml").exists() {
        let toml_str = fs::read_to_string("config.toml").expect("Failed to read config.toml file");
        toml::from_str(&toml_str).expect("Failed to deserialize config.toml")
    } else {
        log::info!("No config.toml file found. Creating default configuration...");
        create_default_config()?;
        println!("Default config.toml created. Please edit it with your endpoint details and run the program again.");
        return Ok(());
    };

    let shared_data: Arc<Mutex<HashMap<String, Vec<TransactionData>>>> = Arc::new(Mutex::new(HashMap::new()));

    let (shutdown_tx, _) = broadcast::channel(1);

    let mut handles = vec![];
    let start_time = get_current_timestamp();

    for endpoint in settings.endpoint {
        let config = settings.config.clone();
        let shared_data_clone = Arc::clone(&shared_data);
        let shutdown_rx = shutdown_tx.subscribe();

        handles.push(task::spawn(async move {
            if let Err(e) = process_endpoint(
                endpoint.clone(),
                config,
                shared_data_clone,
                shutdown_rx,
                start_time
            ).await {
                log::error!("[{}] Error processing endpoint: {:?}", endpoint.name, e);
            }
        }));
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

    let final_data = shared_data.lock().unwrap().clone();
    analyze_delays(&final_data);

    Ok(())
}
