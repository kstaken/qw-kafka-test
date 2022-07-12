use tokio::signal;
use tokio::sync::{mpsc, broadcast};

use clap::{App, Arg};
use log::info;
use env_logger;

mod consumer;
mod split_store;

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = App::new("Local Offset Storage Test")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Experimental locally managed offsets")
        .arg(
            Arg::with_name("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short('g')
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("topics")
                .short('t')
                .long("topics")
                .help("Topic list")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output")
                .short('o')
                .long("output")
                .help("Output directory")
                .takes_value(true)
                .multiple(false)
                .required(true),
        )
        .get_matches();

    info!("Starting");

    let (notices_tx, notices_rx) = broadcast::channel(1);
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(10);

    let shutdown = shutdown_tx.clone();
    tokio::spawn(async move {
        let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
        let brokers = matches.value_of("brokers").unwrap();
        let group_id = matches.value_of("group-id").unwrap();
        let output_directory = matches.value_of("output").unwrap();

        consumer::process(output_directory, brokers, group_id, &topics, notices_rx, shutdown).await;
    });

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Shutting Down");
            notices_tx.send("Shutdown").unwrap();
            //drop(notices_tx);
            info!("Shutdown message sent");
            drop(shutdown_tx);
            let _ = shutdown_rx.recv().await;
            info!("Finalizing Shutdown");
        }
    }
}