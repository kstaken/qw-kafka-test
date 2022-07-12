use std::fs;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use chrono;

use tokio::signal;
use tokio::sync::{mpsc, broadcast};

use clap::{App, Arg};
use log::{info, warn, error};
use env_logger;

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{Message};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::get_rdkafka_version;

struct CustomContext {
    offset_storage: String,
    //simulated_split_ref: Arc<Mutex<Vec<String>>>
    simulated_split_ref: Arc<SplitStore>
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let &Rebalance::Assign(tpl) = rebalance {
            // Invalidate in progress split. Since partitions may be moving away
            // any in progress state will be invalid and a new split would need
            // to be started from the most recently committed offsets.
            self.simulated_split_ref.empty();

            let partitions = tpl.elements();
            for entry in partitions.iter() {
                // Need to use find_partition to get a mutable reference.
                let mut partition = tpl.find_partition(entry.topic(), entry.partition())
                    .expect("Unknown partition");

                let filename = format!("{}/{}/{}", self.offset_storage, entry.topic(), entry.partition());

                let offset = match fs::read_to_string(filename) {
                    Ok(offset_string) => {
                        let numeric_offset = offset_string.parse::<i64>()
                           .expect("Invalid stored offset");

                        if numeric_offset < 0 {
                            Offset::Beginning
                        }
                        else {
                            Offset::Offset(numeric_offset)
                        }
                    }
                    Err(error) => match error.kind() {
                        ErrorKind::NotFound => Offset::Beginning,
                        other_error => {
                            panic!("Offset storage read error: {:?}", other_error)
                        }
                    }
                };

                info!("Setting offsets for {}, {}, {:?}", entry.topic(), entry.partition(), offset);

                partition.set_offset(offset)
                    .expect("Failure setting offset");
            }
        }
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

fn commit(consumer: &LoggingConsumer, group_id: &str) -> std::io::Result<()> {
    info!("Committing local offsets");

    // Storing commits in the file system or an Object storage system should be able to
    // achieve at-least-once semantics. Using a Postgres transaction in combination with
    // with the Split update to Published will achieve exactly-once semantics.
    let topics = consumer.position().expect("Error retrieving offsets prior to commit");
    for ((topic, partition), offset) in topics.to_topic_map() {
        info!("Commiting Offsets: {:?}: {:?} {:?}", topic, partition, offset);
        let path = format!("./offsets/{}/{}/{}", group_id, topic, partition);
        if let rdkafka::Offset::Offset(value) = offset {
            fs::write(path, value.to_string()).expect("Unable to write offset checkpoint to file.");
        }
    }

    Ok(())
}

struct SplitStore {
    store: Mutex<Vec<String>>,
    directory: String
}

impl SplitStore {
    fn new(directory: &str) -> SplitStore {
        fs::create_dir_all(directory).expect("Failure creating data output directory");

        SplitStore {
            store: Mutex::new(Vec::new()),
            directory: String::from(directory)
        }
    }

    fn store(&self, payload: String) {
        self.store.lock().unwrap().push(payload);
    }

    fn flush(&self) {
        let output_file = format!("{}/{}.ldjson", self.directory, chrono::offset::Utc::now().to_rfc3339());

        fs::write(output_file, self.store.lock().unwrap().iter().map( |record| record.to_string() + "\n").collect::<String>())
            .expect("Unable to save data to file.");
    }

    fn empty(&self) {
        self.store.lock().unwrap().clear();
    }

    fn full(&self) -> bool {
        self.store.lock().unwrap().len() == 100_000
    }
}

async fn process(
    output_directory: &str, brokers: &str, group_id: &str, topics: &[&str],
    mut notices: tokio::sync::broadcast::Receiver<&str>,
    shutdown: tokio::sync::mpsc::Sender<&str>
) {
    let simulated_split = Arc::new(SplitStore::new(output_directory));

    let context = CustomContext {
        offset_storage: format!("./offsets/{}", group_id),
        simulated_split_ref: simulated_split.clone()
    };

    // Insure directories exist
    // Need to handle all topics
    let path = format!("./offsets/{}/{}/", group_id, topics[0]);
    fs::create_dir_all(path).expect("Failure creating offsets storage directory");

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    loop {
        tokio::select! {
            _ = notices.recv() => {
                info!("Shutting Down and Flushing on message");
                simulated_split.flush();
                commit(&consumer, group_id)
                    .expect("Error committing offsets");

                break;
            },
            record = consumer.recv() => {
                match record {
                    Err(e) => warn!("Kafka error: {}", e),
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            None => "",
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                warn!("Error while deserializing message payload: {:?}", e);
                                ""
                            }
                        };

                        simulated_split.store(payload.to_string());

                        if simulated_split.full() {
                            simulated_split.flush();

                            commit(&consumer, group_id)
                                .expect("Error committing offsets");

                            // Simulate starting a new split
                            simulated_split.empty();
                        }
                    }
                }
            }
        }
    }
}

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
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let (notices_tx, notices_rx) = broadcast::channel(1);
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(10);

    let shutdown = shutdown_tx.clone();
    tokio::spawn(async move {
        let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
        let brokers = matches.value_of("brokers").unwrap();
        let group_id = matches.value_of("group-id").unwrap();
        let output_directory = matches.value_of("output").unwrap();

        process(output_directory, brokers, group_id, &topics, notices_rx, shutdown).await;
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