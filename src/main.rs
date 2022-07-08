use std::fs;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

use clap::{App, Arg};
use log::{info, warn};
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
    simulated_split_ref: Arc<Mutex<Vec<String>>>
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let &Rebalance::Assign(tpl) = rebalance {
            // Invalidate in progress split. Since partitions may be moving away
            // any in progress state will be invalid and a new split would need
            // to be started from the most recently committed offsets.
            self.simulated_split_ref.lock().unwrap().clear();

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

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let simulated_split = Arc::new(Mutex::new(Vec::new()));

    let context = CustomContext {
        offset_storage: format!("./offsets/{}", group_id),
        simulated_split_ref: simulated_split.clone()
    };

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
        match consumer.recv().await {
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

                simulated_split.lock().unwrap().push(payload.to_string());

                if simulated_split.lock().unwrap().len() == 100_000 {
                    commit(&consumer, group_id)
                        .expect("Error committing offsets");

                    // Simulate starting a new split
                    simulated_split.lock().unwrap().clear();
                }
            }
        };
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = App::new("consumer example")
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
        .get_matches();

    info!("Starting");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, &topics).await
}