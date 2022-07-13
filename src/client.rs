use std::fs;
use std::io::ErrorKind;
use std::sync::Arc;

use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::message::{Message};
use rdkafka::topic_partition_list::Offset;

use crate::split_store;

struct CustomContext {
    offset_storage: String,
    //simulated_split_ref: Arc<Mutex<Vec<String>>>
    simulated_split_ref: Arc<split_store::SplitStore>
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

pub struct Client {
    pub output_directory: String,
    pub offsets_directory: String,
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub notices: tokio::sync::broadcast::Receiver<String>,
    pub shutdown: tokio::sync::mpsc::Sender<String>
}

impl Client {
    fn commit(&self, consumer: &LoggingConsumer) -> std::io::Result<()> {
        info!("Committing local offsets");

        // Storing commits in the file system or an Object storage system should be able to
        // achieve at-least-once semantics. Using a Postgres transaction in combination with
        // with the Split update to Published will achieve exactly-once semantics.
        let topics = consumer.position().expect("Error retrieving offsets prior to commit");
        for ((topic, partition), offset) in topics.to_topic_map() {
            info!("Commiting Offsets: {:?}: {:?} {:?}", topic, partition, offset);
            let path = format!("./offsets/{}/{}/{}", self.group_id, topic, partition);
            if let rdkafka::Offset::Offset(value) = offset {
                fs::write(path, value.to_string()).expect("Unable to write offset checkpoint to file.");
            }
        }

        Ok(())
    }

    pub async fn process(&mut self) {
        let simulated_split = Arc::new(split_store::SplitStore::new(self.output_directory.as_ref()));

        let context = CustomContext {
            offset_storage: format!("./offsets/{}", self.group_id),
            simulated_split_ref: simulated_split.clone()
        };

        // Insure directories exist
        // Need to handle all topics
        let path = format!("./offsets/{}/{}/", self.group_id, self.topics[0]);
        fs::create_dir_all(path).expect("Failure creating offsets storage directory");

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", self.group_id.clone())
            .set("bootstrap.servers", self.brokers.clone())
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");

        consumer
            .subscribe(&self.topics.iter().map(AsRef::as_ref).collect::<Vec<&str>>())
            .expect("Can't subscribe to specified topics");

        loop {
            tokio::select! {
                _ = self.notices.recv() => {
                    info!("Shutting Down and Flushing on message");
                    simulated_split.flush();
                    self.commit(&consumer)
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

                                self.commit(&consumer)
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
}


// These tests are dependent on a running Kafka instance. They're not intended to be unit tests.
#[cfg(test)]
mod tests {
    use tokio::signal;
    use tokio::sync::{mpsc, broadcast};
    use env_logger;

    use super::*;

    #[tokio::test]
    async fn my_test() {
        assert!(true);
        env_logger::init();
        let (notices_tx, notices_rx) = broadcast::channel(1);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(10);

        let task = tokio::spawn(async move {
            let mut client = Client {
                output_directory: String::from("./tests/output"),
                offsets_directory: String::from("./tests/offsets"),
                brokers: String::from("localhost:9092"),
                group_id: String::from("q"),
                topics: vec![String::from("quickwit-test-v1")],
                notices: notices_rx,
                shutdown: shutdown_tx
            };

            client.process().await;
        });

        task.await;
    }
}