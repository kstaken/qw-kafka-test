use std::fs;
use std::sync::Mutex;
use chrono;

pub struct SplitStore {
    store: Mutex<Vec<String>>,
    directory: String
}

impl SplitStore {
    pub fn new(directory: &str) -> SplitStore {
        fs::create_dir_all(directory).expect("Failure creating data output directory");

        SplitStore {
            store: Mutex::new(Vec::new()),
            directory: String::from(directory)
        }
    }

    pub fn store(&self, payload: String) {
        self.store.lock().unwrap().push(payload);
    }

    pub fn flush(&self) {
        let output_file = format!("{}/{}.ldjson", self.directory, chrono::offset::Utc::now().to_rfc3339());

        fs::write(output_file, self.store.lock().unwrap().iter().map( |record| record.to_string() + "\n").collect::<String>())
            .expect("Unable to save data to file.");
    }

    pub fn empty(&self) {
        self.store.lock().unwrap().clear();
    }

    pub fn full(&self) -> bool {
        self.store.lock().unwrap().len() == 100_000
    }
}
