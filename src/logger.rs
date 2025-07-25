use crate::config::LogConfig;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::{BufReader, BufWriter};

#[derive(Debug)]
struct MiniRedis;
impl Store for MiniRedis {}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MiniRedisRequest {
    pub data: u64, // dummy data
}
impl Data for MiniRedisRequest {}
use std::fmt::Debug;

type Term = u64;
pub(crate) trait Data {}
trait Store: Sync + Send + Debug {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct LogEntry<T: Data> {
    pub term: Term,
    pub data: T,
}

#[derive(Debug, Default)]
pub(crate) struct Log(pub Vec<LogEntry<MiniRedisRequest>>);

#[derive(Debug)]
pub(crate) struct Logger {
    log: Log, // Updated to stable storage before responding to RPCs
    store: Option<Box<dyn Store>>,

    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: u64,

    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    commit_index: u64,

    // these log indexes are just to track the index of latest entry and next index entry
    prev_log_index: u64,
    prev_log_term: u64,
    next_index: u64,

    buffer_log: bool,
    prev_persist_index: u64,
    log_file: Option<File>,
    log_file_path: Option<String>,
}

pub(crate) struct LogAppendEntry {
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub commit_index: u64,
    pub entries: Vec<LogEntry<MiniRedisRequest>>,
}

impl Default for Logger {
    fn default() -> Self {
        Self {
            log: Log(vec![]),
            store: None,
            last_applied: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            commit_index: 0,
            next_index: 0,
            buffer_log: false,
            prev_persist_index: 0,
            log_file: None,
            log_file_path: None,
        }
    }
}

impl Logger {
    fn new() -> Self {
        todo!();
    }
    pub(crate) fn init(&mut self, config: &LogConfig) -> Result<(), Box<dyn Error>> {
        println!("Log init");
        let path = config.get_log_path();
        let len = Logger::init_log(&mut self.log, &path)?;
        self.prev_persist_index = len;
        self.log_file_path = Some(path);
        Ok(())
    }
    fn init_log(log: &mut Log, path: &str) -> Result<u64, Box<dyn Error>> {
        println!("Loading logs from {}", path);
        let f = File::open::<String>(path.into())?;
        let mut buf_reader = BufReader::new(f);
        let mut out = vec![];
        loop {
            match bincode::deserialize_from(&mut buf_reader) {
                Ok(entry) => {
                    out.push(entry);
                }
                Err(e) => {
                    if let bincode::ErrorKind::Io(ref io_err) = *e {
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(e.into());
                }
            }
        }
        let len = out.len() as u64;
        *log = Log(out);
        Ok(len)
    }
    pub(crate) fn persist_log(&mut self, idx: u64) -> Result<(), Box<dyn Error>> {
        // takes the index and saves log upto the index
        println!("Persist log");
        let f = if let Some(ref file) = &self.log_file {
            Some(file)
        } else if let Some(path) = &self.log_file_path {
            self.log_file = Some(OpenOptions::new().write(true).append(true).open(path)?);
            self.log_file.as_ref()
        } else {
            None
        };
        if f.is_none() {
            return Err(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "File path not specified or is incorrect!",
            )));
        }
        let mut buf_writer = BufWriter::new(f.unwrap());

        let data = &self.log.0.as_slice()[self.prev_persist_index as usize + 1..idx as usize + 1];
        for d in data {
            let _ = bincode::serialize_into(&mut buf_writer, d)?;
            self.prev_persist_index += 1;
        }
        // check if all the log entry is stored
        assert!(
            self.prev_persist_index == idx,
            "failed to persist log upto {} expected {}",
            self.prev_persist_index,
            idx
        );
        Ok(())
    }
    pub(crate) fn get_empty_append_entry(&self) -> LogAppendEntry {
        LogAppendEntry {
            prev_log_index: self.prev_log_index,
            prev_log_term: self.prev_log_term,
            commit_index: self.commit_index,
            entries: vec![],
        }
    }

    pub(crate) fn get_prev_log_index(&self) -> u64 {
        self.prev_log_index
    }
    pub(crate) fn get_prev_log_term(&self) -> u64 {
        self.prev_log_term
    }

    pub(crate) fn log_index_term_checker(&self, index: u64, term: u64) -> bool {
        if (self.prev_log_term < term)
            || (self.prev_log_term == term && self.prev_log_index <= index)
        {
            true
        } else {
            false
        }
    }
    pub(crate) fn log_index_term_checker_equal(&self, index: u64, term: u64) -> bool {
        if self.prev_log_term == term && self.prev_log_index == index {
            true
        } else {
            false
        }
    }

    // checks if entry exists at the given index and return its term or false
    pub(crate) fn get_entry_term_at(&self, idx: u64) -> Option<u64> {
        if self.log.0.len() as u64 > idx {
            Some(self.log.0[idx as usize].term)
        } else {
            None
        }
    }

    pub(crate) fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    pub(crate) fn commit(&mut self, idx: u64) -> Result<(), Box<dyn Error>> {
        // if idx > commit_index, commit_index = min(idx, index_of_last_entry)
        if self.buffer_log == false {
            self.persist_log(min(idx, self.prev_log_index))?;
            self.commit_index = self.prev_persist_index;
        } else {
            // this section handles late persistent storage for performance
            todo!();
        }
        Ok(())
    }

    pub(crate) fn get_log_at_index(&self, idx: u64) -> Result<LogEntry<MiniRedisRequest>, ()> {
        if idx < self.log.0.len() as u64 {
            Ok(self.log.0[idx as usize].clone())
        } else {
            Err(())
        }
    }

    pub(crate) fn is_log_empty(&self) -> bool {
        self.log.0.len() == 0
    }

    pub(crate) fn insert_from(&mut self, idx: u64, entries: Vec<LogEntry<MiniRedisRequest>>) {
        self.log.0.truncate(idx as usize); // remove all the following entries which are invalid
        self.prev_log_index = idx - 1;
        self.next_index = idx;

        for entry in entries {
            self.prev_log_term = entry.term;
            self.log.0.push(entry);
            self.prev_log_index += 1;
            self.next_index += 1;
        }
    }
}
