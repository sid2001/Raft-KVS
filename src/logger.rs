use crate::config::LogConfig;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::{BufReader, BufWriter};

#[derive(Debug)]
struct MiniRedis;
impl Store for MiniRedis {}
#[derive(Debug, Serialize, Deserialize)]
pub struct MiniRedisRequest {
    pub data: u32, // dummy data
}
impl Data for MiniRedisRequest {}
use std::fmt::Debug;

type Term = u64;
pub(crate) trait Data {}
trait Store: Sync + Send + Debug {}

#[derive(Debug, Serialize, Deserialize)]
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
    pub(crate) fn persist_log(&mut self) -> Result<(), Box<dyn Error>> {
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

        let data = &self.log.0.as_slice()[self.prev_persist_index as usize + 1..];
        for d in data {
            let _ = bincode::serialize_into(&mut buf_writer, d)?;
        }
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
    pub(crate) fn insert(&mut self, entry: LogEntry<MiniRedisRequest>) {
        let len = self.log.0.len() as u64;
        if len < self.next_index + 1 {
            self.prev_log_term = entry.term;
            self.log.0.push(entry);
            self.next_index = len;
            self.prev_log_index = len - 1;
        } else {
            self.prev_log_index = entry.term;
            self.log.0[self.next_index as usize] = entry;
            self.prev_log_index = self.next_index;
            self.next_index += 1;
        }
    }
}
