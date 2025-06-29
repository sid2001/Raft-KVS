use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read};
use std::{env, fmt};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Server {
    pub id: u64,
    pub host: String,
    pub port: String,
    pub endpoint: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct LogConfig {
    pub log_file: String,
    pub path: String,
    pub buffer_log: bool,
    pub log_persist_threshold: u64,
}

impl LogConfig {
    pub fn get_log_path(&self) -> String {
        format!("{}/{}", self.path.clone(), self.log_file.clone())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Config {
    pub servers: Vec<Server>,
    pub majority: u64,
    pub server: Server,
    pub log_config: LogConfig,
    pub nodes: u64,
}

impl Config {
    fn init() -> Result<Config, std::io::Error> {
        let file = File::open("./.config.rf")?;
        let args: Vec<String> = env::args().collect();
        if args.len() < 2 {
            eprintln!("Provide server id.");
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "server id not provided",
            ));
        }
        let reader = BufReader::new(file);

        let mut config: Config = serde_json::from_reader(reader)?;
        let id = match args[1].parse::<usize>() {
            Ok(i) => i,
            Err(_) => return Err(std::io::Error::new(std::io::ErrorKind::Other, "invalid id")),
        };

        println!("Init Config");
        config.server = config.servers[id - 1].clone();
        println!("config loaded {:?}", config);
        Ok(config)
    }
    pub fn load_config() -> Result<Config, std::io::Error> {
        println!("Loading config");
        Config::init()
    }
    pub fn get_log_config(&self) -> &LogConfig {
        &self.log_config
    }
    pub fn get_address(&self) -> String {
        println!("Fetching address");
        format!("{}:{}", self.server.host.clone(), self.server.port.clone())
    }
}
