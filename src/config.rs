use std::fmt;

#[derive(Debug)]
pub struct Server {
    pub id: u64,
    pub host: String,
    pub port: String,
    pub endpoint: String,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) struct Config {
    pub servers: Option<Vec<Server>>,
    pub majority: u64,
    pub server: Server,
    pub log_config: LogConfig,
    pub nodes: u64,
}

impl Config {
    fn init() -> Config {
        let port: String = "6969".into();
        let host: String = "127.0.0.1".into();
        println!("Init Config");
        let server = Server {
            id: 1,
            host: host.clone(),
            port: port.clone(),
            endpoint: format!("http://{}:{}", host, port),
        };
        let log_config = LogConfig {
            log_file: "log.rf".into(),
            path: ".".into(),
            buffer_log: false,
            log_persist_threshold: 0,
        };
        let config = Config {
            servers: None,
            majority: 1,
            server,
            log_config,
            nodes: 1,
        };
        config
    }
    pub fn load_config() -> Config {
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
