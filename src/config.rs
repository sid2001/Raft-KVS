#[derive(Debug)]
struct Server {
    id: u64,
    host: String,
    port: String,
}

#[derive(Debug)]
pub(crate) struct LogConfig {
    log_file: String,
    path: String,
    buffer_log: bool,
    log_persist_threshold: u64,
}

impl LogConfig {
    pub fn get_log_path(&self) -> String {
        format!("{}/{}", self.path.clone(), self.log_file.clone())
    }
}

#[derive(Debug)]
pub(crate) struct Config {
    peer: Option<Vec<Server>>,
    server: Server,
    log_config: LogConfig,
    nodes: u64,
}

impl Config {
    fn init() -> Config {
        println!("Init Config");
        let server = Server {
            id: 1,
            host: "127.0.0.1".into(),
            port: "6969".into(),
        };
        let log_config = LogConfig {
            log_file: "log.rf".into(),
            path: ".".into(),
            buffer_log: false,
            log_persist_threshold: 0,
        };
        let config = Config {
            peer: None,
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
