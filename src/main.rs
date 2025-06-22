#![allow(unused_imports)]
#![allow(dead_code)]

use crate::logger::*;
use config::Config;
use core::fmt;
use proto::raft_rpc_client::RaftRpcClient;
use proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
use proto::{AppendEntry, AppendEntryResponse, RequestVote, RequestVoteResponse};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::process::Output;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{self, Duration, Instant};
use tonic::server;
use tonic::{
    transport::{self, Endpoint},
    Request, Response, Status,
};

mod config;
mod logger;
mod proto {
    tonic::include_proto!("raft");
}

trait RPCResponse {}

const DEFAULT_CHANNEL_CAPACITY: u32 = 32;
const DEFAULT_HEART_BEAT_PERIOD: u64 = 50;
const DEFAULT_TIME_OUT_PERIOD: u64 = 1000;

#[derive(Debug, PartialEq, Eq)]
enum State {
    Leader,
    Candidate,
    Follower,
}

#[derive(Debug)]
enum Msg {
    AppendEntry(AppendEntry),
    RequestVote(RequestVote),
}

#[derive(Debug)]
enum ResponseMsg {
    RequestVoteResponse(RequestVoteResponse),
    AppendEntryResponse(AppendEntryResponse),
}
impl RPCResponse for ResponseMsg {}

struct RequestMsg<T: RPCResponse> {
    msg: Msg,
    sender: oneshot::Sender<T>,
}

//#[derive(Debug)]
//struct Peer(Endpoint);

#[derive(Debug)]
struct Server {
    // for each server, index of the next log entry to send to that server (initialised to leader
    // last log index + 1)
    next_index: u64,

    // for each server index, of highest log entry known to be replicated on server (initialized to
    // 0, increases monotonically)
    match_index: u64,

    endpoint: Endpoint,
}

impl Server {
    fn init(&mut self, prev_log_index: u64) {
        self.next_index = prev_log_index + 1;
        self.match_index = 0;
    }
}

#[derive(Debug)]
struct Raft {
    id: u64,

    // Updated on stable storage before responding to RPCs
    current_term: u64, // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    voted_for: Option<u64>,
    logger: Logger,

    state: Mutex<State>,
    votes: u64, // do we need mutex on this??
    servers: HashMap<u64, crate::Server>,
    config: Option<Config>,
}

impl Default for Raft {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            id: 0,
            votes: 0,
            servers: HashMap::new(),
            logger: Logger::default(),
            state: Mutex::new(State::Follower),
            config: None,
        }
    }
}

#[derive(Debug)]
struct MyError(String);
impl Error for MyError {}
impl Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: {}", self.0)
    }
}

impl Raft {
    fn init(config: &Config) -> Result<Raft, Box<dyn Error>> {
        let mut raft = Raft::default();
        raft.logger.init(&config.get_log_config())?;

        if let Some(ref servers) = config.servers {
            for server in servers {
                let ep = server.endpoint.clone();
                let serv = crate::Server {
                    next_index: raft.logger.get_prev_log_index() + 1,
                    match_index: 0,
                    // fix this
                    // todo
                    endpoint: Endpoint::from_shared(ep).ok().unwrap(),
                };
                raft.servers.insert(server.id, serv);
            }
        }
        // todo()
        // replicate all necessary configs into raft instance
        //raft.config = Some(config);
        Ok(raft)
    }
    fn handle_time_out(&mut self) -> Result<(), Box<dyn Error>> {
        //let client = RaftRpcClient;
        // todo
        match self.state.lock() {
            Ok(mut s) => {
                *s = State::Candidate;
            }
            Err(_) => {
                return Err(Box::new(MyError("Failed accquiring state lock!".into())));
            }
        }
        self.current_term += 1;

        // reinitialize server state after election
        for (_, server) in &mut self.servers {
            server.init(self.logger.get_prev_log_index());
        }
        let _lock = self.state.lock().unwrap();
        Ok(())
    }

    fn get_empty_append_entry(&self) -> AppendEntry {
        let ae = self.logger.get_empty_append_entry();
        AppendEntry {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: ae.prev_log_index,
            prev_log_term: ae.prev_log_term,
            leader_commit: ae.commit_index,
            entries: vec![],
        }
    }

    async fn handle_heartbeat(&self) -> Result<(), MyError> {
        for (_, peer) in &self.servers {
            match RaftRpcClient::connect(peer.endpoint.clone()).await {
                Ok(mut client) => {
                    let _res = client
                        .append_entries_rpc(self.get_empty_append_entry())
                        .await;
                }
                Err(e) => {
                    println!("Cannot coonect with peer {:?}\nError: {}", peer, e);
                }
            }
        }
        Ok(())
    }

    async fn heart(
        &mut self,
        ch: &mut Receiver<RequestMsg<ResponseMsg>>,
    ) -> Result<(), Box<dyn Error>> {
        //tokio::spawn(Raft::heartbeat(&mut self));
        let timeout =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
        let mut heartbeat =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_HEART_BEAT_PERIOD));

        tokio::pin!(timeout); // this is done if same sleep in continuously called in select
        tokio::pin!(heartbeat);

        loop {
            tokio::select! {
                // this is prone to stall if lock is contented hopefully this lock is only used
                // when changing state or reading which has to be exclusive among each ohter so the
                // contention good in this scenario as the other recv branch also depends on lock
                // state
                () = &mut timeout, if *(self.state.lock().unwrap()) != State::Leader => {
                    println!("Oh timer expired do something please onicha!!");
                    timeout.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                    todo!();
                },
                () = &mut heartbeat, if *(self.state.lock().unwrap()) == State::Leader => {
                    heartbeat.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                    self.handle_heartbeat().await?;
                },
                _req = ch.recv() => {
                    timeout.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                }
            }
        }
    }
}

struct RPCServer {
    sender: mpsc::Sender<RequestMsg<ResponseMsg>>,
}

#[tonic::async_trait]
impl RaftRpc for RPCServer {
    async fn request_vote_rpc(
        &self,
        request: Request<RequestVote>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let (o_tx, o_rx) = oneshot::channel::<ResponseMsg>();
        match self
            .sender
            .send(RequestMsg {
                msg: Msg::RequestVote(request.get_ref().clone()),
                sender: o_tx,
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e);
                todo!();
            }
        }
        match o_rx.await {
            Ok(v) => match v {
                ResponseMsg::RequestVoteResponse(msg) => Ok(Response::new(msg)),
                _ => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
            },
            Err(_) => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
        }
    }
    async fn append_entries_rpc(
        &self,
        _request: Request<AppendEntry>,
    ) -> Result<Response<AppendEntryResponse>, Status> {
        Err(Status::new(tonic::Code::Ok, "Nah"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx, mut rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let config = Config::load_config();
    let mut raft = Raft::init(&config)?;
    let rpc_server = RPCServer { sender: tx };
    let address = config.get_address();

    tokio::spawn(
        transport::Server::builder()
            .add_service(RaftRpcServer::new(rpc_server))
            .serve(address.parse().unwrap()),
    );
    let _ = raft.heart(&mut rx).await;
    Ok(())
}
