#![allow(unused_imports)]
#![allow(dead_code)]

use crate::logger::*;
use config::Config;
use proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
use proto::{AppendEntry, AppendEntryResponse, RequestVote, RequestVoteResponse};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{self, Duration, Instant};
use tonic::{transport::Server, Request, Response, Status};

mod config;
mod logger;
mod proto {
    tonic::include_proto!("raft");
}

trait RPCResponse {}

const DEFAULT_CHANNEL_CAPACITY: u32 = 32;
const DEFAULT_TIME_OUT_PERIOD: u64 = 1000;

#[derive(Debug)]
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

#[derive(Debug)]
struct Raft {
    current_term: u64,
    voted_for: Option<u32>,
    logger: Logger,
    state: State,
    servers: Option<Vec<Server>>,
    config: Option<Config>,
    request_ch_r: Option<mpsc::Receiver<RequestMsg<ResponseMsg>>>,
}

impl Default for Raft {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            logger: Logger::default(),
            state: State::Follower,
            servers: None,
            config: None,
            request_ch_r: None,
        }
    }
}

impl Raft {
    fn init(
        config: &Config,
        rx: mpsc::Receiver<RequestMsg<ResponseMsg>>,
    ) -> Result<Raft, Box<dyn Error>> {
        let mut raft = Raft::default();
        raft.logger.init(&config.get_log_config())?;
        raft.request_ch_r = Some(rx);
        // todo()
        // replicate all necessary configs into raft instance
        //raft.config = Some(config);
        Ok(raft)
    }
    fn time_out() {}
    async fn heart(&mut self) -> Result<(), ()> {
        let sleep =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
        let ch = match self.request_ch_r.as_mut() {
            Some(ch) => ch,
            None => return Err(()),
        };
        tokio::pin!(sleep); // this is done if same sleep in continuously called in select

        loop {
            tokio::select! {
                () = &mut sleep => {
                    println!("Oh timer expired do something please onicha!!");
                    sleep.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                },
                _req = ch.recv() => {
                    sleep.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
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
    let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let config = Config::load_config();
    let mut raft = Raft::init(&config, rx)?;
    let rpc_server = RPCServer { sender: tx };
    let address = config.get_address();

    tokio::spawn(
        Server::builder()
            .add_service(RaftRpcServer::new(rpc_server))
            .serve(address.parse().unwrap()),
    );
    let _ = raft.heart().await;
    Ok(())
}
