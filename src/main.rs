#![allow(unused_imports)]
#![allow(dead_code)]

use crate::logger::*;
use config::Config;
use core::fmt;
use proto::raft_rpc_client::RaftRpcClient;
use proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
use proto::{AppendEntry, AppendEntryResponse, RequestVote, RequestVoteResponse};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::process::Output;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, watch};
use tokio::time::{self, timeout, Duration, Instant};
use tonic::server;
use tonic::transport::channel;
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

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
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
    sender: mpsc::Sender<T>,
}

//#[derive(Debug)]
//struct Peer(Endpoint);

#[derive(Debug)]
struct ServerAgent {
    // for each server, index of the next log entry to send to that server (initialised to leader
    // last log index + 1)
    next_index: u64,

    // for each server index, of highest log entry known to be replicated on server (initialized to
    // 0, increases monotonically)
    match_index: u64,

    term: u64,

    endpoint: Endpoint,

    channel_ae: mpsc::Receiver<RequestMsg<ResponseMsg>>,
    channel_rv: mpsc::Receiver<RequestMsg<ResponseMsg>>,

    request_queue: VecDeque<AppendEntry>,
    watch_state: watch::Receiver<State>,
}

#[derive(Debug)]
struct AgentLink {
    endpoint: Endpoint,

    // communication channel between the server agent and main heart
    channel_ae: Option<mpsc::Sender<RequestMsg<ResponseMsg>>>,
    channel_rv: Option<mpsc::Sender<RequestMsg<ResponseMsg>>>,
}

#[derive(Debug)]
struct Raft {
    id: u64,

    // Updated on stable storage before responding to RPCs
    current_term: Arc<Mutex<u64>>, // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    voted_for: Option<u64>,
    logger: Arc<Mutex<Logger>>,

    state: Mutex<State>,
    votes: u64, // do we need mutex on this??
    majority: u64,
    servers: HashMap<u64, AgentLink>,
    config: Option<Config>,
}

impl Default for Raft {
    fn default() -> Self {
        Self {
            current_term: Arc::new(Mutex::new(0)),
            voted_for: None,
            id: 0,
            votes: 0,
            majority: 0, // todo this zero value looks waste; maybe change it to Option::None
            servers: HashMap::new(),
            logger: Arc::new(Mutex::new(Logger::default())),
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

async fn serve_server(
    mut sa: ServerAgent,
    logger: Arc<Mutex<Logger>>,
    term: Arc<Mutex<u64>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (tx, rx) = oneshot::channel();

    loop {
        if *sa.watch_state.borrow() == State::Follower {
            time::sleep_until(Instant::now() + Duration::from_millis(100)).await;
            continue;
        }
        let ae = sa.channel_ae.recv();
        let rv = sa.channel_rv.recv();
        tokio::pin!(rv);
        tokio::pin!(ae);

        tokio::select! {
            biased;

            Some(request_v) = rv => {
                // resetting server agents' volatile states
                sa.next_index = logger.lock().unwrap().get_prev_log_index() + 1;
                sa.match_index = 0;

                let mut client = RaftRpcClient::connect(sa.endpoint.clone()).await?;
                match request_v.msg {
                    Msg::RequestVote(req) => {
                        let res = client.request_vote_rpc(req).await?;
                        if let Err(err) = request_v.sender.send(ResponseMsg::RequestVoteResponse(res.into_inner())).await{
                            return Err(Box::new(MyError("Kand ho gaya bhai vote response nhi gaya heart ko!!".to_string())));
                        }
                    },
                    _ => ()
                }
            }
            Some(entry) = ae =>{},
        }
    }
}

impl Raft {
    fn init(config: Config) -> Result<Raft, Box<dyn Error>> {
        let mut raft = Raft::default();
        raft.logger.lock().unwrap().init(&config.get_log_config())?;

        if let Some(servers) = config.servers {
            for server in servers {
                let agt_link = AgentLink {
                    endpoint: Endpoint::from_shared(server.endpoint).ok().unwrap(),
                    channel_rv: None,
                    channel_ae: None,
                };
                raft.servers.insert(server.id, agt_link);
            }
        }
        // todo()
        // replicate all necessary configs into raft instance
        //raft.config = Some(config);
        Ok(raft)
    }

    fn term_checker(&self, term: u64) -> bool {
        if *self.current_term.lock().unwrap() > term {
            false
        } else {
            true
        }
    }

    // delete this method no use now
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
        let mut term = self.current_term.lock().unwrap();
        *term = *term + 1;

        // reinitialize server state after election
        for (_, server) in &mut self.servers {
            server.init(self.logger.lock().unwrap().get_prev_log_index());
        }
        let _lock = self.state.lock().unwrap();
        Ok(())
    }

    fn get_empty_append_entry(&self) -> AppendEntry {
        let ae = self.logger.lock().unwrap().get_empty_append_entry();
        AppendEntry {
            term: *self.current_term.lock().unwrap(), // will be auto copied as value is u64
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
                    let (o_tx, o_rx) = oneshot::channel();
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
        ch_ae: &mut Receiver<RequestMsg<ResponseMsg>>,
        ch_rv: &mut Receiver<RequestMsg<ResponseMsg>>,
    ) -> Result<(), Box<dyn Error>> {
        //tokio::spawn(Raft::heartbeat(&mut self));
        // todo randomize timeout duration
        let timeout =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
        let heartbeat =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_HEART_BEAT_PERIOD));

        let (tx_ws, rx_ws) = watch::channel(*(self.state.lock().unwrap()));

        tokio::pin!(timeout); // this is done if same sleep in continuously called in select
        tokio::pin!(heartbeat);

        // heart<--->server_agent<--->server1
        //     |
        //     <---->server_agent<--->server2
        for (_, ep) in &mut self.servers {
            let (tx_ae, rx_ae) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
            let (tx_rv, rx_rv) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);

            ep.channel_rv = Some(tx_rv);
            ep.channel_ae = Some(tx_ae);

            let sa = ServerAgent {
                next_index: self.logger.lock().unwrap().get_prev_log_index() + 1,
                match_index: 0,
                term: *self.current_term.lock().unwrap(),

                endpoint: ep.endpoint.clone(),
                channel_ae: rx_ae,
                channel_rv: rx_rv,
                watch_state: rx_ws.clone(),
                request_queue: VecDeque::new(),
            };
            tokio::spawn(serve_server(
                sa,
                self.logger.clone(),
                self.current_term.clone(),
            ));
        }

        loop {
            tokio::select! {
                biased;
                // this is prone to stall if lock is contented hopefully this lock is only used
                // when changing state or reading which has to be exclusive among each ohter so the
                // contention good in this scenario as the other recv branch also depends on lock
                // state
                () = &mut timeout, if *(self.state.lock().unwrap()) != State::Leader => {
                    // todo re-code this portion to listen to incoming request votes from other
                    // candidates maybe this work perfectly say this server has timedout and voted
                    // for itself so any other vote request should be reject by this one also if
                    // some other candidate is selected as a leader first that means this node will
                    // not receive enough vote and eventually gets timed out. Now this heart should
                    // wait after election period so that the other leader can ack this server
                    // but if this server becomes the leader then immediately it will send ack to
                    // other follower, this operation will be carried by the server agents
                    // automatically as the server will again become a leader
                    println!("Oh timer expired do something please onicha!!");
                    timeout.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                    *self.state.lock().unwrap() = State::Candidate;
                    *self.current_term.lock().unwrap() += 1;
                    self.votes = 0;

                    self.voted_for = Some(self.id);
                    let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize); // make the default channel capacity into number of servers it is waiting for
                    for (_, agt_link) in self.servers {
                        if let Some(ch) = agt_link.channel_rv {
                            let request = Msg::RequestVote(
                                RequestVote {
                                    term: *self.current_term.lock().unwrap(),
                                    candidate_id: self.id,
                                    last_log_index: self.logger.get_prev_log_index(),
                                    last_log_term: self.logger.get_prev_log_term()
                                }
                            );
                            ch.send(RequestMsg {
                                msg: request,
                                sender: tx.clone()
                            });
                        }
                    }
                    loop {
                        // it should wait for some time for the vote responses before the timeout
                        // timeout will be detected once all the server agents waiting for response
                        // gets timed out and returns a false response; enough failed responses
                        // will fail achieving majority votes
                        if let Some(res) = rx.recv().await {
                            match res {
                                ResponseMsg::RequestVoteResponse(v_res) => {
                                    if self.term_checker(v_res.term) && v_res.vote_granted{
                                        self.votes += 1;
                                    }
                                    if self.votes >= self.majority {
                                        // change the state to leader
                                        *self.state.lock().unwrap() = State::Leader;
                                        break;
                                    }
                                }
                                _ => ()
                            }
                        }else{
                            println!("This node has lost leader election!!");
                            // now wait for ack from other candidate
                            timeout.reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                        }
                    }
                },
                () = &mut heartbeat, if *(self.state.lock().unwrap()) == State::Leader => {
                    heartbeat.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                    self.handle_heartbeat().await?;
                },
                _req = ch_ae.recv() => {
                    timeout.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                },
                _req = ch_rv.recv() => {

                }
            }
        }
    }
}

struct RPCServer {
    sender_ae: mpsc::Sender<RequestMsg<ResponseMsg>>,
    sender_rv: mpsc::Sender<RequestMsg<ResponseMsg>>,
}

#[tonic::async_trait]
impl RaftRpc for RPCServer {
    async fn request_vote_rpc(
        &self,
        request: Request<RequestVote>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let (o_tx, o_rx) = oneshot::channel::<ResponseMsg>();
        match self
            .sender_ae
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
    let (tx_ae, mut rx_ae) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let (tx_rv, mut rx_rv) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let config = Config::load_config();
    let address = config.get_address();
    let mut raft = Raft::init(config)?;
    let rpc_server = RPCServer {
        sender_ae: tx_ae,
        sender_rv: tx_rv,
    };

    tokio::spawn(
        transport::Server::builder()
            .add_service(RaftRpcServer::new(rpc_server))
            .serve(address.parse().unwrap()),
    );
    let _ = raft.heart(&mut rx_ae, &mut rx_rv).await;
    Ok(())
}
