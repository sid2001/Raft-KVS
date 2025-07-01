#![allow(unused_imports)]
#![allow(dead_code)]

use crate::logger::*;
use config::Config;
use core::fmt;
use proto::raft_rpc_client::RaftRpcClient;
use proto::raft_rpc_server::{RaftRpc, RaftRpcServer};
use proto::{AppendEntry, AppendEntryResponse, RequestVote, RequestVoteResponse};
use rand::prelude::*;
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
use tokio::time::{self, sleep_until, timeout, Duration, Instant};
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
const DEFAULT_TIME_OUT_PERIOD: u64 = 2000;

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

impl ResponseMsg {
    fn new_vr(term: u64, vote_granted: bool, candidate_id: u64) -> ResponseMsg {
        ResponseMsg::RequestVoteResponse(RequestVoteResponse {
            term,
            vote_granted,
            candidate_id,
        })
    }
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

    state: Arc<Mutex<State>>,
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
            state: Arc::new(Mutex::new(State::Follower)),
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

fn check_state_leader(state: watch::Receiver<State>) -> bool {
    //println!("checking state leader");
    let ans = *state.borrow() == State::Leader;
    //println!("{:?}", ans);
    ans
}

fn check_state_follower_not(state: watch::Receiver<State>) -> bool {
    //println!("checking state follower not");
    let ans = *state.borrow() != State::Follower;
    //println!("not follower: {:?}", ans);
    ans
}

async fn serve_server(
    mut sa: ServerAgent,
    logger: Arc<Mutex<Logger>>,
    term: Arc<Mutex<u64>>,
    state: Arc<Mutex<State>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    //let (tx, rx) = oneshot::channel();
    println!("Server Agent spawned");
    loop {
        //let watch_state = *sa.watch_state.borrow();
        //if watch_state == State::Follower {
        //    time::sleep_until(Instant::now() + Duration::from_millis(100)).await;
        //    continue;
        //}
        let ae = sa.channel_ae.recv();
        let rv = sa.channel_rv.recv();
        let deadline = Instant::now() + Duration::from_millis(100);
        let heartbeat = time::sleep_until(deadline);
        tokio::pin!(rv);
        tokio::pin!(heartbeat);
        tokio::pin!(ae);

        tokio::select! {
            biased;

            Some(request_v) = rv => {
                if !check_state_follower_not(sa.watch_state.clone()) {continue;}
                println!("Request vote(SA)");
                // resetting server agents' volatile states
                sa.next_index = logger.lock().unwrap().get_prev_log_index() + 1;
                sa.match_index = 0;

                match RaftRpcClient::connect(sa.endpoint.clone().timeout(Duration::from_millis(500))).await {
                    Ok(mut client) => {
                        match request_v.msg {
                            Msg::RequestVote(req) => {
                                // repeat vote request if response is not received ---todo once the timing is
                                // decided for election timeout and request timeout
                                let res = client.request_vote_rpc(req).await?;
                                if let Err(_) = request_v.sender.send(ResponseMsg::RequestVoteResponse(res.into_inner())).await{
                                    println!("Election got over before the response was received or maybe an error!!");
                                }
                            },
                        _ => ()
                        }
                    }
                    Err(_) => {
                        eprintln!("Couldn't connect with the server");
                    }
                };
                drop(request_v.sender);
            }
            Some(_entry) = ae =>{},
            () = heartbeat => {
                if !check_state_leader(sa.watch_state.clone()) {continue;}
                //println!("watch state {:?}",(watch_state == State::Leader));
                match RaftRpcClient::connect(sa.endpoint.clone().timeout(Duration::from_millis(500))).await {
                    Ok(mut client) => {
                        //println!("lub dub");
                        let ae = AppendEntry{
                                term: *term.lock().unwrap(),
                                leader_id: 0,
                                prev_log_term:0,
                                prev_log_index: 0,
                                entries: vec![],
                                leader_commit: 0
                            };
                        client.append_entries_rpc(ae).await?;
                    }
                    Err(_) => {
                        eprintln!("Couldn't connect with the server");
                    }
                };
            },
            else => {
                println!("getting skipped flag");
                continue;
            }
        }
    }
}

impl Raft {
    fn init(config: Config) -> Result<Raft, Box<dyn Error>> {
        let mut raft = Raft::default();
        raft.logger.lock().unwrap().init(&config.get_log_config())?;
        raft.id = config.server.id;
        raft.majority = config.majority;

        for (i, server) in config.servers.iter().enumerate() {
            println!("init servers: {}", raft.id);
            if i as u64 == (raft.id - 1) {
                continue;
            }
            let agt_link = AgentLink {
                endpoint: Endpoint::from_shared(server.endpoint.clone()).ok().unwrap(),
                channel_rv: None,
                channel_ae: None,
            };

            raft.servers.insert(server.id, agt_link);
        }
        // todo()
        // replicate all necessary configs into raft instance
        //raft.config = Some(config);
        Ok(raft)
    }

    fn term_checker_greater(current_term: u64, term: u64) -> bool {
        if current_term < term {
            true
        } else {
            false
        }
    }

    fn term_checker_equal(current_term: u64, term: u64) -> bool {
        if current_term == term {
            true
        } else {
            false
        }
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

    async fn heart(
        &mut self,
        ch_ae: &mut Receiver<RequestMsg<ResponseMsg>>,
        ch_rv: &mut Receiver<RequestMsg<ResponseMsg>>,
    ) -> Result<(), Box<dyn Error>> {
        //tokio::spawn(Raft::heartbeat(&mut self));
        // todo randomize timeout duration
        let heartbeat =
            time::sleep_until(Instant::now() + Duration::from_millis(DEFAULT_HEART_BEAT_PERIOD));

        let (tx_ws, rx_ws) = watch::channel(*(self.state.lock().unwrap()));
        tx_ws.send(State::Follower);

        tokio::pin!(heartbeat);

        // heart<--->server_agent<--->server1
        //     |
        //     <---->server_agent<--->server2
        println!("server count: {}", self.servers.len());
        for (_, ep) in &mut self.servers {
            println!("Spawning server agent!!");
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
                self.state.clone(),
            ));
        }

        loop {
            let interval = rand::random_range(5000..=6000);
            let timeout = time::sleep_until(Instant::now() + Duration::from_millis(interval));
            tokio::pin!(timeout);
            {
                let state = self.state.lock().unwrap();
                let logger = self.logger.lock().unwrap();
                let term = self.current_term.lock().unwrap();
                print!("\x1B[2J\x1B[H"); // ANSI escape: clear screen + move cursor to top-left
                println!("===== Raft Node [{}] State =====", self.id);
                println!("Role           : {:?}", *state);
                println!("Current Term   : {}", *term);
                println!("Voted For      : {:?}", self.voted_for);
                println!("Commit Index   : ");
                //println!("Last Applied   : {}", state.last_applied);
                //println!("Log Entries    : {}", state.log_len);
            }
            tokio::select! {
                //biased;
                // this is prone to stall if lock is contented hopefully this lock is only used
                // when changing state or reading which has to be exclusive among each ohter so the
                // contention good in this scenario as the other recv branch also depends on lock
                // state
                () = &mut timeout => {
                    if *(self.state.lock().unwrap()) == State::Leader {continue;}

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
                    {*self.state.lock().unwrap() = State::Candidate; // --wrap
                    println!("flag 1");
                    *self.current_term.lock().unwrap() += 1;}
                    println!("flag 2");
                    self.votes = 1;
                    if let Err(_) = tx_ws.send(State::Candidate) {
                        println!("watch couldnt update");
                    }

                    self.voted_for = Some(self.id);
                    let (tx, mut rx) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize); // make the default channel capacity into number of servers it is waiting for
                    for (_, agt_link) in &self.servers {
                        if let Some(ref ch) = agt_link.channel_rv {
                            println!("flag3");
                            let logger = self.logger.lock().unwrap();
                            println!("flag4");
                            let term = self.current_term.lock().unwrap();
                            println!("flag5");
                            let request = Msg::RequestVote(
                                RequestVote {
                                    term: *term,
                                    candidate_id: self.id,
                                    last_log_index: logger.get_prev_log_index(),
                                    last_log_term: logger.get_prev_log_term()
                                }
                            );
                            drop(logger);
                            drop(term);
                            if let Err(_) = ch.send(RequestMsg {
                                msg: request,
                                sender: tx.clone()
                            }).await {
                                println!("send error rv from heart to sa");
                            }
                            println!("flag8")
                        }
                    }
                    drop(tx);
                    loop {
                        println!("flag9 {:?}",*rx_ws.borrow());
                        // it should wait for some time for the vote responses before the timeout
                        // timeout will be detected once all the server agents waiting for response
                        // gets timed out and returns a false response; enough failed responses
                        // will fail achieving majority votes
                        if let Some(res) = rx.recv().await {
                            println!("flag10");
                            match res {
                                ResponseMsg::RequestVoteResponse(v_res) => {
                                    let mut state = self.state.lock().unwrap();
                                    let mut term = self.current_term.lock().unwrap();
                                    println!("Received res msg from sa term {}, cand {} granted {} curr_term {}",v_res.term,v_res.candidate_id,v_res.vote_granted, *term);
                                    if Raft::term_checker_greater(*term,v_res.term) {
                                        *term = v_res.term; // -- wrap
                                        *state = State::Follower;
                                        self.voted_for = None;
                                        tx_ws.send(State::Follower);
                                        //timeout.as_mut().reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                                        break;
                                    } else if Raft::term_checker_equal(*term, v_res.term) && v_res.vote_granted {
                                        println!("one vote recvd from {}",v_res.candidate_id);
                                        self.votes += 1;
                                    }
                                    if self.votes >= self.majority {
                                        println!("Election won. term {} votes {} maj {}",*term, self.votes, self.majority);
                                        // change the state to leader
                                        //
                                        // todo wrap state change functionality into a function or
                                        // a method
                                        *state = State::Leader;
                                        tx_ws.send(State::Leader);
                                        break;
                                    }
                                }
                                _ => ()
                            }
                        } else {
                            println!("This node has lost leader election!!");
                            // now wait for ack from other candidate
                            //timeout.as_mut().reset(Instant::now() + Duration::from_millis(10000));
                            break;
                        }
                        println!("flag6");
                    }
                    println!("flag7");
                },
                Some(req) = ch_ae.recv() => {
                    println!("heartbeat");
                    match req.msg {
                            Msg::AppendEntry(e) => {
                                if Raft::term_checker_greater(*self.current_term.lock().unwrap(),e.term) {
                                    *self.state.lock().unwrap() = State::Follower;
                                    tx_ws.send(State::Follower);
                                    *self.current_term.lock().unwrap() = e.term;
                                }
                                let res = AppendEntryResponse{term: 0,success: true, candidate_id: self.id};
                                req.sender.send(ResponseMsg::AppendEntryResponse(res)).await?;
                            },
                            _ => ()
                        }
                },
                Some(req) = ch_rv.recv() => {
                    match req.msg {
                        Msg::RequestVote(r) => {
                            println!("Vote request is being evaluated");
                            // if term < current_term or already voted then return vote not granted
                            // if term >= current_term and log index is not updated
                            // todo another check for log term when repeated request vote is
                            // implemented
                            let res;
                            let mut term = self.current_term.lock().unwrap();
                            let mut state = self.state.lock().unwrap();
                            let logger = self.logger.lock().unwrap();
                            if Raft::term_checker_greater(*term, r.term){
                                *term = r.term;
                                *state = State::Follower;
                                self.voted_for = None;
                                //timeout.reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));

                                if logger.log_index_term_checker(r.last_log_index, r.last_log_term) {
                                    // ---yes
                                    self.voted_for = Some(r.candidate_id);
                                    res = ResponseMsg::new_vr(r.term, true,self.id);
                                } else {
                                    // --no
                                    res = ResponseMsg::new_vr(r.term, false,self.id);
                                }
                            } else if Raft::term_checker_equal(*term, r.term) {
                                if (self.voted_for.is_none() && logger.log_index_term_checker(r.last_log_index,r.last_log_term)) || (self.voted_for.unwrap() == r.candidate_id && logger.log_index_term_checker_equal(r.last_log_index, r.last_log_term)) {
                                    // --yes
                                    self.voted_for = Some(r.candidate_id);
                                    res = ResponseMsg::new_vr(r.term, true, self.id);
                                    //timeout.reset(Instant::now() + Duration::from_millis(DEFAULT_TIME_OUT_PERIOD));
                                } else {
                                    // --no
                                    res = ResponseMsg::new_vr(r.term, false, self.id);
                                }
                            } else {
                                // --no
                                res = ResponseMsg::new_vr(*term, false, self.id);
                            }
                            if self.voted_for.is_none() || self.voted_for.unwrap() != self.id {
                                *state = State::Follower;
                            }
                            tx_ws.send((*state).clone());
                            println!("evaluation completed");
                            match req.sender.send(res).await {
                                Err(err) => {
                                    println!("Can't send response to rpc handler!!");
                                    return Err(Box::new(err));
                                }
                                _ => ()
                            }
                        },
                        _ => ()
                    }
                },
                else => {
                    println!("heart select else");
                    continue;
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
        let (tx, mut rx) = mpsc::channel::<ResponseMsg>(DEFAULT_CHANNEL_CAPACITY as usize);
        println!("Request vote incoming");
        match self
            .sender_rv
            .send(RequestMsg {
                msg: Msg::RequestVote(request.get_ref().clone()),
                sender: tx,
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e);
                todo!();
            }
        }
        match rx.recv().await {
            Some(v) => {
                println!("vote res received");
                match v {
                    ResponseMsg::RequestVoteResponse(msg) => Ok(Response::new(msg)),
                    _ => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
                }
            }
            None => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
        }
    }
    async fn append_entries_rpc(
        &self,
        request: Request<AppendEntry>,
    ) -> Result<Response<AppendEntryResponse>, Status> {
        let (tx, mut rx) = mpsc::channel::<ResponseMsg>(DEFAULT_CHANNEL_CAPACITY as usize);
        println!("AppendEntry incoming");
        match self
            .sender_ae
            .send(RequestMsg {
                msg: Msg::AppendEntry(request.get_ref().clone()),
                sender: tx,
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                println!("{:?}", e);
                todo!();
            }
        }
        match rx.recv().await {
            Some(v) => {
                println!("ae res received");
                match v {
                    ResponseMsg::AppendEntryResponse(msg) => Ok(Response::new(msg)),
                    _ => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
                }
            }
            None => Err(Status::new(tonic::Code::Unknown, "hm something went wrong")),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx_ae, mut rx_ae) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let (tx_rv, mut rx_rv) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY as usize);
    let config = Config::load_config()?;
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
