use bitcoin::consensus::{encode, Decodable};
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::Address;
use bitcoin::Network;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const BUFSZ: usize = 4096;
const CHANNEL_BUF_SZ: usize = 10;
const PROTOCOL_VERSION: u32 = 70001;
const MAX_PEERS: usize = 500;
#[derive(Debug, StructOpt)]
#[structopt(name = "mystruct", about = "about string")]
struct ProgramArgs {
    #[structopt(short, long)]
    nodes: Vec<SocketAddr>,
}

#[derive(Debug)]
struct ReceiverMsg {
    id: SocketAddr,
    msg: ReceiverMsgContent,
}

#[derive(Debug)]
enum ReceiverMsgContent {
    IncomingMessage(NetworkMessage),
    ReceiverDown,
    ReceiverUp(),
}

struct ReceiverPeerState {
    sock_read_chan: oneshot::Receiver<OwnedReadHalf>,
    sender: Sender<ReceiverMsg>,
    peer_addr: SocketAddr,
}

#[derive(Debug)]
struct PeerState {
    received_ver: bool,
    received_ver_ack: bool,
    sender: Sender<NetworkMessage>,
    address: SocketAddr,
    sender_thread_handle: JoinHandle<()>,
    receiver_thread_handle: JoinHandle<()>,
}
struct SenderPeerState {
    receiver: Receiver<NetworkMessage>,
    sock_write_chan: oneshot::Receiver<OwnedWriteHalf>,
    peer_addr: SocketAddr,
}

pub struct StreamReader<R: AsyncRead + Unpin, const BSIZE: usize> {
    pub stream: R,
    data: [u8; BSIZE],
    unparsed: Vec<u8>,
}

impl<R: AsyncRead + Unpin, const BSIZE: usize> StreamReader<R, BSIZE> {
    pub fn new(stream: R) -> StreamReader<R, BSIZE> {
        StreamReader {
            stream,
            data: [0u8; BSIZE],
            unparsed: Vec::with_capacity(512),
        }
    }
    pub async fn read_next<D: Decodable>(&mut self) -> Result<D, encode::Error> {
        loop {
            match encode::deserialize_partial::<D>(&self.unparsed) {
                Err(encode::Error::Io(ref err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    let count = self.stream.read(&mut self.data).await?;
                    if count > 0 {
                        self.unparsed.extend(self.data[0..count].iter());
                    } else {
                        return Err(encode::Error::Io(io::Error::from(
                            io::ErrorKind::UnexpectedEof,
                        )));
                    }
                }
                Err(err) => return Err(err),
                Ok((message, index)) => {
                    self.unparsed.drain(..index);
                    return Ok(message);
                }
            }
        }
    }
}
async fn connect_to_peer(addr: &SocketAddr) -> Option<(OwnedReadHalf, OwnedWriteHalf)> {
    println!("Connecting to: {}", addr);
    let sock = TcpStream::connect(addr).await;
    if sock.is_err() {
        return None;
    }
    let (sock_read, mut sock_write) = sock.unwrap().into_split();
    if send_ver_to_peer(&mut sock_write).await.is_ok() {
        return Some((sock_read, sock_write));
    }
    None
}

fn get_current_height() -> i32 {
    712447i32
}
async fn send_ver_to_peer(sock: &mut OwnedWriteHalf) -> Result<(), ()> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let ver_msg = VersionMessage {
        version: PROTOCOL_VERSION,
        timestamp: timestamp.as_secs() as i64,
        services: ServiceFlags::NONE,
        receiver: Address::new(&sock.peer_addr().unwrap(), ServiceFlags::NONE),
        sender: Address::new(&sock.local_addr().unwrap(), ServiceFlags::NONE),
        nonce: 0u64,
        relay: true,
        user_agent: "TinySybil".to_string(),
        start_height: get_current_height(),
    };
    let net_msg = NetworkMessage::Version(ver_msg);
    let raw_msg = RawNetworkMessage {
        magic: Network::Bitcoin.magic(),
        payload: net_msg,
    };
    if sock
        .write_all(encode::serialize(&raw_msg).as_slice())
        .await
        .is_ok()
    {
        Ok(())
    } else {
        Err(())
    }
}

async fn sender_peer_thread(mut state: SenderPeerState) -> () {
    let mut socket = match state.sock_write_chan.await {
        Err(e) => {
            // println!("[SENDER] Failed receive socket write half. Error: {}", e);
            return;
        }
        Ok(sock) => sock,
    };
    loop {
        match state.receiver.recv().await {
            None => {
                println!("Sender thread leaving: {}", state.peer_addr);
                return;
            }
            Some(msg) => {
                let raw_msg = RawNetworkMessage {
                    magic: Network::Bitcoin.magic(),
                    payload: msg,
                };
                let bytes = encode::serialize(&raw_msg);
                if let Err(e) = socket.write(&bytes).await {
                    println!("Failed sending! Error: {}", e);
                }
            }
        };
    }
}
async fn receiver_peer_thread(state: ReceiverPeerState) -> () {
    let socket = match state.sock_read_chan.await {
        Err(e) => {
            // println!("[RECEIVER] Failed to get sock half! Error: {}", e);
            return;
        }
        Ok(sock) => sock,
    };
    let mut stream_in = StreamReader::<_, BUFSZ>::new(socket);
    state
        .sender
        .send(ReceiverMsg {
            id: state.peer_addr,
            msg: ReceiverMsgContent::ReceiverUp(),
        })
        .await
        .expect("Failed to tell I'm up, nobody listens anyway... just leaving...");
    loop {
        match stream_in.read_next::<RawNetworkMessage>().await {
            Err(err) => {
                println!("Receiver got error: {}", err);
                state
                    .sender
                    .send(ReceiverMsg {
                        id: state.peer_addr,
                        msg: ReceiverMsgContent::ReceiverDown,
                    })
                    .await
                    .expect("Can't report I'm down... this is really weird, leaving!");
                return;
            }
            Ok(n) => {
                if let Err(x) = state
                    .sender
                    .send(ReceiverMsg {
                        id: state.peer_addr,
                        msg: ReceiverMsgContent::IncomingMessage(n.payload),
                    })
                    .await
                {
                    println!("Receiver channel error: {}", x);
                    state
                        .sender
                        .send(ReceiverMsg {
                            id: state.peer_addr,
                            msg: ReceiverMsgContent::ReceiverDown,
                        })
                        .await
                        .expect("Can't report I'm down... this is really weird, leaving!");
                    return;
                }
            }
        }
    }
}

fn add_peer(
    peer_addr: SocketAddr,
    sender: Sender<ReceiverMsg>,
    peer_states: &mut HashMap<SocketAddr, PeerState>,
) {
    let (sender_msg, receiver_msg) = channel(CHANNEL_BUF_SZ);
    let (sender_write_half, receiver_write_half) = oneshot::channel();
    let (sender_read_half, receiver_read_half) = oneshot::channel();
    let receiver_state = ReceiverPeerState {
        sock_read_chan: receiver_read_half,
        sender,
        peer_addr: peer_addr.clone(),
    };
    let sender_peer_state = SenderPeerState {
        sock_write_chan: receiver_write_half,
        receiver: receiver_msg,
        peer_addr: peer_addr,
    };
    let receiver_thread_handle = tokio::spawn(receiver_peer_thread(receiver_state));
    let sender_thread_handle = tokio::spawn(sender_peer_thread(sender_peer_state));
    let peer_state = PeerState {
        address: peer_addr,
        sender: sender_msg,
        received_ver: false,
        received_ver_ack: false,
        sender_thread_handle,
        receiver_thread_handle,
    };
    peer_states.insert(peer_addr, peer_state);
    tokio::spawn(async move {
        if let Some((sock_read, sock_write)) = connect_to_peer(&peer_addr).await {
            sender_write_half.send(sock_write).unwrap();
            sender_read_half.send(sock_read).unwrap();
        }
    });
}

async fn handle_peers(
    mut incoming_rec: Receiver<ReceiverMsg>,
    mut peer_states: HashMap<SocketAddr, PeerState>,
    incoming_sender: Sender<ReceiverMsg>,
) {
    loop {
        let msg = match incoming_rec.recv().await {
            None => {
                println!("HANDLER LEAVING!");
                return;
            }
            Some(msg) => msg,
        };
        let peer_id = msg.id;
        let peer_state = match peer_states.get(&peer_id) {
            None => {
                panic!("This shouldn't happen! Got message from unknown peer!");
            }
            Some(e) => e,
        };
        let network_msg = match msg.msg {
            ReceiverMsgContent::ReceiverDown => {
                peer_states.remove(&peer_id);
                println!("Ditching peer: {}", peer_id);
                println!("Peer count:{}", peer_states.len());
                continue;
            }
            ReceiverMsgContent::ReceiverUp() => {
                println!("Receiver Up");
                continue;
            }
            ReceiverMsgContent::IncomingMessage(msg) => msg,
        };
        let peer = match peer_states.get_mut(&peer_id) {
            None => {
                println!("I thought peer {} would exist, but it doesn't", peer_id);
                continue;
            }
            Some(peer) => peer,
        };
        let channel_out = &mut peer.sender;
        match network_msg {
            NetworkMessage::Version(_) => {
                if peer.received_ver {
                    dbg!(format!("Already received verack from peer {}", msg.id));
                    continue;
                }
                peer.received_ver = true;
                let verack = NetworkMessage::Verack;
                if let Err(_) = channel_out.send(verack).await {
                    peer_states.remove(&peer_id);
                }
            }
            NetworkMessage::Verack => {
                if peer.received_ver_ack {
                    println!("Already received verack from peer {}", msg.id);
                    continue;
                }
                peer.received_ver_ack = true;
            }
            NetworkMessage::Ping(nonce) => {
                let pong_msg = NetworkMessage::Pong(nonce);
                if let Err(_) = peer.sender.send(pong_msg).await {
                    peer_states.remove(&peer_id);
                    continue;
                }
                if let Err(e) = peer.sender.send(NetworkMessage::GetAddr).await {
                    println!(
                        "[MAIN] Failed to send GetAddr to peer. Ditching. Error: {}",
                        e
                    );
                    peer_states.remove(&peer_id);
                    continue;
                };
            }
            NetworkMessage::Addr(addr_msg) => {
                addr_msg
                    .into_iter()
                    .filter_map(|(_, addr)| match addr.to_socket_addrs() {
                        Err(e) => {
                            println!("Failed to parse address! Error: {}", e);
                            None
                        }
                        Ok(sock_addr) => {
                            let sock_addrs: Vec<_> = sock_addr.collect();
                            Some(sock_addrs)
                        }
                    })
                    .for_each(|peer_addrs| {
                        peer_addrs.into_iter().for_each(|peer_addr| {
                            if peer_states.contains_key(&peer_addr) {
                                return;
                            }
                            if peer_states.len() > MAX_PEERS {
                                return;
                            }
                            println!("Peer count:{}", peer_states.len());
                            add_peer(peer_addr, incoming_sender.clone(), &mut peer_states);
                        })
                    });
            }
            NetworkMessage::AddrV2(addr_msg) => {
                addr_msg
                    .into_iter()
                    .take(5)
                    .filter_map(|addr| match addr.to_socket_addrs() {
                        Err(e) => {
                            println!("Failed to parse address! Error: {}", e);
                            None
                        }
                        Ok(mut sock_addr) => sock_addr.nth(0),
                    })
                    .for_each(|peer_addr| {
                        if !peer_states.contains_key(&peer_addr) {
                            add_peer(peer_addr, incoming_sender.clone(), &mut peer_states)
                        }
                    });
            }

            _ => {
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let arg = ProgramArgs::from_args();
    let (incoming_send, incoming_rec) = channel::<ReceiverMsg>(CHANNEL_BUF_SZ);
    let mut peer_states = HashMap::<SocketAddr, PeerState>::new();
    arg.nodes
        .into_iter()
        .for_each(|addr| add_peer(addr, incoming_send.clone(), &mut peer_states));
    handle_peers(incoming_rec, peer_states, incoming_send).await;
    println!("Leaving... bye");
}
