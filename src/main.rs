use bitcoin::consensus::{encode, Decodable};
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::Address;
use bitcoin::Network;
use futures::future::{join_all, try_join_all};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const BUFSZ: usize = 4096;
const CHANNEL_BUF_SZ: usize = 10;
const PROTOCOL_VERSION: u32 = 70016;
const HARVESTER_FREQUENCY_SECS: u64 = 20;
#[derive(Debug, StructOpt)]
#[structopt(name = "mystruct", about = "about string")]
struct ProgramArgs {
    #[structopt(short, long)]
    nodes: Vec<SocketAddr>,
}

#[derive(Debug)]
struct IncomingMessage {
    peer_addr: SocketAddr,
    msg: NetworkMessage,
}

struct ReceiverPeerState {
    stream_in: StreamReader<OwnedReadHalf, BUFSZ>,
    sender: Sender<IncomingMessage>,
    peer_addr: SocketAddr,
}

struct PeerState {
    received_ver: bool,
    received_ver_ack: bool,
    receiver_handle: JoinHandle<()>,
    outgoing_sock: OwnedWriteHalf,
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

async fn receiver_peer_thread(mut state: ReceiverPeerState) -> () {
    loop {
        match state.stream_in.read_next::<RawNetworkMessage>().await {
            Err(err) => {
                dbg!(format!(
                    "Got error: {:?} - Peer Addr: {:?} ",
                    err, state.peer_addr
                ));
                return;
            }
            Ok(n) => {
                if let Err(x) = state
                    .sender
                    .send(IncomingMessage {
                        peer_addr: state.peer_addr,
                        msg: n.payload,
                    })
                    .await
                {
                    dbg!(format!("Receiver channel error: {:?}", x));
                    return;
                }
            }
        }
    }
}

async fn send_network_message(
    payload: NetworkMessage,
    sock: &mut OwnedWriteHalf,
) -> Result<(), ()> {
    let raw_msg = RawNetworkMessage {
        magic: Network::Bitcoin.magic(),
        payload,
    };
    let bytes = encode::serialize(&raw_msg);
    if let Err(error) = sock.write_all(&bytes).await {
        dbg!(format!("Error in sender thread: {:?}", error));
        return Err(());
    }
    Ok(())
}
async fn bootstrap_harvester(out_peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>) {
    loop {
        sleep(Duration::from_secs(HARVESTER_FREQUENCY_SECS));
        println!("Fetching addresses...");
        let mut peers = out_peers.lock().await;
        try_join_all(peers.iter_mut().map(|(addr, peer)| {
            let get_msg = NetworkMessage::GetAddr;
            send_network_message(get_msg, &mut peer.outgoing_sock)
        }))
        .await
        .expect("Failed to harvet addresses!");
    }
}

async fn add_receiver(
    peer_addr: SocketAddr,
    sender: Sender<IncomingMessage>,
) -> Option<(SocketAddr, PeerState)> {
    println!("Adding received! Addr: {}", peer_addr);
    if let Some((sock_read, outgoing_sock)) = connect_to_peer(&peer_addr).await {
        let receiver_state = ReceiverPeerState {
            stream_in: StreamReader::new(sock_read),
            sender,
            peer_addr: peer_addr.clone(),
        };
        let receiver_handle = tokio::spawn(async move {
            receiver_peer_thread(receiver_state).await;
        });

        return Some((
            peer_addr,
            PeerState {
                received_ver: false,
                received_ver_ack: false,
                receiver_handle,
                outgoing_sock,
            },
        ));
    }
    None
}

async fn handle_addr_message(
    addresses: &mut Vec<SocketAddr>,
    sock_addrs: Vec<SocketAddr>,
    incoming_sender: Sender<IncomingMessage>,
) -> Vec<(SocketAddr, PeerState)> {
    join_all(sock_addrs.into_iter().filter_map(|sock_addr| {
        if !addresses.contains(&sock_addr) {
            addresses.push(sock_addr);
            println!("Added another address: {}", sock_addr);
            println!("Total addresses: {}", addresses.len());
            return Some(add_receiver(sock_addr, incoming_sender.clone()));
        }
        None
    }))
    .await
    .into_iter()
    .filter_map(|x| x)
    .collect()
}

async fn handle_peers(
    peers_states: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    mut incoming_rec: Receiver<IncomingMessage>,
    incoming_sender: Sender<IncomingMessage>,
) {
    let mut addresses = {
        let peer_states_locked = peers_states.lock().await;
        peer_states_locked.keys().map(|addr| addr.clone()).collect()
    };
    loop {
        let msg = match incoming_rec.recv().await {
            None => return,
            Some(msg) => msg,
        };
        let mut peers_states_locked = peers_states.lock().await;
        let peer = &mut peers_states_locked
            .get_mut(&msg.peer_addr)
            .expect("The key should have existed!");
        let sock_out = &mut peer.outgoing_sock;
        let network_msg = msg.msg;
        // dbg!(&network_msg);
        match network_msg {
            NetworkMessage::Version(_) => {
                if peer.received_ver {
                    dbg!(format!(
                        "Already received verack from peer {}",
                        msg.peer_addr
                    ));
                    continue;
                }
                peer.received_ver = true;
                let verack = NetworkMessage::Verack;
                send_network_message(verack, sock_out)
                    .await
                    .expect("Failed to send network message");
            }
            NetworkMessage::Verack => {
                if peer.received_ver_ack {
                    println!("Already received verack from peer {}", msg.peer_addr);
                    continue;
                }
                peer.received_ver_ack = true;
            }
            NetworkMessage::Ping(nonce) => {
                let pong_msg = NetworkMessage::Pong(nonce);
                send_network_message(pong_msg, sock_out)
                    .await
                    .expect("Failed to send network message");
                println!("Sent Pong, nonce: {}", nonce);
            }
            NetworkMessage::Addr(addr_msg) => {
                let sock_addrs = addr_msg
                    .into_iter()
                    .filter_map(|(_, addr)| match addr.to_socket_addrs() {
                        Err(e) => {
                            println!("Failed to parse address! Error: {}", e);
                            None
                        }
                        Ok(mut sock_addr) => sock_addr.nth(0),
                    })
                    .collect();
                handle_addr_message(&mut addresses, sock_addrs, incoming_sender.clone())
                    .await
                    .into_iter()
                    .for_each(|(sock_addr, peer_state)| {
                        peers_states_locked.insert(sock_addr, peer_state);
                    })
            }
            NetworkMessage::AddrV2(addr_msg) => {
                let sock_addrs = addr_msg
                    .into_iter()
                    .filter_map(|addr| match addr.to_socket_addrs() {
                        Err(e) => {
                            println!("Failed to parse address! Error: {}", e);
                            None
                        }
                        Ok(mut sock_addr) => sock_addr.nth(0),
                    })
                    .collect();
                handle_addr_message(&mut addresses, sock_addrs, incoming_sender.clone())
                    .await
                    .into_iter()
                    .for_each(|(sock_addr, peer_state)| {
                        peers_states_locked.insert(sock_addr, peer_state);
                    })
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
    let (incoming_send, incoming_rec) = channel::<IncomingMessage>(CHANNEL_BUF_SZ);
    let peers_states: HashMap<_, _> = join_all(
        arg.nodes
            .into_iter()
            .map(|addr| add_receiver(addr, incoming_send.clone()))
            .collect::<Vec<_>>(),
    )
    .await
    .into_iter()
    .filter_map(|f| f)
    .collect();
    let peers_states = Arc::new(Mutex::new(peers_states));
    let peer_states_clone = peers_states.clone();
    let harvested_handle = tokio::spawn(async move { bootstrap_harvester(peer_states_clone) });
    handle_peers(peers_states, incoming_rec, incoming_send).await;
    harvested_handle.await;
    println!("Leaving... bye");
}
