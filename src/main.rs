use bitcoin::consensus::{encode, Decodable};
use bitcoin::network::constants::{ServiceFlags, PROTOCOL_VERSION};
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::Address;
use bitcoin::Network;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

const BUFSZ: usize = 4096;
const CHANNEL_BUF_SZ: usize = 10;
#[derive(Debug, StructOpt)]
#[structopt(name = "mystruct", about = "about string")]
struct ProgramArgs {
    #[structopt(short, long)]
    nodes: Vec<SocketAddr>,
}

#[derive(Debug)]
struct IncomingMessage {
    id: usize,
    msg: NetworkMessage,
}
struct ReceiverPeerState {
    id: usize,
    stream_in: StreamReader<OwnedReadHalf, BUFSZ>,
    sender: Sender<IncomingMessage>,
    peer_addr: SocketAddr,
}

struct PeerState {
    received_ver: bool,
    received_ver_ack: bool,
    receiver_handle: JoinHandle<()>,
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

async fn connect_to_peers(addrs: &[SocketAddr]) -> (Vec<OwnedReadHalf>, Vec<OwnedWriteHalf>) {
    let mut incoming_sockets: Vec<_> = Vec::with_capacity(addrs.len());
    let mut outgoing_sockets: Vec<_> = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let sock = TcpStream::connect(addr).await;
        if sock.is_err() {
            continue;
        }
        let (sock_read, mut sock_write) = sock.unwrap().into_split();
        if send_ver_to_peer(&mut sock_write).await.is_ok() {
            incoming_sockets.push(sock_read);
            outgoing_sockets.push(sock_write);
        }
    }
    (incoming_sockets, outgoing_sockets)
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
                        id: state.id,
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

async fn send_network_message(payload: NetworkMessage, sock: &mut OwnedWriteHalf) -> () {
    let raw_msg = RawNetworkMessage {
        magic: Network::Bitcoin.magic(),
        payload,
    };
    let bytes = encode::serialize(&raw_msg);
    if let Err(error) = sock.write_all(&bytes).await {
        dbg!(format!("Error in sender thread: {:?}", error));
        return;
    }
}
async fn bootstrap_peers(
    sockets: Vec<OwnedReadHalf>,
    incoming_sender: Sender<IncomingMessage>,
) -> Vec<PeerState> {
    sockets
        .into_iter()
        .enumerate()
        .map(|(id, sock_read)| {
            // Create receiver
            let peer_addr = sock_read
                .peer_addr()
                .expect("There should be an address in the socket");
            let receiver_state = ReceiverPeerState {
                id,
                stream_in: StreamReader::new(sock_read),
                sender: incoming_sender.clone(),
                peer_addr,
            };
            let receiver_handle = tokio::spawn(async move {
                receiver_peer_thread(receiver_state).await;
            });

            PeerState {
                received_ver: false,
                received_ver_ack: false,
                receiver_handle,
            }
        })
        .collect()
}

async fn handle_peers(
    mut peers_states: Vec<PeerState>,
    mut incoming_rec: Receiver<IncomingMessage>,
    mut outgoing_sockets: Vec<OwnedWriteHalf>,
) {
    loop {
        let msg = match incoming_rec.recv().await {
            None => return,
            Some(msg) => msg,
        };
        let peer = &mut peers_states[msg.id];
        let sock_out = &mut outgoing_sockets[msg.id];
        let network_msg = msg.msg;
        match network_msg {
            NetworkMessage::Version(ver_msg) => {
                if peer.received_ver {
                    dbg!(format!("Already received verack from peer {}", msg.id));
                    continue;
                }
                peer.received_ver = true;
                dbg!("Received Ver Message!");
                dbg!(format!("{:?}", ver_msg));
                let verack = NetworkMessage::Verack;
                send_network_message(verack, sock_out).await;
            }
            NetworkMessage::Verack => {
                if peer.received_ver_ack {
                    dbg!(format!("Already received verack from peer {}", msg.id));
                    continue;
                }
                peer.received_ver_ack = true;
            }
            n => {
                dbg!(format!("Received some unsupported message: {}", n.cmd()));
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let arg = ProgramArgs::from_args();
    let (incoming_sockets, outgoing_sockets) = connect_to_peers(&arg.nodes).await;
    let (incoming_send, incoming_rec) = channel::<IncomingMessage>(CHANNEL_BUF_SZ);
    let peers_states = bootstrap_peers(incoming_sockets, incoming_send).await;
    handle_peers(peers_states, incoming_rec, outgoing_sockets).await;
    println!("Leaving... bye");
}
