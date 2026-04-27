use std::{io::ErrorKind, net::SocketAddr, sync::Arc};

use ahash::{HashMap, HashMapExt};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use socket2::{Domain, Protocol, Socket as Socket2, Type};
use tokio::{
    net::UdpSocket as TokioUdpSocket,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};

use crate::server::transport::{Server, ServerOptions, Socket};

/// Bind one or more `TokioUdpSocket`s to the same listener address.
///
/// When `listener_count > 1` (and the platform supports it), each socket sets
/// `SO_REUSEADDR` and `SO_REUSEPORT` so the kernel hashes incoming flows
/// across them, enabling parallel `recv_from` loops on multiple cores.
fn bind_udp_listeners(
    listen: SocketAddr,
    v6_only: bool,
    send_buffer_size: usize,
    recv_buffer_size: usize,
    listener_count: usize,
) -> Result<Vec<TokioUdpSocket>> {
    let count = listener_count.max(1);
    let mut sockets = Vec::with_capacity(count);
    for idx in 0..count {
        let domain = if listen.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
        let socket = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        if listen.is_ipv6() {
            socket.set_only_v6(v6_only)?;
        }
        socket.set_nonblocking(true)?;
        if count > 1 {
            socket.set_reuse_address(true)?;
            #[cfg(unix)]
            socket.set_reuse_port(true)?;
        }
        apply_udp_buffer_sizes(&socket, listen, send_buffer_size, recv_buffer_size);
        if let Err(e) = socket.bind(&listen.into()) {
            log::error!("udp listener {listen}: bind socket #{idx} failed: {e}");
            return Err(e.into());
        }
        let std_socket: std::net::UdpSocket = socket.into();
        sockets.push(TokioUdpSocket::from_std(std_socket)?);
    }
    Ok(sockets)
}

fn resolve_listener_count(requested: usize) -> usize {
    // 0 = auto. SO_REUSEPORT is only useful on Unix; on Windows we coerce
    // to a single listener regardless of the request.
    #[cfg(not(unix))]
    {
        let _ = requested;
        return 1;
    }
    #[cfg(unix)]
    {
        if requested >= 1 {
            return requested;
        }
        let auto = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        // Cap at 8 -- diminishing returns past that and we don't want to
        // monopolise the tokio runtime threads on huge boxes.
        auto.clamp(1, 8)
    }
}

fn apply_udp_buffer_sizes(
    socket: &Socket2,
    listen: SocketAddr,
    send_buffer_size: usize,
    recv_buffer_size: usize,
) {
    if send_buffer_size > 0 {
        if let Err(e) = socket.set_send_buffer_size(send_buffer_size) {
            log::warn!(
                "udp listener {listen}: SO_SNDBUF set failed: requested={send_buffer_size} error={e}"
            );
        }
        if let Ok(actual) = socket.send_buffer_size() {
            if actual < send_buffer_size / 2 {
                log::warn!(
                    "udp listener {listen}: SO_SNDBUF clamped: requested={send_buffer_size} actual={actual} (raise net.core.wmem_max on Linux or kern.ipc.maxsockbuf on macOS)"
                );
            }
        }
    }
    if recv_buffer_size > 0 {
        if let Err(e) = socket.set_recv_buffer_size(recv_buffer_size) {
            log::warn!(
                "udp listener {listen}: SO_RCVBUF set failed: requested={recv_buffer_size} error={e}"
            );
        }
        if let Ok(actual) = socket.recv_buffer_size() {
            if actual < recv_buffer_size / 2 {
                log::warn!(
                    "udp listener {listen}: SO_RCVBUF clamped: requested={recv_buffer_size} actual={actual} (raise net.core.rmem_max on Linux or kern.ipc.maxsockbuf on macOS)"
                );
            }
        }
    }
}

pub struct UdpSocket {
    close_signal_sender: UnboundedSender<SocketAddr>,
    bytes_receiver: Receiver<Bytes>,
    socket: Arc<TokioUdpSocket>,
    addr: SocketAddr,
}

impl Socket for UdpSocket {
    async fn read(&mut self) -> Option<Bytes> {
        self.bytes_receiver.recv().await
    }

    async fn write(&mut self, buffer: &[u8]) -> Result<()> {
        if let Err(e) = self.socket.send_to(buffer, self.addr).await {
            if e.kind() != ErrorKind::ConnectionReset {
                return Err(e.into());
            }
        }

        Ok(())
    }

    async fn close(&mut self) {
        self.bytes_receiver.close();

        let _ = self.close_signal_sender.send(self.addr);
    }
}

pub struct UdpServer {
    receiver: UnboundedReceiver<(UdpSocket, SocketAddr)>,
    socket: Arc<TokioUdpSocket>,
}

impl Server for UdpServer {
    type Socket = UdpSocket;

    async fn bind(options: &ServerOptions) -> Result<Self> {
        let listener_count = resolve_listener_count(options.listener_count);
        let sockets: Vec<Arc<TokioUdpSocket>> = bind_udp_listeners(
            options.listen,
            options.v6_only,
            options.send_buffer_size,
            options.recv_buffer_size,
            listener_count,
        )?
        .into_iter()
        .map(Arc::new)
        .collect();

        if sockets.len() > 1 {
            log::info!(
                "udp listener {}: bound {} sockets via SO_REUSEPORT",
                options.listen,
                sockets.len()
            );
        }

        let send_socket = sockets[0].clone();
        let (socket_sender, socket_receiver) = unbounded_channel::<(UdpSocket, SocketAddr)>();
        let (close_signal_sender, mut close_signal_receiver) = unbounded_channel::<SocketAddr>();

        // Shared demux state: source address -> bounded inbound channel for that
        // peer's per-source task. Read-mostly under steady state -- writes only
        // on first packet from a new peer or on session close.
        let demux_map: Arc<RwLock<HashMap<SocketAddr, Sender<Bytes>>>> =
            Arc::new(RwLock::new(HashMap::with_capacity(1024)));

        // Cleanup task: drains close-signal events and removes the entry from
        // the shared map. Runs independently of recv loops so a single recv
        // task stalling on its socket never blocks cleanup.
        {
            let demux_map = demux_map.clone();
            tokio::spawn(async move {
                while let Some(addr) = close_signal_receiver.recv().await {
                    demux_map.write().remove(&addr);
                }
            });
        }

        // Spawn N recv loops, one per kernel socket. They share the demux map
        // so any flow's first packet (received on whichever socket the kernel
        // assigned) registers the peer once. All subsequent packets from the
        // same source land on the same socket (kernel hashes by 4-tuple) so
        // there's no contention on hot-path lookups -- they're read-only on
        // the RwLock.
        let demuxer_capacity = options.demuxer_capacity;
        let mtu = options.mtu;

        for socket in sockets.into_iter() {
            let demux_map = demux_map.clone();
            let socket_sender = socket_sender.clone();
            let close_signal_sender = close_signal_sender.clone();
            let send_socket = send_socket.clone();

            tokio::spawn(async move {
                // Reusable receive buffer. After each packet we split() it
                // into a `Bytes` (zero-copy, just an Arc<...> bump) and
                // refill the BytesMut for the next iteration. No per-packet
                // heap allocation on the hot path beyond the initial buffer.
                let mut buffer = BytesMut::with_capacity(mtu);
                buffer.resize(mtu, 0);

                loop {
                    let (size, addr) = match socket.recv_from(&mut buffer).await {
                        Ok(it) => it,
                        // Note: An error will also be reported when the remote host is
                        // shut down, which is not processed yet, but a
                        // warning will be issued.
                        Err(e) => {
                            if e.kind() != ErrorKind::ConnectionReset {
                                log::error!("udp server recv_from error={e}");

                                break;
                            } else {
                                continue;
                            }
                        }
                    };

                    if size < 4 {
                        continue;
                    }

                    // Zero-copy hand-off: split() takes ownership of the filled
                    // prefix of the buffer (returning a new BytesMut), and
                    // freeze() converts that into a `Bytes` (cheap Arc) we can
                    // forward into the bounded per-peer channel without an
                    // extra memcpy.
                    let mut packet = buffer.split_to(size);
                    let bytes = packet.split().freeze();
                    buffer.reserve(mtu);
                    buffer.resize(mtu, 0);

                    {
                        let map = demux_map.read();
                        if let Some(stream) = map.get(&addr) {
                            let _ = stream.try_send(bytes);
                            continue;
                        }
                    }

                    // First packet from a new peer: take the write lock and
                    // double-check (another recv loop may have raced us and
                    // already inserted), then register the new per-peer task.
                    let (tx, bytes_receiver) = channel::<Bytes>(demuxer_capacity);
                    if tx.try_send(bytes).is_err() {
                        continue;
                    }

                    let inserted = {
                        let mut map = demux_map.write();
                        if map.contains_key(&addr) {
                            // Race: another recv loop already registered this peer
                            // between our read-lock check and write-lock acquire.
                            // Drop our channel; the existing one will get the next
                            // packet for this addr.
                            false
                        } else {
                            map.insert(addr, tx);
                            true
                        }
                    };

                    if !inserted {
                        continue;
                    }

                    if socket_sender
                        .send((
                            UdpSocket {
                                close_signal_sender: close_signal_sender.clone(),
                                socket: send_socket.clone(),
                                bytes_receiver,
                                addr,
                            },
                            addr,
                        ))
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }

        Ok(Self {
            receiver: socket_receiver,
            socket: send_socket,
        })
    }

    async fn accept(&mut self) -> Option<(UdpSocket, SocketAddr)> {
        self.receiver.recv().await
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
}
