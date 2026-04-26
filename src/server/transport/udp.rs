use std::{io::ErrorKind, net::SocketAddr, sync::Arc};

use ahash::{HashMap, HashMapExt};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use socket2::{Domain, Protocol, Socket as Socket2, Type};
use tokio::{
    net::UdpSocket as TokioUdpSocket,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};

use crate::server::transport::{Server, ServerOptions, Socket};

fn bind_udp(listen: SocketAddr, v6_only: bool) -> Result<TokioUdpSocket> {
    let domain = if listen.is_ipv6() { Domain::IPV6 } else { Domain::IPV4 };
    let socket = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if listen.is_ipv6() {
        socket.set_only_v6(v6_only)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&listen.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    Ok(TokioUdpSocket::from_std(std_socket)?)
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
        let socket = Arc::new(bind_udp(options.listen, options.v6_only)?);
        let (socket_sender, socket_receiver) = unbounded_channel::<(UdpSocket, SocketAddr)>();
        let (close_signal_sender, mut close_signal_receiver) = unbounded_channel::<SocketAddr>();

        {
            let socket = socket.clone();

            let mut buffer = BytesMut::zeroed(options.mtu);
            let demuxer_capacity = options.demuxer_capacity;

            tokio::spawn(async move {
                let mut sockets = HashMap::<SocketAddr, Sender<Bytes>>::with_capacity(1024);

                loop {
                    tokio::select! {
                        ret = socket.recv_from(&mut buffer) => {
                            let (size, addr) = match ret {
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

                            if let Some(stream) = sockets.get(&addr) {
                                let _ = stream.try_send(Bytes::copy_from_slice(&buffer[..size]));
                            } else {
                                let (tx, bytes_receiver) = channel::<Bytes>(demuxer_capacity);

                                // Send the first packet to the new socket
                                if tx.try_send(Bytes::copy_from_slice(&buffer[..size])).is_err() {
                                    continue;
                                }

                                sockets.insert(addr, tx);

                                if socket_sender
                                    .send((
                                        UdpSocket {
                                            close_signal_sender: close_signal_sender.clone(),
                                            socket: socket.clone(),
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
                        }
                        Some(addr) = close_signal_receiver.recv() => {
                            let _ = sockets.remove(&addr);
                        }
                        else => {
                            break;
                        }
                    }
                }
            });
        }

        Ok(Self {
            receiver: socket_receiver,
            socket,
        })
    }

    async fn accept(&mut self) -> Option<(UdpSocket, SocketAddr)> {
        self.receiver.recv().await
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
}
