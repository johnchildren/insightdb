use std::io;
use std::str;
use bytes::BytesMut;
use tokio_io::codec::{Framed, Encoder, Decoder};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::Service;
use futures::{future, Future};

pub struct QueryStrCodec;

impl Decoder for QueryStrCodec {
    type Item = String;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<String>> {
        match buf.iter().position(|&b| b == b'\n') {
            Some(i) => decode_buf(buf, i),
            None => Ok(None),
        }
    }
}

fn decode_buf(buf: &mut BytesMut, i: usize) -> io::Result<Option<String>> {
    let line = buf.split_to(i);
    // Also remove the '\n'
    buf.split_to(1);
    // Turn this data into a UTF string and return it in a Frame.
    match str::from_utf8(&line) {
        Ok(s) => Ok(Some(s.to_string())),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "invalid UTF-8")),
    }
}

impl Encoder for QueryStrCodec {
    type Item = String;
    type Error = io::Error;
    
    fn encode(&mut self, msg: String, buf: &mut BytesMut) -> io::Result<()> {
        buf.extend(msg.as_bytes());
        buf.extend(b"\n");
        Ok(())
    }
}

pub struct QueryProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for QueryProto {
    /// For this protocol style, `Request` matches the `Item` type of the codec's `Decoder`
    type Request = String;

    /// For this protocol style, `Response` matches the `Item` type of the codec's `Encoder`
    type Response = String;

    /// A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, QueryStrCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(QueryStrCodec))
    }
}

pub struct Echo;

impl Service for Echo {
    // These types must match the corresponding protocol types:
    type Request = String;
    type Response = String;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item = Self::Response, Error =  Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        // In this case, the response is immediate.
        Box::new(future::ok(req))
    }
}

pub fn start_server() {
    // Specify the localhost address
    let addr = "0.0.0.0:12345".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(QueryProto, addr);

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    server.serve(|| Ok(Echo));    
}