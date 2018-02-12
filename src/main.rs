extern crate hyper;
extern crate futures;
extern crate pretty_env_logger;
#[macro_use] extern crate log;
extern crate tokio_core;

use futures::sync::mpsc::*;
use futures::sink::Sink;
use futures::Future;
use futures::stream::Stream;

use std::cell::RefCell;
use std::rc::Rc;

use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Service, Request, Response};
use hyper::{Method, Chunk, Body};

#[derive(Debug, Default)]
pub struct Broker {
    subscriptions: Vec<Subscription>,
}

impl Broker {
    fn subscribe(&mut self, subscription: Subscription) {
        self.subscriptions.push(subscription);
    }

    fn publish(&mut self, channel: &str, payload: &str) {
        for subscription in self.subscriptions.iter_mut() {
            if subscription.channel == channel {
                subscription.sender.start_send(Ok(Chunk::from(format!("{}\n", payload))));
                subscription.sender.poll_complete();
            }
        }
    }
}

#[derive(Debug)]
pub struct Subscription {
    channel: String,
    sender: Sender<Result<Chunk, hyper::Error>>,
}

impl Subscription {
    fn new(channel: &str, sender: Sender<Result<Chunk, hyper::Error>>) -> Subscription {
        Subscription { channel: channel.to_string(), sender: sender }
    }
}

#[derive(Debug)]
struct SyncedBroker {
    inner: RefCell<Broker>
}

impl SyncedBroker {
    fn new(broker: Broker) -> SyncedBroker {
        SyncedBroker { inner: RefCell::new(broker) }
    }

    fn subscribe(&self, subscription: Subscription) {
        let mut guard = self.inner.borrow_mut();
        guard.subscribe(subscription);
    }

    fn publish(&self, channel: &str, payload: &str) {
        let mut guard = self.inner.borrow_mut();
        guard.publish(channel, payload);
    }
}

#[derive(Debug)]
struct MessagingServer {
    broker: Rc<SyncedBroker>
}

impl MessagingServer {
    fn new(broker: Rc<SyncedBroker>) -> MessagingServer {
        MessagingServer { broker }
    }
}

static OK: &'static str = "{ \"ok\": true }";

impl Service for MessagingServer {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, _, _headers, body) = req.deconstruct();
    
        match (method, uri.path()) {
            (Method::Get, "/") => {
                Box::new(futures::future::ok(
                    Response::new()
                        .with_header(ContentLength(OK.len() as u64))
                        .with_header(ContentType::plaintext())
                        .with_body(OK)
                ))
            },
            (Method::Head, "/") => {
                Box::new(futures::future::ok(
                    Response::new()
                ))
            },
            (Method::Post, path) => {
                let mut uri = path.split("/");
                uri.next();
                let next = uri.next();

                match next {
                    Some("channel") => {
                        let channel = uri.next().unwrap().to_string();
                        
                        let broker = self.broker.clone();

                        let concat = body.concat2().and_then(move |body_chunk| {
                            let payload = std::str::from_utf8(&body_chunk).unwrap().to_string();

                            broker.publish(&channel, &payload);

                            futures::future::ok(Response::new()
                                .with_header(ContentLength(OK.len() as u64))
                                .with_header(ContentType::plaintext())
                                .with_body(OK)
                            )
                        });

                        Box::new(concat)
                    }
                    Some(_) | None => {
                        unimplemented!()
                    }
                }
            },
            (Method::Get, path) => {
                let mut uri = path.split("/");
                uri.next();
                let next = uri.next();

                match next {
                    Some("channels") => {
                        let channels = uri.next().unwrap().split(",").collect::<Vec<_>>();

                        let channel = channels[0];

                        let (sender, body) = Body::pair();

                        let subscription = Subscription::new(channel, sender);
                        self.broker.subscribe(subscription);

                        Box::new(futures::future::ok(
                            Response::new()
                                .with_body(body)
                        ))
                    }
                    Some(_) | None => {
                        unimplemented!()
                    }
                }
            },
            _ => {
                unimplemented!();
            }
        }
    }
}

fn main() {
    pretty_env_logger::init();
    let addr = "127.0.0.1:3000".parse().unwrap();
    let broker = Rc::new(SyncedBroker::new(Broker::default()));

    let server = Http::new().bind(&addr, move || Ok(MessagingServer::new(broker.clone()))).unwrap();

    info!("Listening on http://{} with 1 thread.", server.local_addr().unwrap());
    server.run().unwrap();
}

#[cfg(test)]
mod test {
    extern crate rand;
    use test::rand::Rng;

    use std::thread;
    use std::net::SocketAddr;
    use futures::sync::oneshot;
    use futures::Future;
    use hyper::server::{Http};
    use super::{MessagingServer, Broker, SyncedBroker};
    use pretty_env_logger;
    use hyper;
    use tokio_core::reactor::Core;
    use std::str;
    use futures::Stream;
    use futures::future::join_all;

    use std::time::Duration;
    use std::sync::{Mutex};

    #[test]
    fn test_heartbeat_get() {
        let mut core = Core::new().expect("core creation error");
        let handle = core.handle();

        let server = serve();

        let client = hyper::Client::new(&handle);
        let test = client.get(format!("http://{}/", server.addr()).parse().expect("uri parse error"))
            .then(|result| {
                let res = result.expect("client http error");
                res.body().concat2()
            })
            .and_then(|body| {
                let body_str = str::from_utf8(body.as_ref()).expect("client body decode error");
                assert_eq!(body_str, "{ \"ok\": true }");
                Ok(())
            });
        core.run(test).expect("client body read error");
    }

    #[test]
    fn test_heartbeat_head() {
        let mut core = Core::new().expect("core creation error");
        let handle = core.handle();

        let server = serve();

        let client = hyper::Client::new(&handle);
        
        let uri = format!("http://{}/", server.addr()).parse().expect("uri parse error");
        let request = hyper::Request::new(hyper::Method::Head, uri);

        let test = client.request(request)
            .then(|result| {
                let res = result.expect("client http error");
                res.body().concat2()
            })
            .and_then(|body| {
                let body_str = str::from_utf8(body.as_ref()).expect("client body decode error");
                Ok(())
            });
        core.run(test).expect("client body read error");
    }

    #[test]
    fn test_post() {
        let mut core = Core::new().expect("core creation error");
        let handle = core.handle();

        let server = serve();

        let client = hyper::Client::new(&handle);
        
        let uri = format!("http://{}/channel/test", server.addr()).parse().expect("uri parse error");
        let mut request = hyper::Request::new(hyper::Method::Post, uri);
        request.set_body("test");

        let test = client.request(request)
            .then(|result| {
                let res = result.expect("client http error");
                res.body().concat2()
            })
            .and_then(|body| {
                let body_str = str::from_utf8(body.as_ref()).expect("client body decode error");
                assert_eq!(body_str, "{ \"ok\": true }");
                Ok(())
            });
        core.run(test).expect("client body read error");
    }

    #[test]
    fn test_subscribe() {
        let mut core = Core::new().expect("core creation error");
        let handle = core.handle();

        let server = serve();

        let client = hyper::Client::new(&handle);
        
        let uri = format!("http://{}/channels/test", server.addr()).parse().expect("uri parse error");
        let request = hyper::Request::new(hyper::Method::Get, uri);

        let uri = format!("http://{}/channel/test", server.addr()).parse().expect("uri parse error");
        let mut publish_request = hyper::Request::new(hyper::Method::Post, uri);
        publish_request.set_body("test");

        let test = client.request(request)
            .then(|result| {
                let res = result.expect("client http error");
                res.body().into_future()
            })
            .map_err(|(err, _)|{
                err
            })
            .and_then(|(chunk, _)| {
                let string = chunk.unwrap();
                let body_str = str::from_utf8(&string).expect("client body decode error");
                assert_eq!(body_str, "test\n");
                Ok(())
            });
        let publish = client.request(publish_request).and_then(|_| Ok(()));

        let futures: Vec<Box<Future<Item=(), Error=hyper::Error>>> = vec![Box::new(test), Box::new(publish)];
        let joined = join_all(futures);
        core.run(joined).expect("client body read error");
    }
    
    struct Serve {
        addr: SocketAddr,
        shutdown_signal: Option<oneshot::Sender<()>>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl Drop for Serve {
        fn drop(&mut self) {
            drop(self.shutdown_signal.take());
            self.thread.take().unwrap().join().unwrap();
        }
    }

    impl Serve {
        fn addr(&self) -> &SocketAddr {
            &self.addr
        }
    }

    fn serve() -> Serve {
        let _ = pretty_env_logger::init();
        let port = rand::thread_rng().gen_range(3000, 4000);
        
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let thread_name = format!("test-server");
        let thread = thread::Builder::new().name(thread_name).spawn(move || {
            let broker = Arc::new(SyncedBroker::new(Broker::default()));

            let mut srv = Http::new()
                .bind(&addr, move || Ok(MessagingServer::new(broker.clone()))).unwrap();
            srv.shutdown_timeout(Duration::from_millis(10));
            
            srv.run_until(shutdown_rx.then(|_| Ok(()))).unwrap();
        }).unwrap();

        Serve {
            addr: addr,
            shutdown_signal: Some(shutdown_tx),
            thread: Some(thread),
        }
    }
}
