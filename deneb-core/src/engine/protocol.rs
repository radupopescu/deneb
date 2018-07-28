use std::marker::PhantomData;
use std::sync::mpsc::{sync_channel, SyncSender};

use errors::DenebResult;

pub trait Request: Send {
    type Reply: Send;
}

pub trait RequestHandler<R>
where
    R: Request,
{
    fn handle(&mut self, request: &R) -> DenebResult<R::Reply>;
}

pub trait HandlerProxy: Send {
    type Handler;
    fn run_handler(&self, handler: &mut Self::Handler);
}

pub struct PackagedRequest<H> {
    inner: Box<HandlerProxy<Handler = H>>,
}

pub type RequestChannel<H> = SyncSender<PackagedRequest<H>>;

pub fn make_request<R, H>(req: R, ch: &RequestChannel<H>) -> DenebResult<R::Reply>
where
    R: Request + 'static,
    H: RequestHandler<R> + 'static,
{
    let (tx, rx) = sync_channel(1);
    let envelope = PackagedRequest {
        inner: Box::new(RequestProxy {
            req,
            tx,
            _hd: PhantomData,
        }),
    };
    if ch.send(envelope).is_err() {
        panic!("Could not send request to engine.");
    }

    rx.recv()?
}

struct RequestProxy<R, H>
where
    R: Request,
    H: RequestHandler<R>,
{
    req: R,
    tx: SyncSender<DenebResult<R::Reply>>,
    _hd: PhantomData<fn(&mut H) -> R::Reply>,
}

impl<R, H> HandlerProxy for RequestProxy<R, H>
where
    R: Request,
    H: RequestHandler<R>,
{
    type Handler = H;
    fn run_handler(&self, hd: &mut Self::Handler) {
        let reply = hd.handle(&self.req);
        if self.tx.send(reply).is_err() {
            panic!("Could not send reply after handling request.");
        }
    }
}

impl<H> HandlerProxy for PackagedRequest<H> {
    type Handler = H;
    fn run_handler(&self, hd: &mut Self::Handler) {
        self.inner.run_handler(hd);
    }
}
