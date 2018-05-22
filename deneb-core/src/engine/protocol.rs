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

pub trait Actor {}

pub struct PackagedRequest<A>
{
    inner: Box<HandlerProxy<Handler = A>>,
}

pub type RequestChannel<A> = SyncSender<PackagedRequest<A>>;

pub fn make_request<R, A>(req: R, ch: &RequestChannel<A>) -> DenebResult<R::Reply>
where
    R: Request + 'static,
    A: RequestHandler<R> + 'static,
{
    let (tx, rx) = sync_channel(1);
    let envelope = PackagedRequest {
        inner: Box::new(RequestProxy {
            req,
            tx,
            _hd: PhantomData,
        }),
    };
    if let Err(_) = ch.send(envelope) {
        panic!("Could not send request to engine.");
    }

    let rep = rx.recv()?;
    rep
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
        if let Err(_) = self.tx.send(reply) {
            panic!("Could not send reply after handling request.");
        }
    }
}

impl<A> HandlerProxy for PackagedRequest<A>
where
    A: Actor,
{
    type Handler = A;
    fn run_handler(&self, hd: &mut Self::Handler) {
        self.inner.run_handler(hd);
    }
}
