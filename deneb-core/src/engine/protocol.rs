use crossbeam_channel::{bounded as channel, Sender};
use std::marker::PhantomData;

use crate::errors::{DenebResult, EngineError};

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

pub type RequestChannel<H> = Sender<PackagedRequest<H>>;

pub fn call<R, H>(req: R, ch: &RequestChannel<H>) -> DenebResult<R::Reply>
where
    R: Request + 'static,
    H: RequestHandler<R> + 'static,
{
    let (tx, rx) = channel(1);
    let envelope = PackagedRequest {
        inner: Box::new(RequestProxy {
            req,
            tx,
            _hd: PhantomData,
        }),
    };
    ch.send(envelope).map_err(|_| EngineError::Send).unwrap();;

    rx.recv().map_err(|_| EngineError::NoReply)?
}

pub fn cast<R, H>(req: R, ch: &RequestChannel<H>)
where
    R: Request + 'static,
    H: RequestHandler<R> + 'static,
{
    let envelope = PackagedRequest {
        inner: Box::new(CastRequestProxy {
            req,
            _hd: PhantomData,
        }),
    };
    ch.send(envelope).map_err(|_| EngineError::Send).unwrap();
}

struct RequestProxy<R, H>
where
    R: Request,
    H: RequestHandler<R>,
{
    req: R,
    tx: Sender<DenebResult<R::Reply>>,
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
        self.tx.send(reply).map_err(|_| EngineError::Send).unwrap();
    }
}

struct CastRequestProxy<R, H>
where
    R: Request,
    H: RequestHandler<R>,
{
    req: R,
    _hd: PhantomData<fn(&mut H) -> R::Reply>,
}

impl<R, H> HandlerProxy for CastRequestProxy<R, H>
where
    R: Request,
    H: RequestHandler<R>,
{
    type Handler = H;
    fn run_handler(&self, hd: &mut Self::Handler) {
        let _ = hd.handle(&self.req);
    }
}

impl<H> HandlerProxy for PackagedRequest<H> {
    type Handler = H;
    fn run_handler(&self, hd: &mut Self::Handler) {
        self.inner.run_handler(hd);
    }
}
