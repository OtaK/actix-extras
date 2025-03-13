//! WebSockets for Actix Web, without actors.
//!
//! For usage, see documentation on [`handle()`].

#![warn(missing_docs)]
#![doc(html_logo_url = "https://actix.rs/img/logo.png")]
#![doc(html_favicon_url = "https://actix.rs/favicon.ico")]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

use std::num::NonZeroUsize;

pub use actix_http::ws::{CloseCode, CloseReason, Item, Message, ProtocolError};
use actix_http::{
    body::{BodyStream, MessageBody},
    ws::handshake,
};
use actix_web::{web, HttpRequest, HttpResponse};
use tokio::sync::mpsc::channel;

mod aggregated;
mod session;
mod stream;

pub use self::{
    aggregated::{AggregatedMessage, AggregatedMessageStream},
    session::{Closed, Session},
    stream::{MessageStream, StreamingBody},
};

/// Begin handling websocket traffic
///
/// ```no_run
/// use std::io;
/// use actix_web::{middleware::Logger, web, App, HttpRequest, HttpServer, Responder};
/// use actix_ws::Message;
/// use futures_util::StreamExt as _;
///
/// async fn ws(req: HttpRequest, body: web::Payload) -> actix_web::Result<impl Responder> {
///     let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;
///
///     actix_web::rt::spawn(async move {
///         while let Some(Ok(msg)) = msg_stream.next().await {
///             match msg {
///                 Message::Ping(bytes) => {
///                     if session.pong(&bytes).await.is_err() {
///                         return;
///                     }
///                 }
///
///                 Message::Text(msg) => println!("Got text: {msg}"),
///                 _ => break,
///             }
///         }
///
///         let _ = session.close(None).await;
///     });
///
///     Ok(response)
/// }
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> io::Result<()> {
///     HttpServer::new(move || {
///         App::new()
///             .route("/ws", web::get().to(ws))
///             .wrap(Logger::default())
///     })
///     .bind(("127.0.0.1", 8080))?
///     .run()
///     .await
/// }
/// ```
pub fn handle(
    req: &HttpRequest,
    body: web::Payload,
) -> Result<(HttpResponse, Session, MessageStream), actix_web::Error> {
    crate::handle_with_config(req, body, Default::default())
}

/// Configuration items for fine-tuning the behavior of actix-ws
#[derive(Debug, Default)]
pub struct WebsocketConfiguration {
    /// The size of the backing channel to account for backpressure
    /// if you wish to have a "rendez-vous" channel, set it to `Some(0)`
    pub backpressure_amount: Option<NonZeroUsize>,
    /// Max size of a Websocket Frame
    pub max_frame_size: Option<NonZeroUsize>,
}

/// This method allows to set more fine-grained parameters than the sane defaults in [`handle`]
///
/// See the documentation on both [`handle`] and [`WebsocketConfiguration`]
pub fn handle_with_config(
    req: &HttpRequest,
    body: web::Payload,
    configuration: WebsocketConfiguration,
) -> Result<(HttpResponse, Session, MessageStream), actix_web::Error> {
    let mut response = handshake(req.head())?;
    let (tx, rx) = channel(
        configuration
            .backpressure_amount
            .map(NonZeroUsize::get)
            .unwrap_or(32),
    );

    let mut stream = MessageStream::new(body.into_inner());
    if let Some(max_size) = configuration.max_frame_size {
        stream = stream.max_frame_size(max_size.get());
    }

    Ok((
        response
            .message_body(BodyStream::new(StreamingBody::new(rx)).boxed())?
            .into(),
        Session::new(tx),
        stream,
    ))
}
