use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;

use async_stream::try_stream;
use ciel_core::provider::ProviderError;
use ciel_core::provider::request::Request;
use ciel_core::provider::response::ResponseEvent;
use ciel_core::provider::response::ResponseStream;
use ciel_core::provider::response::ToolEvent;
use ciel_core::provider::response::ToolResultEvent;
use ciel_core::tool::Tool;
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::TryStreamExt;
use tokio::pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tower::Layer;
use tower::Service;
use tracing::Level;
use tracing::span;
use uuid::Uuid;

pub struct ToolAdapterLayer<S> {
    pd: PhantomData<S>,
    tools: HashMap<String, Tool>,
}

impl<S> ToolAdapterLayer<S> {
    pub fn with_tools(mut self, tools: impl IntoIterator<Item = Tool>) -> Self {
        for tool in tools.into_iter() {
            self.tools.insert(tool.info().name.to_string(), tool);
        }
        self
    }

    pub fn with_tool(mut self, tool: Tool) -> Self {
        self.tools.insert(tool.info().name.to_string(), tool);
        self
    }
}

impl<S> Clone for ToolAdapterLayer<S> {
    fn clone(&self) -> Self {
        Self {
            pd: self.pd,
            tools: self.tools.clone(),
        }
    }
}
impl<S> Default for ToolAdapterLayer<S> {
    fn default() -> Self {
        Self {
            pd: Default::default(),
            tools: Default::default(),
        }
    }
}

impl<S, ResponseStream> Layer<S> for ToolAdapterLayer<S>
where
    S: Service<Request, Response = ResponseStream, Error = ProviderError>,
    ResponseStream: Stream<Item = io::Result<ResponseEvent>>,
{
    type Service = ToolAdapterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ToolAdapterService {
            inner,
            tools: self.tools.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ToolAdapterService<S> {
    inner: S,
    tools: HashMap<String, Tool>,
}

impl<S, TokenStream> Service<Request> for ToolAdapterService<S>
where
    S: Service<Request, Response = TokenStream, Error = ProviderError>,
    S::Future: Send + 'static,
    TokenStream: Stream<Item = io::Result<ResponseEvent>> + Send + 'static,
{
    type Response = ResponseStream;
    type Error = ProviderError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let tools = self.tools.clone();

        for tool in tools.values() {
            // TODO: this could become a problem when multiple tools have the same name
            req.add_tool(tool.info().clone());
        }
        let inner_future = self.inner.call(req);
        let span = span!(Level::DEBUG, "tool_adapter");

        async move {
            let response = inner_future.await?;
            let response = wrap_stream(tools, response);

            Ok(ResponseStream::new(response).instrumented_with(span))
        }
        .boxed()
    }
}

struct ToolState {
    name: String,
    tool_call_id: String,
    sender: Option<mpsc::Sender<String>>,
    receiver: mpsc::Receiver<String>,
    handle: JoinHandle<()>,
}

fn wrap_stream(
    tools: HashMap<String, Tool>,
    stream: impl Stream<Item = io::Result<ResponseEvent>>,
) -> impl Stream<Item = io::Result<ResponseEvent>> {
    try_stream! {
        pin!(stream);

        let mut states = HashMap::new();

        while let Some(event) = stream.try_next().await? {
            match &event {
                ResponseEvent::Tool(ToolEvent::Start { index, tool_call_id, name, handled }) => {
                    if *handled {
                        continue;
                    }

                    yield ResponseEvent::Tool(ToolEvent::Start {
                        index: *index,
                        tool_call_id: tool_call_id.clone(),
                        name: name.clone(),
                        handled: true,
                    });

                    let Some(tool) = tools.get(name.as_str()) else {
                        let index = Uuid::now_v7();
                        yield ResponseEvent::ToolResult(ToolResultEvent::Start {
                            index,
                            tool_call_id: tool_call_id.clone(),
                            name: name.clone(),
                        });
                        yield ResponseEvent::ToolResult(ToolResultEvent::Chunk {
                            index,
                            delta: "ERROR: unknown tool".to_string(),
                        });
                        yield ResponseEvent::ToolResult(ToolResultEvent::Complete { index });
                        continue;
                    };

                    let (args_tx, args_rx) = mpsc::channel(128);
                    let (res_tx, res_rx) = mpsc::channel(128);

                    let tool = tool.clone();
                    let handle = tokio::spawn(async move {
                        tool.call(args_rx, res_tx).await
                    });

                    let state = ToolState {
                        name: name.clone(),
                        tool_call_id: tool_call_id.clone(),
                        sender: Some(args_tx),
                        receiver: res_rx,
                        handle,
                    };
                    states.insert(*index, state);
                },
                event @ ResponseEvent::Tool(ToolEvent::Chunk { index, delta }) => {
                    yield event.clone();

                    let Some(state) = states.get_mut(index) else {
                        continue;
                    };
                    _ = state.sender
                        .as_mut()
                        .expect("channel cannot yet be removed")
                        .send(delta.clone())
                        .await;
                },
                event @ ResponseEvent::Tool(ToolEvent::Complete { index }) => {
                    yield event.clone();

                    let Some(state) = states.get_mut(index) else {
                        continue;
                    };
                    state.sender = None;
                },
                other => {
                    yield other.clone();
                    continue;
                }
            }
        }

        for (index, mut state) in states {
            yield ResponseEvent::ToolResult(ToolResultEvent::Start {
                index,
                tool_call_id: state.tool_call_id,
                name: state.name,
            });

            while let Some(chunk) = state.receiver.recv().await {
                yield ResponseEvent::ToolResult(ToolResultEvent::Chunk {
                    index,
                    delta: chunk,
                });
            }

            let res = state.handle.await;
            if let Err(err) = res {
                yield ResponseEvent::ToolResult(ToolResultEvent::Chunk {
                    index,
                    delta: format!("\nTOOL FAILED: {err}"),
                });
            }

            yield ResponseEvent::ToolResult(ToolResultEvent::Complete { index });
        }
    }
}
