use std::collections::HashMap;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use actson::JsonEvent;
use actson::JsonParser;
use actson::feeder::PushJsonFeeder;
use async_stream::try_stream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use tokio::pin;

use crate::stream::ChannelIndex;
use crate::stream::MessageEvent;
use crate::stream::ResponseEvent;
use crate::stream::ToolEvent;

pub fn parse_token_stream(
    tools: bool,
    stream: impl Stream<Item = io::Result<ResponseEvent>>,
) -> impl Stream<Item = io::Result<ResponseEvent>> {
    try_stream! {
        // Maps old(!!) channel index to a response parser state.
        let mut response_states = HashMap::<ChannelIndex, ResponseParser>::new();
        // Incremental counter for new indices. Using an atomic value was necessary since something
        // like refcell does not work here (not Sync).
        let next_channel_id = AtomicUsize::new(0);

        pin!(stream);
        while let Some(event) = stream.try_next().await? {
            match event {
                ResponseEvent::Reasoning(reasoning) => {
                    yield ResponseEvent::Reasoning(reasoning);
                },
                ResponseEvent::Message(event) => {
                    if tools {
                        continue;
                    }

                    match event {
                        MessageEvent::Start { index } => {
                            response_states.insert(index, ResponseParser::new(&next_channel_id));
                        },
                        MessageEvent::Chunk { index, delta } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.push(&delta);
                            while let Some(next) = state.next()? {
                                yield next;
                            }
                        },
                        MessageEvent::Complete { index } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.done()?;
                            while let Some(next) = state.next()? {
                                yield next;
                            }
                            response_states.remove(&index);
                        },
                    }
                },
                ResponseEvent::Tool(event) => {
                    if !tools {
                        continue;
                    }

                    match event {
                        ToolEvent::Start { index, .. } => {
                            response_states.insert(index, ResponseParser::new(&next_channel_id));
                        },
                        ToolEvent::Chunk { index, delta } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.push(&delta);
                            while let Some(next) = state.next()? {
                                yield next;
                            }
                        },
                        ToolEvent::Complete { index } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.done()?;
                            while let Some(next) = state.next()? {
                                yield next;
                            }
                            response_states.remove(&index);
                        },
                    }
                },
                ResponseEvent::ToolResult(_event) => {
                    // TODO: support passing along tool output from this layer
                    continue;
                },
            }
        }

        for (_index, mut state) in response_states.drain() {
            state.done()?;
            while let Some(next) = state.next()? {
                yield next;
            }
        }
    }
}

struct ResponseParser<'a> {
    json_parser: JsonParser<PushJsonFeeder>,
    state: State,
    index_gen: &'a AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    ExpectRootStart,
    InRootObject,
    ExpectRootValue(RootField),

    InMessagesArray,

    InMessageObject(ChannelIndex),
    ExpectMessageValue(ChannelIndex, MessageField),

    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootField {
    Reasoning,
    ShouldRespond,
    Messages,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageField {
    Reasoning,
    MessageContent,
}

impl<'a> ResponseParser<'a> {
    fn new(index_gen: &'a AtomicUsize) -> Self {
        Self {
            json_parser: JsonParser::new(PushJsonFeeder::new()),
            state: State::ExpectRootStart,
            index_gen,
        }
    }

    fn push(&mut self, input: &str) {
        self.json_parser.feeder.push_bytes(input.as_bytes());
    }

    fn done(&mut self) -> io::Result<()> {
        if self.state != State::Done {
            // TODO: convert state to human-readable error message
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("unexpected end of stream (state: {:?})", self.state),
            ));
        }
        self.json_parser.feeder.done();
        Ok(())
    }

    fn next(&mut self) -> io::Result<Option<ResponseEvent>> {
        fn mk_err(s: impl Into<String>) -> io::Result<Option<ResponseEvent>> {
            Err(io::Error::new(io::ErrorKind::InvalidData, s.into()))
        }

        while let Some(next) = self
            .json_parser
            .next_event()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        {
            match next {
                JsonEvent::NeedMoreInput => {
                    return Ok(None);
                }
                JsonEvent::StartObject => match self.state {
                    State::ExpectRootStart => {
                        self.state = State::InRootObject;
                    }
                    State::InMessagesArray => {
                        let index = self.index_gen.fetch_add(1, Ordering::Relaxed);
                        self.state = State::InMessageObject(index);
                        return Ok(Some(ResponseEvent::Message(MessageEvent::Start { index })));
                    }
                    _ => {
                        return mk_err("unexpected object start");
                    }
                },
                JsonEvent::EndObject => match self.state {
                    State::Done => {}
                    State::InMessageObject(index) => {
                        self.state = State::InMessagesArray;
                        return Ok(Some(ResponseEvent::Message(MessageEvent::Complete {
                            index,
                        })));
                    }
                    State::InRootObject => {
                        self.state = State::Done;
                    }
                    _ => return mk_err("unexpected object end"),
                },
                JsonEvent::StartArray => match self.state {
                    State::ExpectRootValue(RootField::Messages) => {
                        self.state = State::InMessagesArray;
                    }
                    _ => return mk_err("unexpected array start"),
                },
                JsonEvent::EndArray => match self.state {
                    State::InMessagesArray => {
                        self.state = State::InRootObject;
                    }
                    _ => return mk_err("unexpected array start"),
                },
                JsonEvent::FieldName => match self.state {
                    State::InRootObject => match self.json_parser.current_str() {
                        Ok("_reasoning") => {
                            self.state = State::ExpectRootValue(RootField::Reasoning);
                        }
                        Ok("_should_respond") => {
                            self.state = State::ExpectRootValue(RootField::ShouldRespond);
                        }
                        Ok("messages") => {
                            self.state = State::ExpectRootValue(RootField::Messages);
                        }
                        Ok(other) => return mk_err(format!("unexpected field: {other}")),
                        Err(err) => return mk_err(format!("could not get field name: {err}")),
                    },
                    State::InMessageObject(index) => match self.json_parser.current_str() {
                        Ok("_reasoning") => {
                            self.state = State::ExpectMessageValue(index, MessageField::Reasoning);
                        }
                        Ok("message") => {
                            self.state =
                                State::ExpectMessageValue(index, MessageField::MessageContent);
                        }
                        Ok(other) => return mk_err(format!("unexpected field: {other}")),
                        Err(err) => return mk_err(format!("could not get field name: {err}")),
                    },
                    _ => return mk_err("unexpected field name"),
                },
                JsonEvent::ValueString => {
                    let value = match self.json_parser.current_str() {
                        Ok(v) => v,
                        Err(err) => return mk_err(format!("could not get string value: {err}")),
                    };

                    match self.state {
                        State::ExpectRootValue(RootField::Reasoning) => {
                            self.state = State::InRootObject;
                            return Ok(Some(ResponseEvent::Reasoning(value.to_string())));
                        }
                        State::ExpectMessageValue(index, MessageField::Reasoning) => {
                            self.state = State::InMessageObject(index);
                            return Ok(Some(ResponseEvent::Reasoning(value.to_string())));
                        }
                        State::ExpectMessageValue(index, MessageField::MessageContent) => {
                            self.state = State::InMessageObject(index);
                            return Ok(Some(ResponseEvent::Message(MessageEvent::Chunk {
                                index,
                                delta: value.to_string(),
                            })));
                        }
                        _ => return mk_err("unexpected string value"),
                    }
                }
                JsonEvent::ValueTrue => match self.state {
                    State::ExpectRootValue(RootField::ShouldRespond) => {
                        self.state = State::InRootObject;
                    }
                    _ => return mk_err("unexpected true value"),
                },
                JsonEvent::ValueFalse => match self.state {
                    State::ExpectRootValue(RootField::ShouldRespond) => {
                        // TODO: somehow signal that the stream can be aborted
                        self.state = State::InRootObject;
                    }
                    _ => return mk_err("unexpected false value"),
                },
                JsonEvent::ValueFloat => return mk_err("unexpected float value"),
                JsonEvent::ValueInt => return mk_err("unexpected float value"),
                JsonEvent::ValueNull => return mk_err("unexpected float value"),
            }
        }
        Ok(None)
    }
}
