use std::collections::HashMap;
use std::io;

use actson::JsonEvent;
use actson::JsonParser;
use actson::feeder::JsonFeeder;
use async_stream::try_stream;
use ciel_core::provider::response::ChannelIndex;
use ciel_core::provider::response::MessageEvent;
use ciel_core::provider::response::ResponseEvent;
use futures_core::Stream;
use futures_util::TryStreamExt;
use tokio::pin;
use uuid::Uuid;

pub fn parse_token_stream(
    stream: impl Stream<Item = io::Result<ResponseEvent>>,
) -> impl Stream<Item = io::Result<ResponseEvent>> {
    try_stream! {
        // Maps channel index to a response parser state.
        let mut response_states = HashMap::<ChannelIndex, ResponseParser>::new();

        pin!(stream);
        while let Some(event) = stream.try_next().await? {
            match event {
                ResponseEvent::Reasoning(reasoning) => {
                    yield ResponseEvent::Reasoning(reasoning);
                },
                ResponseEvent::Message(event) => {
                    match event {
                        MessageEvent::Start { index } => {
                            response_states.insert(index, ResponseParser::default());
                        },
                        MessageEvent::Chunk { index, delta } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.push(delta);

                            while let Some(next) = state.next()? {
                                yield next;
                            }
                        },
                        MessageEvent::Complete { index } => {
                            let state = response_states
                                .get_mut(&index)
                                .ok_or_else(|| io::Error::other("unknown channel"))?;

                            state.done();
                            while let Some(next) = state.next()? {
                                yield next;
                            }
                            state.assert_final_state()?;
                            response_states.remove(&index);
                        },
                    }
                },
                other => {
                    yield other;
                    continue;
                }
            }
        }

        for (_index, mut state) in response_states.drain() {
            state.done();
            while let Some(next) = state.next()? {
                yield next;
            }
            state.assert_final_state()?;
        }
    }
}

struct ResponseParser {
    json_parser: JsonParser<ChunkJsonFeeder>,
    state: State,
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

impl ResponseParser {
    fn push(&mut self, input: String) {
        let feeder = &mut self.json_parser.feeder;
        if feeder.has_input() {
            feeder.chunk.push_str(&input);
        } else {
            feeder.pos = 0;
            feeder.chunk = input;
        }
    }

    fn done(&mut self) {
        self.json_parser.feeder.done = true;
    }

    fn assert_final_state(&self) -> io::Result<()> {
        if self.state != State::Done {
            // TODO: convert state to human-readable error message
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("unexpected end of stream (state: {:?})", self.state),
            ));
        }
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
                        let index = Uuid::now_v7();
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

impl Default for ResponseParser {
    fn default() -> Self {
        Self {
            json_parser: JsonParser::new(ChunkJsonFeeder::new(String::new())),
            state: State::ExpectRootStart,
        }
    }
}

/// Custom json feeder, as the builtin PushJsonFeeder has a hard limit on buffer size.
struct ChunkJsonFeeder {
    chunk: String,
    pos: usize,
    done: bool,
}

impl ChunkJsonFeeder {
    pub fn new(chunk: String) -> Self {
        Self {
            chunk,
            pos: 0,
            done: false,
        }
    }
}

impl JsonFeeder for ChunkJsonFeeder {
    fn has_input(&self) -> bool {
        self.pos < self.chunk.len()
    }

    fn is_done(&self) -> bool {
        self.done
    }

    fn next_input(&mut self) -> Option<u8> {
        if !self.has_input() {
            None
        } else {
            let r = Some(self.chunk.as_bytes()[self.pos]);
            self.pos += 1;
            r
        }
    }
}
