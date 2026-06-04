use std::io;

use actson::JsonEvent;
use actson::JsonParser;
use actson::feeder::PushJsonFeeder;
use async_stream::try_stream;
use futures_core::Stream;
use futures_util::TryStreamExt;
use tokio::pin;

use crate::adapter::chat::AssistantEvent;
use crate::provider::Token;
use crate::provider::ToolToken;

pub fn parse_token_stream(
    tools: bool,
    stream: impl Stream<Item = io::Result<Token>>,
) -> impl Stream<Item = io::Result<AssistantEvent>> {
    try_stream! {
        let mut state = ChatState::None;

        pin!(stream);
        while let Some(token) = stream.try_next().await? {
            match token {
                Token::Reasoning(reasoning) => {
                    yield AssistantEvent::Reasoning(reasoning);
                },
                Token::Response(response) => {
                    if tools {
                        continue;
                    }
                    match &mut state {
                        ChatState::None => {
                            let mut parser = ResponseParser::default();
                            parser.push(&response);

                            while let Some(next) = parser.next()? {
                                yield next;
                            }

                            state = ChatState::Response(parser);
                        },
                        ChatState::Response(response_parser) => {
                            response_parser.push(&response);

                            while let Some(next) = response_parser.next()? {
                                yield next;
                            }
                        },
                    }
                },
                Token::Tool(ToolToken::Start(_t)) => {
                    if !tools {
                        continue;
                    }

                    match state {
                        ChatState::None => {},
                        ChatState::Response(mut response_parser) => {
                            response_parser.done()?;

                            while let Some(next) = response_parser.next()? {
                                yield next;
                            }
                        },
                    }

                    // TODO: other tools..
                    let parser = ResponseParser::default();
                    state = ChatState::Response(parser);

                },
                Token::Tool(ToolToken::Arguments(args)) => {
                    if !tools {
                        continue;
                    }

                    match &mut state {
                        ChatState::None => {
                            Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected chat state"))?;
                        },
                        ChatState::Response(response_parser) => {
                            response_parser.push(&args);

                            while let Some(next) = response_parser.next()? {
                                yield next;
                            }
                        },
                    }
                },
            }
        }

        match state {
            ChatState::None => {},
            ChatState::Response(mut response_parser) => {
                response_parser.done()?;

                while let Some(next) = response_parser.next()? {
                    yield next;
                }
            },
        }
    }
}

enum ChatState {
    None,
    Response(ResponseParser),
}

struct ResponseParser {
    json_parser: JsonParser<PushJsonFeeder>,
    state: State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    ExpectRootStart,
    InRootObject,
    ExpectRootValue(RootField),

    InMessagesArray,

    InMessageObject,
    ExpectMessageValue(MessageField),

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

    fn next(&mut self) -> io::Result<Option<AssistantEvent>> {
        fn mk_err(s: impl Into<String>) -> io::Result<Option<AssistantEvent>> {
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
                        self.state = State::InMessageObject;
                        return Ok(Some(AssistantEvent::Typing));
                    }
                    _ => {
                        return mk_err("unexpected object start");
                    }
                },
                JsonEvent::EndObject => match self.state {
                    State::Done => {}
                    State::InMessageObject => {
                        self.state = State::InMessagesArray;
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
                    State::InMessageObject => match self.json_parser.current_str() {
                        Ok("_reasoning") => {
                            self.state = State::ExpectMessageValue(MessageField::Reasoning);
                        }
                        Ok("message") => {
                            self.state = State::ExpectMessageValue(MessageField::MessageContent);
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
                            return Ok(Some(AssistantEvent::Reasoning(value.to_string())));
                        }
                        State::ExpectMessageValue(MessageField::Reasoning) => {
                            self.state = State::InMessageObject;
                            return Ok(Some(AssistantEvent::Reasoning(value.to_string())));
                        }
                        State::ExpectMessageValue(MessageField::MessageContent) => {
                            self.state = State::InMessageObject;
                            return Ok(Some(AssistantEvent::Message(value.to_string())));
                        }
                        _ => return mk_err("unexpected string value"),
                    }
                }
                JsonEvent::ValueTrue => match self.state {
                    State::ExpectRootValue(RootField::ShouldRespond) => {
                        // TODO: should a typing event be sent here?
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
            json_parser: JsonParser::new(PushJsonFeeder::new()),
            state: State::ExpectRootStart,
        }
    }
}
