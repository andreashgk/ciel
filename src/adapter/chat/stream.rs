use std::io;
use std::pin::Pin;

use actson::JsonEvent;
use actson::JsonParser;
use actson::feeder::PushJsonFeeder;
use futures_core::Stream;
use futures_util::TryStreamExt;
use futures_util::stream;

use crate::adapter::chat::AssistantEvent;

// TODO: this code is currently quite awful
pub fn parse_response_stream(
    stream: impl Stream<Item = io::Result<String>>,
) -> impl Stream<Item = io::Result<AssistantEvent>> {
    let feeder = PushJsonFeeder::new();
    let parser = JsonParser::new(feeder);
    let stream = Box::pin(stream);

    #[derive(PartialEq, Eq)]
    enum JsonState {
        ShouldRespond,
        Message,
        MessageEnd,
        UnknownField,
    }

    struct State<S> {
        parser: JsonParser<PushJsonFeeder>,
        stream: Pin<Box<S>>,
        state: JsonState,
    }

    let state = State {
        parser,
        stream,
        state: JsonState::UnknownField,
    };

    stream::try_unfold(state, |mut state| async move {
        while let Some(event) = state
            .parser
            .next_event()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        {
            match event {
                JsonEvent::NeedMoreInput => {
                    let next = state.stream.try_next().await?;
                    match next {
                        Some(next) => {
                            state.parser.feeder.push_bytes(next.as_bytes());
                        }
                        None => {
                            state.parser.feeder.done();
                        }
                    }
                    continue;
                }
                // TODO: dont unwrap
                JsonEvent::FieldName => match state.parser.current_str().unwrap() {
                    "should_respond" => {
                        state.state = JsonState::ShouldRespond;
                    }
                    "message" => {
                        state.state = JsonState::Message;
                    }
                    _ => {
                        continue;
                    }
                },
                JsonEvent::ValueString => {
                    if state.state != JsonState::Message {
                        continue;
                    }

                    state.state = JsonState::MessageEnd;

                    return Ok(Some((
                        AssistantEvent::Message(state.parser.current_str().unwrap().to_string()),
                        state,
                    )));
                }
                JsonEvent::StartObject => {
                    if state.state == JsonState::MessageEnd {
                        state.state = JsonState::UnknownField;
                        return Ok(Some((AssistantEvent::Typing, state)));
                    }
                }
                JsonEvent::ValueTrue => {
                    if state.state != JsonState::ShouldRespond {
                        panic!("invalid state");
                    }

                    state.state = JsonState::UnknownField;

                    return Ok(Some((AssistantEvent::Typing, state)));
                }
                JsonEvent::ValueFalse => {
                    if state.state != JsonState::ShouldRespond {
                        panic!("invalid state");
                    }

                    return Ok(None);
                }
                _ => {
                    continue;
                }
            }
        }

        Ok(None)
    })
}
