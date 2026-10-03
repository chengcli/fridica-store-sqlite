//! What the replay ledger keeps of its bulkiest boundaries (#116). Nothing
//! in the daemon reads Slack responses or backend bytes back from it; they are
//! there to debug an incident. With `state.record = "summary"` (the default)
//! a Slack response keeps its outcome and shape, not its body; backend bytes
//! are kept as text whenever they are UTF-8, which loses nothing.
use serde_json::{json, Map, Value};

/// Strings longer than this in recorded call arguments are cut.
const ARGUMENT_CHARS: usize = 500;

/// A Slack Web API response reduced to what explains the call: its outcome,
/// identifiers, pagination, and the size (and time span) of each list.
pub fn slack_body(body: &Value) -> Value {
    let Some(object) = body.as_object() else {
        return body.clone();
    };
    let mut kept = Map::new();
    for key in [
        "ok",
        "error",
        "warning",
        "needed",
        "ts",
        "has_more",
        "is_limited",
    ] {
        if let Some(value) = object.get(key) {
            kept.insert(key.into(), value.clone());
        }
    }
    if let Some(channel) = object.get("channel") {
        // `channel` is an ID in posts and a whole object in conversations.info.
        kept.insert(
            "channel".into(),
            match channel {
                Value::Object(c) => json!({"id":c.get("id"),"name":c.get("name")}),
                other => other.clone(),
            },
        );
    }
    if let Some(cursor) = object
        .get("response_metadata")
        .and_then(|m| m.get("next_cursor"))
    {
        kept.insert("next_cursor".into(), cursor.clone());
    }
    for (key, value) in object {
        if let Value::Array(items) = value {
            kept.insert(format!("{key}_count"), json!(items.len()));
            let times: Vec<&str> = items.iter().filter_map(|i| i["ts"].as_str()).collect();
            if let (Some(first), Some(last)) = (times.iter().min(), times.iter().max()) {
                kept.insert(format!("{key}_ts"), json!([first, last]));
            }
        }
    }
    kept.insert("summarized".into(), json!(true));
    Value::Object(kept)
}

/// Call arguments with long strings cut (a post's text is in the outbox).
pub fn arguments(value: &Value) -> Value {
    match value {
        Value::String(s) if s.chars().count() > ARGUMENT_CHARS => {
            let kept: String = s.chars().take(ARGUMENT_CHARS).collect();
            json!(format!("{kept}[… {} characters]", s.chars().count()))
        }
        Value::Array(items) => Value::Array(items.iter().map(arguments).collect()),
        Value::Object(map) => {
            Value::Object(map.iter().map(|(k, v)| (k.clone(), arguments(v))).collect())
        }
        other => other.clone(),
    }
}

/// A recorded fridica-slack boundary, summarized by kind.
pub fn slack(kind: &str, payload: Value) -> Value {
    let mut payload = payload;
    match kind {
        "slack_http_call" => {
            if let Some(args) = payload.get("arguments").map(arguments) {
                payload["arguments"] = args;
            }
        }
        "slack_http_result" => {
            if let Some(body) = payload
                .get("body")
                .and_then(|b| b.get("json"))
                .map(slack_body)
            {
                payload["body"]["json"] = body;
            }
        }
        _ => {}
    }
    payload
}

/// A catch-up history read's recorded result (`{"call", "result": {"Ok": body}}`).
pub fn history(payload: Value) -> Value {
    let mut payload = payload;
    if let Some(body) = payload
        .get("result")
        .and_then(|r| r.get("Ok"))
        .map(slack_body)
    {
        payload["result"]["Ok"] = body;
    }
    payload
}

/// A backend wire event with received bytes kept as text when they are
/// UTF-8 (about a quarter of the size of a JSON array of numbers).
pub fn wire(event: Value) -> Value {
    let mut event = event;
    let text = event
        .get("bytes")
        .and_then(Value::as_array)
        .and_then(|bytes| {
            bytes
                .iter()
                .map(|b| b.as_u64().and_then(|b| u8::try_from(b).ok()))
                .collect::<Option<Vec<u8>>>()
        })
        .and_then(|bytes| String::from_utf8(bytes).ok());
    if let (Some(text), Some(object)) = (text, event.as_object_mut()) {
        object.remove("bytes");
        object.insert("text".into(), json!(text));
    }
    event
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slack_bodies_keep_outcome_and_shape() {
        let history = json!({"ok":true,"has_more":false,"messages":[{"ts":"2.1","text":"x".repeat(5000)},{"ts":"1.1","text":"y"}],
            "response_metadata":{"next_cursor":"abc"},"pin_count":0});
        let kept = slack_body(&history);
        assert_eq!(
            kept,
            json!({"ok":true,"has_more":false,"messages_count":2,"messages_ts":["1.1","2.1"],"next_cursor":"abc","summarized":true})
        );
        let info = slack_body(
            &json!({"ok":true,"channel":{"id":"C1","name":"general","purpose":{"value":"long"}}}),
        );
        assert_eq!(info["channel"], json!({"id":"C1","name":"general"}));
        let failed = slack_body(&json!({"ok":false,"error":"ratelimited"}));
        assert_eq!(failed["error"], "ratelimited");
        let result = slack(
            "slack_http_result",
            json!({"call":3,"status":200,"body":{"json":history}}),
        );
        assert_eq!(
            (
                result["status"].as_u64(),
                result["body"]["json"]["messages_count"].as_u64()
            ),
            (Some(200), Some(2))
        );
        let call = slack(
            "slack_http_call",
            json!({"method":"chat.postMessage","arguments":{"text":"z".repeat(900),"channel":"C1"}}),
        );
        assert!(call["arguments"]["text"]
            .as_str()
            .unwrap()
            .ends_with("[… 900 characters]"));
        assert_eq!(
            history_from(json!({"call":1,"result":{"Err":"timeout"}})),
            json!({"call":1,"result":{"Err":"timeout"}})
        );
    }
    fn history_from(v: Value) -> Value {
        history(v)
    }
    #[test]
    fn wire_bytes_become_text_without_loss() {
        let event = json!({"direction":"received","bytes":"{\"type\":\"result\"}\n".bytes().collect::<Vec<u8>>()});
        let kept = wire(event);
        assert_eq!(
            kept,
            json!({"direction":"received","text":"{\"type\":\"result\"}\n"})
        );
        let binary = json!({"direction":"received","bytes":[255,254]});
        assert_eq!(wire(binary.clone()), binary);
        let other = json!({"direction":"start","command":["claude"]});
        assert_eq!(wire(other.clone()), other);
    }
}
