//! HTTP-level tests: the real router over the in-memory store, driven
//! in-process with `tower::ServiceExt::oneshot` (no sockets).

use std::{sync::Arc, time::Duration};

use axon_core::store::MemoryStore;
use axon_server::{AppState, Options, config::AccessConfig, router};
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

const ALICE: &str = "dev-alice-token";
const BOB: &str = "dev-bob-token";

fn app() -> Router {
    let state = AppState::new(
        Arc::new(MemoryStore::default()),
        &AccessConfig::dev(),
        Options::default(),
    )
    .expect("dev config is valid");
    router(state)
}

struct Reply {
    status: StatusCode,
    headers: header::HeaderMap,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("not JSON ({e}): {}", self.body))
    }
}

async fn call(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Reply {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = match body {
        Some(json) => {
            request = request.header(header::CONTENT_TYPE, "application/json");
            Body::from(json.to_string())
        }
        None => Body::empty(),
    };
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        app.clone()
            .oneshot(request.body(body).expect("request builds")),
    )
    .await
    .expect("request completes")
    .expect("router is infallible");
    let (parts, body) = response.into_parts();
    let bytes = tokio::time::timeout(Duration::from_secs(10), body.collect())
        .await
        .expect("body completes")
        .expect("body reads")
        .to_bytes();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8(bytes.to_vec()).expect("utf-8 body"),
    }
}

async fn get(app: &Router, uri: &str, token: &str) -> Reply {
    call(app, Method::GET, uri, Some(token), None).await
}

async fn post(app: &Router, uri: &str, token: &str, body: Value) -> Reply {
    call(app, Method::POST, uri, Some(token), Some(body)).await
}

/// Parses an SSE body into `(id, event, data)` triples, skipping comments.
fn parse_sse(body: &str) -> Vec<(String, String, Value)> {
    body.split("\n\n")
        .filter_map(|frame| {
            let field = |name: &str| {
                frame
                    .lines()
                    .find_map(|line| line.strip_prefix(name)?.strip_prefix(':'))
                    .map(|v| v.trim_start().to_owned())
            };
            Some((
                field("id")?,
                field("event")?,
                serde_json::from_str(&field("data")?).ok()?,
            ))
        })
        .collect()
}

fn pipeline() -> Value {
    json!({
        "name": "apoe4 vs p-tau217",
        "steps": [
            {"id": "select", "tool": "cohort.query",
             "input": {"biomarker": "plasma_ptau217", "group_by": "apoe4_carrier"}},
            {"id": "test", "tool": "stats.welch_t_test",
             "input": {"a": {"$ref": "select#/groups/carrier"},
                       "b": {"$ref": "select#/groups/non_carrier"}}},
        ],
    })
}

#[tokio::test]
async fn requires_a_valid_bearer_token() {
    let app = app();
    for token in [None, Some("wrong-token")] {
        let reply = call(&app, Method::GET, "/v1/jobs", token, None).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
        assert_eq!(reply.headers[header::WWW_AUTHENTICATE], "Bearer");
        assert_eq!(reply.json()["error"]["code"], "unauthorized");
    }
    // Operational endpoints stay open.
    assert_eq!(
        call(&app, Method::GET, "/healthz", None, None).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn submits_a_job_and_streams_it_to_completion() {
    let app = app();
    let created = post(&app, "/v1/jobs", ALICE, pipeline()).await;
    assert_eq!(created.status, StatusCode::CREATED);
    let id = created.json()["id"].as_str().expect("job id").to_owned();
    assert_eq!(created.headers[header::LOCATION], format!("/v1/jobs/{id}"));

    // The SSE response ends by itself once the job is terminal, so reading the
    // whole body is "wait for completion".
    let stream = get(&app, &format!("/v1/jobs/{id}/events"), ALICE).await;
    assert_eq!(stream.status, StatusCode::OK);
    assert_eq!(stream.headers[header::CONTENT_TYPE], "text/event-stream");
    let events = parse_sse(&stream.body);
    let kinds: Vec<&str> = events.iter().map(|(_, kind, _)| kind.as_str()).collect();
    assert_eq!(
        kinds,
        [
            "submitted",
            "step_started",
            "step_succeeded",
            "step_started",
            "step_succeeded",
            "succeeded"
        ]
    );
    // SSE ids are the sequence numbers, and the payload repeats them.
    for (index, (id, _, data)) in events.iter().enumerate() {
        assert_eq!(id, &(index + 1).to_string());
        assert_eq!(data["seq"], index + 1);
    }

    // Resuming with Last-Event-ID replays only what was missed.
    let request = Request::get(format!("/v1/jobs/{id}/events"))
        .header(header::AUTHORIZATION, format!("Bearer {ALICE}"))
        .header("last-event-id", "4")
        .body(Body::empty())
        .expect("request builds");
    let resumed = app.clone().oneshot(request).await.expect("infallible");
    let body = resumed
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let resumed = parse_sse(std::str::from_utf8(&body).expect("utf-8"));
    assert_eq!(
        resumed
            .iter()
            .map(|(id, ..)| id.as_str())
            .collect::<Vec<_>>(),
        ["5", "6"]
    );

    // The folded view agrees with the stream and carries provenance.
    let job = get(&app, &format!("/v1/jobs/{id}"), ALICE).await.json();
    assert_eq!(job["status"], "succeeded");
    assert_eq!(job["last_seq"], 6);
    let test_step = &job["steps"][1];
    assert!(test_step["output"]["p_value"].as_f64().expect("p-value") < 0.001);
    assert_eq!(test_step["provenance"]["tool"], "stats.welch_t_test");
    assert_eq!(test_step["provenance"]["principal"], "user:alice");
    assert_eq!(
        test_step["provenance"]["output_sha256"]
            .as_str()
            .map(str::len),
        Some(64)
    );

    let listed = get(&app, "/v1/jobs", ALICE).await.json();
    assert_eq!(listed["jobs"][0]["id"], id);

    let metrics = call(&app, Method::GET, "/metrics", None, None).await.body;
    assert!(metrics.contains("axon_jobs_submitted_total 1"), "{metrics}");
    assert!(
        metrics.contains("axon_jobs_finished_total{status=\"succeeded\"} 1"),
        "{metrics}"
    );
}

#[tokio::test]
async fn jobs_are_invisible_to_other_principals() {
    let app = app();
    let created = post(&app, "/v1/jobs", ALICE, pipeline()).await;
    let id = created.json()["id"].as_str().expect("job id").to_owned();

    for uri in [format!("/v1/jobs/{id}"), format!("/v1/jobs/{id}/events")] {
        let reply = get(&app, &uri, BOB).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{uri}");
    }
    let cancel = post(&app, &format!("/v1/jobs/{id}/cancel"), BOB, json!({})).await;
    assert_eq!(cancel.status, StatusCode::NOT_FOUND);
    assert_eq!(get(&app, "/v1/jobs", BOB).await.json()["jobs"], json!([]));
    // Malformed ids are a 404 as well, not a parser error.
    assert_eq!(
        get(&app, "/v1/jobs/not-a-uuid", ALICE).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn rejects_bad_submissions_before_anything_runs() {
    let app = app();

    // bob may not use cohort.query.
    let forbidden = post(&app, "/v1/jobs", BOB, pipeline()).await;
    assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
    assert_eq!(forbidden.json()["error"]["code"], "forbidden");

    let unknown = post(
        &app,
        "/v1/jobs",
        ALICE,
        json!({"steps": [{"id": "a", "tool": "nope"}]}),
    )
    .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    assert_eq!(unknown.json()["error"]["code"], "unknown_tool");

    let forward_ref = post(
        &app,
        "/v1/jobs",
        ALICE,
        json!({"steps": [{"id": "a", "tool": "stats.describe", "input": {"values": {"$ref": "b#/x"}}}]}),
    )
    .await;
    assert_eq!(forward_ref.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(forward_ref.json()["error"]["code"], "invalid_job_spec");

    let malformed = post(&app, "/v1/jobs", ALICE, json!({"stepz": []})).await;
    assert_eq!(malformed.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(malformed.json()["error"]["code"], "invalid_body");

    assert_eq!(get(&app, "/v1/jobs", ALICE).await.json()["jobs"], json!([]));
}

#[tokio::test]
async fn cancelling_a_finished_job_conflicts() {
    let app = app();
    let created = post(&app, "/v1/jobs", ALICE, pipeline()).await;
    let id = created.json()["id"].as_str().expect("job id").to_owned();
    get(&app, &format!("/v1/jobs/{id}/events"), ALICE).await; // wait for completion

    let cancel = post(&app, &format!("/v1/jobs/{id}/cancel"), ALICE, json!({})).await;
    assert_eq!(cancel.status, StatusCode::CONFLICT);
    assert_eq!(cancel.json()["error"]["message"], "job already succeeded");
}

#[tokio::test]
async fn tools_are_listed_and_invoked_per_caller() {
    let app = app();
    let names = |reply: Reply| -> Vec<String> {
        reply.json()["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|t| t["name"].as_str().expect("name").to_owned())
            .collect()
    };
    assert_eq!(
        names(get(&app, "/v1/tools", ALICE).await),
        ["cohort.query", "stats.describe", "stats.welch_t_test"]
    );
    assert_eq!(names(get(&app, "/v1/tools", BOB).await), ["stats.describe"]);

    let ok = post(
        &app,
        "/v1/tools/stats.describe/invoke",
        BOB,
        json!({"values": [1, 2, 3, 4]}),
    )
    .await;
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(ok.json()["output"]["mean"], 2.5);
    assert_eq!(ok.json()["provenance"]["principal"], "user:bob");

    let denied = post(
        &app,
        "/v1/tools/cohort.query/invoke",
        BOB,
        json!({"biomarker": "age"}),
    )
    .await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN);

    let invalid = post(
        &app,
        "/v1/tools/stats.describe/invoke",
        BOB,
        json!({"values": "many"}),
    )
    .await;
    assert_eq!(invalid.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(invalid.json()["error"]["code"], "invalid_tool_input");
}

#[tokio::test]
async fn speaks_the_model_context_protocol() {
    let app = app();
    let rpc = |id: u64, method: &str, params: Value| json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});

    let init = post(
        &app,
        "/mcp",
        ALICE,
        rpc(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            }),
        ),
    )
    .await
    .json();
    assert_eq!(init["id"], 1);
    assert_eq!(init["result"]["protocolVersion"], "2025-03-26");
    assert_eq!(init["result"]["serverInfo"]["name"], "axon");
    assert!(init["result"]["capabilities"]["tools"].is_object());

    // Notifications are accepted without a body.
    let note = post(
        &app,
        "/mcp",
        ALICE,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;
    assert_eq!(
        (note.status, note.body.as_str()),
        (StatusCode::ACCEPTED, "")
    );

    let tools = post(&app, "/mcp", BOB, rpc(2, "tools/list", json!({})))
        .await
        .json();
    let tools = tools["result"]["tools"].as_array().expect("tools").clone();
    assert_eq!(tools.len(), 1, "bob only sees what he may call");
    assert_eq!(tools[0]["name"], "stats.describe");
    assert_eq!(tools[0]["inputSchema"]["type"], "object");

    let called = post(
        &app,
        "/mcp",
        BOB,
        rpc(
            3,
            "tools/call",
            json!({"name": "stats.describe", "arguments": {"values": [2, 4]}}),
        ),
    )
    .await
    .json();
    let result = &called["result"];
    assert_eq!(result["isError"], false);
    assert_eq!(result["structuredContent"]["mean"], 3.0);
    assert_eq!(result["_meta"]["axon/provenance"]["tool"], "stats.describe");
    // The text block is the same payload, for clients without structured output.
    let text: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().expect("text")).expect("json");
    assert_eq!(&text, &result["structuredContent"]);

    // A tool bob may not call looks exactly like one that does not exist.
    for name in ["cohort.query", "no.such.tool"] {
        let denied = post(
            &app,
            "/mcp",
            BOB,
            rpc(4, "tools/call", json!({"name": name, "arguments": {}})),
        )
        .await
        .json();
        assert_eq!(denied["error"]["code"], -32602, "{name}");
        assert_eq!(denied["error"]["message"], format!("unknown tool `{name}`"));
    }

    // A tool that runs and fails is a result, not a protocol error.
    let failed = post(
        &app,
        "/mcp",
        ALICE,
        rpc(
            5,
            "tools/call",
            json!({"name": "stats.welch_t_test", "arguments": {"a": [1, 1], "b": [2, 2]}}),
        ),
    )
    .await
    .json();
    assert_eq!(failed["result"]["isError"], true);

    let unknown = post(&app, "/mcp", ALICE, rpc(6, "resources/list", json!({})))
        .await
        .json();
    assert_eq!(unknown["error"]["code"], -32601);
    let invalid = post(
        &app,
        "/mcp",
        ALICE,
        json!({"jsonrpc": "1.0", "id": 7, "method": "ping"}),
    )
    .await
    .json();
    assert_eq!(
        (&invalid["error"]["code"], &invalid["id"]),
        (&json!(-32600), &json!(7))
    );

    assert_eq!(
        call(
            &app,
            Method::POST,
            "/mcp",
            None,
            Some(rpc(8, "ping", json!({})))
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
}
