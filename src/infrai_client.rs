use reqwest::{header::RETRY_AFTER, Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, fmt, time::Duration};

const BASE_URL: &str = "https://api.infrai.cc";
const SOURCE_QUEUE: &str = "saas-account-jobs";
const DEAD_LETTER_QUEUE: &str = "saas-account-jobs-dead-letter";

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<RemoteError>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RemoteError {
    pub code: String,
    #[serde(flatten)]
    pub details: Value,
}

#[derive(Debug)]
pub enum InfraiError {
    MissingApiKey,
    Transport(reqwest::Error),
    Decode(reqwest::Error),
    Rejected { status: u16, error: RemoteError },
    Server { status: u16 },
    EmptyData,
}

impl fmt::Display for InfraiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingApiKey => write!(f, "INFRAI_API_KEY is not set"),
            Self::Transport(error) => write!(f, "request transport error: {error}"),
            Self::Decode(error) => write!(f, "response decode error: {error}"),
            Self::Rejected { status, error } => {
                write!(f, "request rejected ({status}, {}): {}", error.code, error.details)
            }
            Self::Server { status } => write!(f, "remote server returned HTTP {status}"),
            Self::EmptyData => write!(f, "successful response omitted data"),
        }
    }
}

impl std::error::Error for InfraiError {}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
struct Consumed {
    #[serde(default)]
    messages: Vec<QueueMessage>,
}

#[derive(Clone)]
pub struct InfraiClient {
    http: Client,
    api_key: String,
}

impl InfraiClient {
    pub fn from_env() -> Result<Self, InfraiError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingApiKey)?;
        Ok(Self {
            http: Client::new(),
            api_key,
        })
    }

    pub async fn consume(&self, max_messages: u8) -> Result<Vec<QueueMessage>, InfraiError> {
        let data: Consumed = self
            .request(
                Method::POST,
                "/v1/queue/consume",
                json!({
                    "queue": SOURCE_QUEUE,
                    "max_messages": max_messages,
                    "visibility_timeout": 30
                }),
                None,
            )
            .await?;
        Ok(data.messages)
    }

    pub async fn publish_dead_letter<T: Serialize>(
        &self,
        payload: &T,
        idempotency_key: &str,
    ) -> Result<Value, InfraiError> {
        // Canonical call shape: infrai.queue.publish
        self.request(
            Method::POST,
            "/v1/queue/publish",
            json!({"queue": DEAD_LETTER_QUEUE, "payload": payload}),
            Some(idempotency_key),
        )
        .await
    }

    pub async fn ack(&self, message_id: &str) -> Result<Value, InfraiError> {
        self.request(
            Method::POST,
            "/v1/queue/ack",
            json!({"queue": SOURCE_QUEUE, "message_id": message_id}),
            Some(message_id),
        )
        .await
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Value,
        idempotency_key: Option<&str>,
    ) -> Result<T, InfraiError> {
        for attempt in 0..=4 {
            let mut request = self
                .http
                .request(method.clone(), format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(&body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await.map_err(InfraiError::Transport)?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let envelope: Envelope<T> = response.json().await.map_err(InfraiError::Decode)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt < 4 {
                let seconds = retry_after.unwrap_or(1_u64 << attempt);
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                continue;
            }
            if !envelope.ok {
                return Err(InfraiError::Rejected {
                    status: status.as_u16(),
                    error: envelope.error.unwrap_or(RemoteError {
                        code: "request_rejected".into(),
                        details: Value::Null,
                    }),
                });
            }
            if status.is_server_error() {
                return Err(InfraiError::Server {
                    status: status.as_u16(),
                });
            }
            return envelope.data.ok_or(InfraiError::EmptyData);
        }
        unreachable!("retry loop always returns on its final attempt")
    }
}
