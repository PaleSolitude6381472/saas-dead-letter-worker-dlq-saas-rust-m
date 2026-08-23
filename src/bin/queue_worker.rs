use saas_dead_letter_worker::{
    infrai_client::{InfraiClient, InfraiError},
    job_policy::{decide_failure, JobDecision, SaaSJob},
};
use std::{env, fmt};

#[derive(Debug)]
enum WorkerError {
    Infrai(InfraiError),
    InvalidJob(serde_json::Error),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Infrai(error) => write!(f, "queue operation failed: {error}"),
            Self::InvalidJob(error) => write!(f, "invalid job payload: {error}"),
        }
    }
}

impl std::error::Error for WorkerError {}

impl From<InfraiError> for WorkerError {
    fn from(value: InfraiError) -> Self {
        Self::Infrai(value)
    }
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let client = InfraiClient::from_env()?;
    let failure_reason = env::var("JOB_FAILURE_REASON")
        .unwrap_or_else(|_| "upstream business rule rejected the job".into());

    for message in client.consume(10).await? {
        let job: SaaSJob = serde_json::from_value(message.payload)
            .map_err(WorkerError::InvalidJob)?;

        match decide_failure(job, &failure_reason) {
            JobDecision::Retry => {
                println!("retry scheduled by visibility window: {}", message.message_id);
            }
            JobDecision::DeadLetter(dead_letter) => {
                let key = format!("dead-letter:{}", dead_letter.job.job_id);
                client.publish_dead_letter(&dead_letter, &key).await?;
                client.ack(&message.message_id).await?;
                println!("dead-lettered and acknowledged: {}", message.message_id);
            }
        }
    }
    Ok(())
}

