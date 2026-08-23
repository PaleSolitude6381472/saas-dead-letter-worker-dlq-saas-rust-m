use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    TenantOnboarding,
    AccountLifecycle,
    AdminOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SaaSJob {
    pub job_id: String,
    pub tenant_id: String,
    pub kind: JobKind,
    pub attempt: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeadLetter {
    pub job: SaaSJob,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDecision {
    Retry,
    DeadLetter(DeadLetter),
}

pub fn decide_failure(job: SaaSJob, reason: impl Into<String>) -> JobDecision {
    if job.attempt < 3 {
        JobDecision::Retry
    } else {
        JobDecision::DeadLetter(DeadLetter {
            job,
            reason: reason.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn third_failed_admin_operation_moves_to_dead_letter() {
        let job = SaaSJob {
            job_id: "job-1042".into(),
            tenant_id: "tenant-acme".into(),
            kind: JobKind::AdminOperation,
            attempt: 3,
        };

        let decision = decide_failure(job.clone(), "role assignment rejected");

        assert_eq!(
            decision,
            JobDecision::DeadLetter(DeadLetter {
                job,
                reason: "role assignment rejected".into(),
            })
        );
    }
}

