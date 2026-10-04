# Reliability refresh: demand evidence and scope

Sources checked **2026-09-07**. Target: North American internships, 2027 new-grad
and early-career backend/infrastructure work. LakeBridge contributes reproducible
data preparation and publication correctness; it is not a training/serving system.
The existing portfolio already covers queues, stream processing, service APIs and
GPU/inference work. Strengthening this publication boundary avoids duplicating them.

## Recent official job descriptions

| Sample | Stage, location and date evidence | Relevant signals and limits |
|---|---|---|
| [Amazon AWS Data Services, Fall 2026 SDE intern, 10412530](https://www.amazon.jobs/jobs/10412530) | Seattle/US; enrolled students graduating Jan 2027–Dec 2029; Aug/Sep 2026 start. Official text retrieved; no posting date in extracted text. [University distribution record](https://career.mercer.edu/jobs/amazon-software-development-engineer-intern-aws-data-services-fall-2026-us/) dates recruitment to May 7, 2026; that date is secondary evidence. | Distributed systems, fault tolerance, cloud services, monitoring, debugging, CI/CD; project experience counts. Recent demand sample, not a guarantee the start window remains practical. Full-time internship availability needs separate checking. |
| [Snowflake Infrastructure Automation, Fall 2026 intern](https://careers.snowflake.com/us/en/job/SNCOUSCC3FB214186D4B439D3F15B47307A231EXTERNALENUSBBDEE8BD443C456FA4F01B363D843310/Software-Engineer-Intern-Infrastructure-Automation-Fall-2026) | Bellevue/Menlo Park; enrolled undergraduates/Master's/PhD. Official indexed text retrieved; direct open returned an internal fetch error. Posting date absent there; [Wing distribution record](https://careers.wing.vc/companies/snowflake/jobs/78108812-software-engineer-intern-infrastructure-automation-fall-2026) says May 9, 2026 and no longer accepting applications. | Python/Java, reliable services, functional/performance automation and release engineering. Terraform/Jenkins/Ansible and Docker/Kubernetes are pluses, not universal minimums. Recent historical sample, not a verified open job. |
| [Stripe Software Engineer, New Grad, 8128744](https://stripe.com/careers/listing/software-engineer-new-grad/8128744) | Seattle/New York/South San Francisco; degree by summer 2027; at most 18 months professional experience excluding internships. Official page retrieved; posting date undisclosed, so publication within six months is unverified. | Fundamentals, understanding unfamiliar systems, written explanation, safe system updates and independent small-project delivery; backend/infrastructure specialization preferred. AI-assisted work must be reviewed and validated; this does not require an AI feature in LakeBridge. |

These three purposive samples are not a market prevalence estimate. Reliable
delivery, tests/debugging and explainable decisions recur with different emphasis.
AWS-specific tooling, Snowflake's automation tool preferences and Stripe's language
mix are company preferences. AI wording does not establish that a migration project
needs an LLM wrapper. Sponsorship was not verified from these texts. More senior
Amazon roles requiring 3+ years and non-US roles were excluded from the core sample.

## Official ecosystem check

| Component and sources | Observed state | Decision, benefit and cost |
|---|---|---|
| [Spark source tag](https://github.com/apache/spark/tree/v3.5.9), [3.5.9 release](https://spark.apache.org/releases/spark-release-3-5-9.html), [4.0 SQL migration](https://spark.apache.org/docs/4.0.0/sql-migration-guide.html) | 3.5.9 maintenance/security/correctness release: July 16, 2026; official news also lists 4.2.0 on July 14. | Retain qualified 3.5.9. Defects are Python/SQL transaction boundaries. Spark 4 adds engine/SQL/Scala artifact qualification with no identified benefit to this fix. No newest-major or universally safest-runtime claim. |
| [Iceberg GitHub releases](https://github.com/apache/iceberg/releases), [release notes](https://iceberg.apache.org/releases/) | 1.10.2: May 18, 2026, bug/security fixes. Project release date for 1.11.0: May 19; GitHub publication shows May 20. 1.11.0 provides Spark 3.5/Scala 2.12 artifacts and drops Java 11. | Keep runtime/AWS bundle at 1.10.2. 1.11.0 is plausible on JDK 17, not rejected as incompatible. No required new feature for this repair; upgrade requires repeating JDBC/S3, branch, retention and recovery qualification. |
| [Airflow releases](https://github.com/apache/airflow/releases), [support table](https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html), [Airflow 3 migration](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html) | Stable documentation: 3.3.1. Airflow 2 EOL: April 22, 2026. Airflow 3 changes execution architecture and public interfaces. | Optional 2.10.5 remains historical. Migration needs scheduler/API/worker topology, DAG imports and task-execution validation; separate deployment gap. |
| [MinIO repository](https://github.com/minio/minio) | Archived April 25, 2026, read-only. | Preserve historical reproducibility; pinned image is not a maintained production choice. Replacement needs S3 semantics, inventory and maintenance qualification. |

This is a focused dependency decision, not a complete vulnerability assessment of
every Python/JVM package. No dependency or lockfile changed in this update.

## Prioritized engineering plan and result

| Priority | Existing gap | Value and acceptance |
|---|---|---|
| P0 | Owner committed before recovery inputs | Commit run and owner together; verify process exit, PostgreSQL session loss, rollback and non-overwriting retries. |
| P0 | Abort used stale state and separate terminal writes | Reread after lock; atomically abort and release ownership; reject stale terminal updates and roll back injected failures. |
| P1 | Report error demoted completed publication | Keep post-completion I/O outside the lifecycle failure handler; Spark publication survives and retry creates no snapshots. |
| P1 | Evidence described only prior qualification | Preserve prior metrics/commit links; add local-only evidence, regression metrics and operator boundaries. |

Acceptance is recorded in [the runbook](run-lifecycle.md) and
[verification](evidence/run-lifecycle/verification.json). No throughput, latency,
availability or cost-improvement claim is made.
