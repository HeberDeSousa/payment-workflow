# payment-workflow
Workflow for a fictional payment infrastructure company processing millions of transactions per day. Its platform consists of Java/Spring Boot microservices, PostgreSQL, Kafka, AWS, and Kubernetes.

The company has a 24/7 production support team responsible for detecting and resolving incidents affecting payment processing.

# Current workflow

When a production incident occurs:

1 - Payment failure 

2 - Monitoring detects anomaly

3 - Create incident

4 - On-call engineer investigates (steps below are not necessarily followed in order)

    4.1 - Check dashboards
  
    4.2 - Search logs and traces

    4.3 - Check Kafka
  
    4.4 - Check recent deployments
  
    4.5 - Search previous incidents
  
    4.6 - Investigate database
    

5 - Identify probable cause

6 - Apply remediation

7 - Validate recovery

8 - Document incident

9 - Postmortem / RCA

# Identifying deterministic and non-deterministic steps

| Step                                 | Type                      | Risk          |
| ------------------------------------ | ------------------------- | ------------- |
| Detect anomaly                       | Deterministic/statistical | High          |
| Create incident                      | Deterministic             | Low           |
| Gather logs                          | Deterministic             | Low           |
| Correlate logs                       | Non-deterministic         | Medium        |
| Analyze deployment changes           | Mixed                     | Medium        |
| Investigate database                 | Mixed                     | Medium        |
| Search historical incidents/runbooks | Non-deterministic         | Low           |
| Determine root cause                 | Non-deterministic         | High          |
| Execute remediation                  | Deterministic             | **Very High** |
| Validate recovery                    | Deterministic             | High          |
| Generate incident report             | Non-deterministic         | Low           |
| Generate RCA draft                   | Non-deterministic         | Medium        |


# Workflow Evaluation

The goal is to get the highest value × feasibility × risk profile.


Investigation phase is potentially the biggest business opportunity, but not necessarily the best first AI deployment opportunity. 
Based on classification the strongest initial candidates are:


- Search historical incidents — very high value, low risk.
- Correlate logs — high value, medium risk.
- Generate incident reports — high value, low risk.
- Generate RCA draft — high value, medium risk.


# Deployment Strategy

New workflow

1 - Payment failure 

2 - Monitoring detects anomaly

3 - Create incident

4 - Collect investigation data (Logs / Metrics / Traces / Deployments / Git / DB

        4.1 - Correlate information (AI)

        4.2 - Search historical incidents/runbooks (AI)

5 - Determine probable cause (**AI + human-in-the-loop approval**)

6 - Apply remediation

7 - Validate recovery

8 - Document incident (**AI**)

9 - Postmortem / RCA (**AI + human-in-the-loop**)


# Overall architecture

Assume the fictional company already has:

Kafka — event streaming
AWS — infrastructure
CloudWatch — AWS logs/metrics
OpenSearch — application logs
Prometheus/Grafana — application metrics
Jira/ServiceNow — incident management
GitHub — source code
PostgreSQL — application database
Kubernetes/EKS — application platform
Confluence/S3 — documentation and historical incident data

## AI platform architecture implementation

```
                         EXISTING SYSTEMS
 ┌─────────────────────────────────────────────────────────────┐
 │                                                             │
 │  Kafka       AWS        OpenSearch    Prometheus    GitHub  │
 │    │          │              │            │           │     │
 └────┼──────────┼──────────────┼────────────┼───────────┼─────┘
      │          │              │            │           │
      ▼          ▼              ▼            ▼           ▼
 ┌──────────────────────────────────────────────────────────────┐
 │                    Integration Layer                         │
 │                                                              │
 │ API Gateway │ Kafka Consumers │ AWS APIs │ Observability API │
 └────────────────────────────┬─────────────────────────────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │ AI Orchestrator     │
                   │                     │
                   │ LLM + Tool Calling  │
                   └──────────┬──────────┘
                              │
             ┌────────────────┼────────────────┐
             ▼                ▼                ▼
      Historical Search   Investigation   Report Generator
             │                │                │
             └────────────────┼────────────────┘
                              ▼
                       Knowledge Layer
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
                Vector DB          Search Index
```

The AI model itself doesn't need to know how Kafka, AWS, or OpenSearch works.

## Kafka integration

### Kafka as a source of incident context

Suppose PayFlow processes payment events:

- payment.created
- payment.authorized
- payment.failed
- payment.completed
                
The AI shouldn't consume the entire Kafka cluster. Deploy a dedicated AI Investigation Consumer instead.

```
Kafka
  │
  ├── payment.created
  ├── payment.authorized
  ├── payment.failed
  └── payment.completed
          │
          ▼
   AI Event Consumer
          │
          ▼
   Filtering / Aggregation
          │
          ▼
    Incident Context Store
```

The consumer could filter events based on:
- service
- environment
- timestamp
- correlation_id
- event_type
- incident_id

For example, if an incident starts at 14:30 UTC the system could retrieve events from 14:20 → 14:40 rather than sending millions of Kafka messages to the LLM.

### Kafka should feed a deterministic preprocessing layer

Kafka could produce a large amount of events per hour or even per second so we have to avoid every event going directly to LLM.
So the flow would be:

Kafka -> Consumer -> Stream processing (Filter, Aggregate, Deduplicate, Correlate, Enrich -> Incident Context -> AI

Example:

10,000 payment.failed events<br>
             ↓<br>
Grouped by provider<br>
             ↓<br>
Provider A: 8,921 failures<br>
Provider B: 127 failures<br>
Provider C: 52 failures

The AI receives:
```
{
  "service": "payment-processing",
  "window": "14:30-14:40",
  "failures": 9100,
  "providers": {
    "A": 8921,
    "B": 127,
    "C": 52
  }
}
```
Now the LLM can reason about the information instead of processing raw event streams.

# AWS Integration

AI platform gets access through AWS APIs exposed as controlled tools.

AI Agent<br>
↓<br>
Tool Gateway
─ CloudWatchTool
─ EC2Tool
─ EKSObservabilityTool
─ S3Tool
─ RDSMetricsTool
─ LambdaTool

**Each tool needs to have a very narrow contract.**

## CloudWatch

Suppose the incident is: "Payment service latency increased."

AI could invoke:
```
get_metric(
    service="payment-processing",
    metric="Latency",
    start="14:20",
    end="14:40"
)
```
The tool queries CloudWatch and returns structured information:
```
{
  "metric": "Latency",
  "p50": 210,
  "p95": 1840,
  "p99": 4210,
  "baseline_p95": 350,
  "timestamp": "14:32"
}
```

LLM doesn't directly interact with AWS but receives the result and reasons about it.

### CloudWatch Logs

For AWS-hosted services, CloudWatch Logs could be exposed through a similar tool:
```
search_logs(
    service="payment-processing",
    start="14:30",
    end="14:40",
    query="TimeoutException"
)
```

The tool returns: 
14:32:01 TimeoutException PaymentProviderClient
14:32:03 TimeoutException PaymentProviderClient
14:32:04 TimeoutException PaymentProviderClient
...

adding preprocessing the tool should return to the LLM:
```
Total occurrences: 8,923

First occurrence: 14:32:01

Affected component:
PaymentProviderClient

Top exception:
SocketTimeoutException

Frequency:
+1,340% compared with baseline
```
## AWS S3 as a knowledge repository

S3 could be used for historical knowledge. For example:

```
s3://payflow-ai-knowledge/

    incidents/
    postmortems/
    runbooks/
    architecture/
    service-documents/
```

A pipeline can process those documents:

S3 -> Document ingestion -> Chunking -> Metadata extraction -> Embeddings -> Vector database

The retrieval system searches:

- previous incidents
- postmortems
- runbooks
- architecture documents

The goal is for AI to recognize and relate previous problems 

## EKS / Kubernetes integration

Expose read-only tools such as:
- get_pods()
- get_deployments()
- get_events()
- get_pod_logs()
- get_resource_usage()

AI could request:
```
get_deployment(
    namespace="payments",
    deployment="payment-processing"
)
```

and receive:
```
{
  "version": "v4.18",
  "replicas": 12,
  "ready": 12,
  "deployment_time": "14:27"
}
```

Then correlate that with:

```
14:27 deployment
14:32 error increase
```
generating strong evidence
