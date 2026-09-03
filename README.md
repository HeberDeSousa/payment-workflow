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

### Incident Producer example

```
// Existing Incident Management service
// Publishes an event whenever a new production incident is created.

@Service
public class IncidentEventPublisher {

    private final KafkaTemplate<String, IncidentCreatedEvent> kafka;

    public void publish(Incident incident) {

        IncidentCreatedEvent event =
            new IncidentCreatedEvent(
                incident.getId(),
                incident.getService(),
                incident.getSeverity(),
                incident.getCreatedAt()
            );

        kafka.send(
            "incident.created",
            incident.getId().toString(),
            event
        );
    }
}
```

### Consumer example

```
@Component
public class IncidentAIConsumer {

    private final InvestigationOrchestrator orchestrator;

    @KafkaListener(
        topics = "incident.created",
        groupId = "ai-investigation"
    )
    public void handle(IncidentCreatedEvent event) {

        // Start investigation asynchronously.
        // The consumer should NOT block while the LLM is working.

        orchestrator.startInvestigation(event);
    }
}
```

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

###Cloudwatch service example:
```
@Service
public class CloudWatchService {

    private final CloudWatchClient cloudWatch;

    public MetricSummary getMetrics(Incident incident) {

        // Query only the metrics relevant to this incident.
        GetMetricDataRequest request =
            buildMetricRequest(
                incident.getService(),
                incident.getCreatedAt()
            );

        GetMetricDataResponse response =
            cloudWatch.getMetricData(request);

        return summarizeMetrics(response);
    }
}
```


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

### RAG implementation example

```
def search_historical_incidents(context):

    # Create a semantic representation of the current incident.
    query_embedding = embedding_model.embed(
        build_incident_description(context)
    )

    # Retrieve semantically similar incidents.
    semantic_results = vector_db.search(
        embedding=query_embedding,
        top_k=10
    )

    # Also perform exact/keyword searches for error codes,
    # service names and exception types.
    keyword_results = search_engine.search(
        service=context.service,
        errors=context.error_types
    )

    # Combine both retrieval strategies.
    return rerank(
        semantic_results + keyword_results
    )
```
We should use hybrid search instead of relying exclusively on vector similarity.

## IAM and security

Use IAM roles with least privilege with permissions limited to:

- CloudWatch read
- S3 read specific buckets
- EKS read-only
- RDS monitoring read

**It would not have AdministratorAccess or unrestricted like**<br>
- **s3:***
- **ec2:***
- **rds:***
- **eks:***

### Data security

Production logs can contain sensitive information so before sending data to an external LLM we need to introduce the following workflow:

Production data -> Data classification -> PII / secret detection -> Redaction -> Context filtering -> LLM

Resulting in

Before:
```
customerId=12345
idNumber=1111111111111111
address=abc, 123
error=TimeoutException
```
After:
```
customerId=[REDACTED]
idNumber=[REDACTED]
address=[REDACTED]
error=TimeoutException
```

# Database integration

We will choose a more conservative approach.
AI should initially have access to:
- read-only metadata
- query performance metrics
- connection pool metrics
- locks
- deadlocks
- slow queries
- database health

For example:
- get_database_health()
- get_slow_queries()
- get_connection_pool()
- get_locks()

AI could recieve:

```
Connection pool:
95/100

Waiting connections:
37

Slow queries:
+320%

Deadlocks:
0
```

It could then hypothesize:

"Database connection exhaustion may be contributing to the latency increase."

But the AI would not have arbitrary SQL execution privileges.

# Error handling strategies 

## Standardized integration result

We should prevent every integration from handling errors differently.

```
public sealed interface ToolResult<T>
        permits ToolSuccess, ToolFailure {

    boolean isSuccess();
}

public record ToolSuccess<T>(
        T data
) implements ToolResult<T> {

    @Override
    public boolean isSuccess() {
        return true;
    }
}

public record ToolFailure<T>(
        String code,
        String message,
        boolean retryable
) implements ToolResult<T> {

    @Override
    public boolean isSuccess() {
        return false;
    }
}
```

Now every integration can return either: SUCCESS or FAILURE without throwing infrastructure details into the AI reasoning layer.

## AWS integration with timeout and retry

CloudWatch might temporarily fail so we should never allow an AWS API call to hang the entire investigation.

```
@Service
public class CloudWatchService {

    private final CloudWatchClient cloudWatch;

    public ToolResult<MetricSummary> getMetrics(
            Incident incident) {

        try {

            GetMetricDataRequest request =
                    buildRequest(incident);

            GetMetricDataResponse response =
                    cloudWatch.getMetricData(request);

            return new ToolSuccess<>(
                    summarizeMetrics(response)
            );

        } catch (CloudWatchException e) {

            // AWS can return throttling or temporary service errors.
            if (isRetryable(e)) {

                return new ToolFailure<>(
                    "AWS_TEMPORARY_ERROR",
                    "CloudWatch temporarily unavailable",
                    true
                );
            }

            return new ToolFailure<>(
                "AWS_REQUEST_FAILED",
                "Unable to retrieve CloudWatch metrics",
                false
            );

        } catch (SdkClientException e) {

            // Network timeout, DNS problem, connection failure, etc.
            return new ToolFailure<>(
                "AWS_NETWORK_ERROR",
                "Unable to connect to CloudWatch",
                true
            );
        }
    }
}
```

The orchestrator can then decide what to do.

## Retry with exponential backoff

For transient failures we could use:

```
public <T> T executeWithRetry(
        Supplier<T> operation) {

    int maxAttempts = 3;

    for (int attempt = 1;
         attempt <= maxAttempts;
         attempt++) {

        try {
            return operation.get();

        } catch (TransientException e) {

            if (attempt == maxAttempts) {
                throw e;
            }

            // 100ms → 200ms → 400ms
            long delay =
                100L * (long) Math.pow(2, attempt - 1);

            sleep(delay);
        }
    }

    throw new IllegalStateException(
        "Unexpected retry state"
    );
}
```

## Retry strategy

We can classify errors that should be retryable such as:
- Network timeout
- HTTP 503
- AWS throttling
- Temporary Kafka failure

and avoid retrying in cases like:
- HTTP 400
- Invalid credentials
- Permission denied
- Invalid query
- Malformed request

For example:

```
private boolean isRetryable(AwsException e) {

    return e.statusCode() == 429
        || e.statusCode() == 502
        || e.statusCode() == 503
        || e.statusCode() == 504;
}
```

## Circuit breaker

In case CloudWatch is completely unavailable we could use a circuit breaker.

```
             ┌─────────────┐
             │   CLOSED    │
             │ normal      │
             └──────┬──────┘
                    │
             failures increase
                    │
                    ▼
             ┌─────────────┐
             │    OPEN     │
             │ fail fast   │
             └──────┬──────┘
                    │
                timeout
                    │
                    ▼
             ┌─────────────┐
             │ HALF-OPEN   │
             │ test request│
             └─────────────┘
```

Code example:
```
if (circuitBreaker.isOpen()) {

    return new ToolFailure<>(
        "CIRCUIT_OPEN",
        "CloudWatch integration temporarily disabled",
        false
    );
}

try {

    return callCloudWatch();

} catch (TransientException e) {

    circuitBreaker.recordFailure();

    throw e;
}
```

The AI investigation should continue using whatever information is available.

## Graceful degradation

Suppose some services are not available. The system could return:

```
Investigation completed with partial data.

Available:
✓ Application logs
✓ Kafka context
✓ Deployment information
✓ Historical incidents

Unavailable:
✗ CloudWatch metrics
✗ Database metrics

Confidence: MEDIUM

Recommendation:
Validate infrastructure metrics manually before
performing remediation.
```

Implementation:
```
public InvestigationContext collectContext(
        Incident incident) {

    var logs =
        safely(() -> logService.findRelevantLogs(incident));

    var metrics =
        safely(() -> cloudWatch.getMetrics(incident));

    var kafka =
        safely(() -> kafkaService.getContext(incident));

    var deployment =
        safely(() -> deploymentService.getDeployment(incident));

    return new InvestigationContext(
        logs,
        metrics,
        kafka,
        deployment
    );
}
```

## Safe execution wrapper example
```
private <T> Optional<T> safely(
        Supplier<T> operation) {

    try {

        return Optional.ofNullable(
            operation.get()
        );

    } catch (Exception e) {

        // Record the integration failure.
        metrics.increment(
            "ai.integration.failure"
        );

        logger.warn(
            "Integration failed",
            e
        );

        // Don't fail the entire investigation.
        return Optional.empty();
    }
}
```

The key behavior is: Integration failure -> Log it -> Measure it -> Return partial result -> Continue investigation

## Kafka error handling

Kafka could have some problems such as:
- consumer crashes
- malformed message
- duplicate message
- downstream AI service unavailable
- processing timeout


### Dead Letter Topic Strategy

```
incident.created
       │
       ▼
AI Consumer
       │
       ├── success ──────────→ processing complete
       │
       └── failure
             │
             ▼
       Retry Topic
             │
        max retries?
          /       \
        no         yes
        │           │
        ▼           ▼
      retry       DLQ
```

```
@KafkaListener(
    topics = "incident.created",
    groupId = "ai-investigation"
)
public void consume(
        ConsumerRecord<String, IncidentCreatedEvent> record) {

    try {

        // Idempotency check prevents duplicate investigations.
        if (investigationRepository.exists(
                record.key())) {

            return;
        }

        orchestrator.startInvestigation(
            record.value()
        );

        investigationRepository.markProcessed(
            record.key()
        );

    } catch (TransientException e) {

        // Kafka infrastructure / temporary dependency problem.
        throw e;

    } catch (InvalidIncidentException e) {

        // Bad input should not be retried indefinitely.
        dlqPublisher.publish(record, e);
    }
}
```

### Idempotency

This is especially important with Kafka because messages can be delivered more than once.

Without idempotency: incident.created -> AI investigation -> report generated -> incident.created -> AI investigation AGAIN -> second report

Instead we can use:
```
if (investigationRepository.exists(incidentId)) {
    return; // Already processed
}

investigationRepository.create(incidentId);

investigate(incident);
```

For production we could use a database constraint as an additional guarantee:
```
CREATE UNIQUE INDEX
idx_investigation_incident
ON investigations(incident_id);
```

## LLM failure

The LLM itself can fail.

Possible problems:
- timeout
- rate limit
- provider unavailable
- invalid response
- malformed JSON
- hallucinated fields
- context too large

### LLM call Isolation
```
def analyze(context):

    try:

        response = llm.generate(
            context=context,
            timeout=10
        )

        return validate_response(response)

    except TimeoutError:

        return InvestigationFailure(
            code="LLM_TIMEOUT"
        )

    except RateLimitError:

        return InvestigationFailure(
            code="LLM_RATE_LIMIT"
        )

    except InvalidResponseError:

        return InvestigationFailure(
            code="LLM_INVALID_RESPONSE"
        )
```

### Structured output validation

```
def validate_response(response):

    result = parse_json(response)

    # Validate schema before using the result.
    if not result.get("hypotheses"):
        raise InvalidResponseError()

    for hypothesis in result["hypotheses"]:

        if "cause" not in hypothesis:
            raise InvalidResponseError()

        if "evidence" not in hypothesis:
            raise InvalidResponseError()

        confidence = hypothesis.get("confidence")

        if not 0 <= confidence <= 1:
            raise InvalidResponseError()

    return result
```

This creates: LLM -> Schema validation -> Business validation -> Application

### Hallucination protection

We can implement an evidence requirement.

AI shouldn't be allowed to produce: 
```
Root cause:
Database failure
```
unless it can provide evidence.

The output contract could require:
```
{
  "cause": "Database connection exhaustion",
  "confidence": 0.78,
  "evidence": [
    {
      "source": "RDS",
      "observation": "98% connection utilization"
    },
    {
      "source": "OpenSearch",
      "observation": "Connection timeout errors"
    }
  ]
}
```

### Confidence calculation example

We can calculate an evidence score. For example:

```
def calculate_confidence(hypothesis, evidence):

    score = 0

    if evidence.deployment_correlation:
        score += 0.25

    if evidence.log_correlation:
        score += 0.25

    if evidence.metric_correlation:
        score += 0.20

    if evidence.historical_match:
        score += 0.20

    if evidence.code_change:
        score += 0.10

    return min(score, 1.0)
```

### Timeouts and bounded investigations

```
class InvestigationBudget:

    max_duration = 60
    max_tool_calls = 20

    def allow_tool_call(self):

        if self.tool_calls >= self.max_tool_calls:
            return False

        self.tool_calls += 1
        return True
```
This prevents runaway agent behavior and uncontrolled costs.

### Rate limiting

If too many incidents arrive simultaneously the LLM provider could be overwhelmed.
We could introduce a queue and concurrency limit.
```
Kafka
  ↓
Investigation Queue
  ↓
Worker Pool
  │
  ├── Worker 1
  ├── Worker 2
  ├── Worker 3
  └── Worker N
```

For example:
```
ExecutorService executor =
    Executors.newFixedThreadPool(10);
```

This means we can control maximum concurrent investigations rather than allowing unlimited AI calls.

### Fallback model strategy

If the AI provider supports multiple models, we could implement:
Primary model failure -> Secondary model failure -> Deterministic investigation summary

being careful with model fallback because different models may produce different quality.

Code example:
```
try:
    return primary_model.analyze(context)

except ProviderUnavailable:

    return fallback_model.analyze(context)

except Exception:

    return deterministic_summary(context)
```

The deterministic fallback might produce:

```
Incident summary

Errors:
SocketTimeoutException: 7,312

Deployment:
v4.18 at 14:27

Error spike:
14:32

Historical incident:
INC-3912

AI analysis unavailable.
Manual investigation recommended.
```

instead of failing the entire incident workflow.

### Observability of the AI platform

The AI system itself needs observability.
We can use seom metrics like:
- ai_investigation_duration
- ai_tool_call_duration
- ai_tool_failures
- ai_llm_latency
- ai_llm_errors
- ai_llm_tokens
- ai_investigation_success_rate
- ai_partial_investigation_rate
- ai_hallucination_rate
- ai_recommendation_acceptance_rate

This lets the FDE team determine whether the AI system itself is becoming a reliability problem.

