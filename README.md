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


# AI deployment strategy

The goal is to get the highest value × feasibility × risk profile.


Investigation phase is potentially the biggest business opportunity, but not necessarily the best first AI deployment opportunity. 
Based on classification the strongest initial candidates are:


- Search historical incidents — very high value, low risk.
- Correlate logs — high value, medium risk.
- Generate incident reports — high value, low risk.
- Generate RCA draft — high value, medium risk.

