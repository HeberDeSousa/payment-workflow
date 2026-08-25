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
