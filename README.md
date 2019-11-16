# Client Billable Event Sync

Execute this jar to write a CSV queried from the `default.client_billable_events` table to the `realtime-expiration-service-LifetimeBudgetMetrics` 
DynamoDB table.

## Prerequisites

* Get Qubole access from DevTools team
* Get AWS credentials with write permissions for DynamoDB `realtime-expiration-service-LifetimeBudgetMetrics` in the `K8s` account
* [Install scala 2.12.10](https://www.scala-lang.org/download/). Java version must be 1.8

## Usage

1. Check `s3` source for client billable events table. Wait for a new partition to be added, and then scale down the 
`realtime-expiration-service` deployment in Kubernetes to 0 pods. 
    
    Protip: to find `s3` location, execute in Qubole and scroll to the bottom of the results:
    ```sql
    SHOW CREATE TABLE `default.client_billable_events`;
    ```

2. Download results from this Qubole query against `default.client_billable_events` in a CSV:
    ```sql
    SELECT campaign_metadata_contract_uri_id                                          as contract_id,
           campaign_metadata_campaign_uri_id                                          as campaign_id,
           MAX(CAST(to_unixtime(CAST(event_header_event_at_timestamp AS timestamp)) AS BIGINT)) as ts,
           MAX(CAST(to_unixtime(CAST(event_header_event_at_timestamp AS timestamp) + interval '1' year) AS BIGINT)) as ttl,
           MIN(CAST(to_unixtime(CAST(event_header_event_at_timestamp AS timestamp)) AS BIGINT)) as first_activity,
           SUM(
                   CASE
                       WHEN billable_entity_account_uri_id != '41' AND event_mode = 'ASSERT' THEN billable_amount_usd_micros
                       WHEN billable_entity_account_uri_id != '41' AND event_mode = 'REVERT'
                           THEN -1 * billable_amount_usd_micros
                       ELSE 0
                       END
               )                                                                      as micros,
           SUM(
                   CASE
                       WHEN billable_entity_account_uri_id != '41' AND event_mode = 'ASSERT' THEN 1
                       WHEN billable_entity_account_uri_id != '41' AND event_mode = 'REVERT' THEN -1
                       ELSE 0
                       END
               )                                                                      as redemptions,
           SUM(
                   CASE
                       WHEN billable_entity_account_uri_id = '41' AND event_mode = 'ASSERT' THEN billable_amount_usd_micros
                       WHEN billable_entity_account_uri_id = '41' AND event_mode = 'REVERT'
                           THEN -1 * billable_amount_usd_micros
                       ELSE 0
                       END
               )                                                                      as micros_untracked,
           SUM(
                   CASE
                       WHEN billable_entity_account_uri_id = '41' AND event_mode = 'ASSERT' THEN 1
                       WHEN billable_entity_account_uri_id = '41' AND event_mode = 'REVERT' THEN -1
                       ELSE 0
                       END
               )                                                                      as redemptions_untracked
    FROM default.client_billable_events
    GROUP BY 1, 2
    ORDER BY ts DESC;
    ```

3. In your terminal, log into AWS and assume the role with write permissions for `realtime-expiration-service-LifetimeBudgetMetrics`

4. Execute the `.jar` file for this project with the path to your CSV as a command line argument:
```commandline
$ java -jar <path/to>data-engineering-event-sync.jar <path/to/data.csv> 
```
