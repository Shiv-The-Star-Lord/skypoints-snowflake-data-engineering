# Performance and Scalability

## 1. Current Development Environment

| Setting | Observed value |
|---|---|
| Database | SKYPOINTS_DE |
| Warehouse | COMPUTE_WH |
| Warehouse size | X-Small |
| Warehouse type | Standard |
| Multi-cluster | No (min/max cluster count: 1) |
| Auto-resume | Enabled |
| Auto-suspend | 300 seconds |

The current environment is a small development setup. It is suitable for functional validation but does not demonstrate production-scale throughput.

## 2. Current Data Volume

Observed table counts during the development validation:

| Table | Row count |
|---|---:|
| RAW.RAW_MEMBER_PROFILES | 20 |
| STG.STG_MEMBER_PROFILES | 15 |
| CORE.MEMBER_PROFILE | 15 |
| OPS.MEMBER_BATCH_CONTROL | 13 |
| RAW.RAW_REDEMPTIONS | 3 |
| STG.STG_REDEMPTION_TRANSACTIONS | 3 |
| CORE.REDEMPTION_TRANSACTIONS | 3 |

These counts represent the current test dataset, not production data volumes.

The difference between RAW and STG member counts is expected because staging applies deduplication and change detection. CORE maintains member history using SCD Type 2.

## 3. Observed Incremental Member Pipeline SQL Timings

The following successful DML statements were observed in Snowflake query history during an incremental member-processing run:

| Operation | Elapsed seconds |
|---|---:|
| Batch registration | 0.301 |
| Mark STG started | 0.260 |
| Incremental STG insert | 0.858 |
| Mark STG completed | 0.411 |
| Mark CORE started | 0.277 |
| Expire existing CORE versions | 0.269 |
| Insert CORE versions | 0.530 |
| Mark CORE completed | 0.274 |
| **Combined elapsed time** | **3.180** |

The combined value is the sum of the eight individual statement elapsed times. It is not the complete end-to-end pipeline duration and should not be interpreted as pure warehouse compute time.

The observed scans were very small because the development dataset contains only a few records. These measurements are a functional baseline, not a production performance benchmark.

## 4. Implemented Performance-Oriented Design

### Incremental member processing

- The processor identifies batches that are pending or failed rather than reprocessing every historical batch.
- STG transformation reads RAW records associated with eligible batches.
- Only changed member records are inserted into STG.
- CORE processing handles eligible batches and avoids inserting source records already represented in CORE.
- Batch statuses allow processing to resume after failures.

### Deduplication and change detection

- Source record hashes are used to identify meaningful changes.
- Consecutive unchanged member versions are suppressed.
- Redemption transactions are merged using their business keys.
- Repeated ingestion of an identical source file is detected using its checksum.

### Country-specific projections

- CORE retains the central SCD2 member history.
- Country-specific tables contain current-state projections.
- Country synchronization uses MERGE operations and removes members that no longer belong to a country.

### Data quality

- Mandatory fields and source data are validated.
- Duplicate and invalid records are checked.
- SCD2 validity and country-table consistency are checked.
- Redemption transaction uniqueness and member references are validated.

## 5. Scalability Considerations for Production

The assessment describes a workload that could reach billions of records daily. The current development environment has not been tested at that scale. The following measures should be evaluated before production deployment.

### Compute scaling

- Select warehouse size based on measured workload and latency requirements.
- Consider multi-cluster warehouses when concurrent workloads create queueing.
- Configure auto-suspend and auto-resume to balance cost and availability.
- Monitor warehouse credits, queue time, and query execution metrics.

### Data layout and query pruning

- Keep incremental transformations scoped to eligible batches.
- Evaluate clustering only after inspecting query profiles and micro-partition pruning.
- Consider clustering keys such as ingestion date or batch-related columns only when query patterns and data volume justify them.
- Avoid adding Search Optimization or clustering without measured evidence that they improve the relevant queries.

### Ingestion and orchestration

- Use appropriately sized micro-batches or other ingestion patterns based on latency and throughput requirements.
- Monitor ingestion failures, batch status, record counts, and processing duration.
- The current batch-control implementation assumes serialized processing. Concurrent workers require an explicit coordination or locking strategy to prevent duplicate batch registration and processing races.
- The current incremental ordering assumes chronologically arriving data. Late-arriving historical records need additional handling to preserve correct SCD2 timelines.

### Monitoring

Track at least:

- Source files and records received.
- Records inserted, updated, rejected, or skipped.
- Batch processing status and failure details.
- Query elapsed time and bytes scanned.
- Warehouse queue time and credit consumption.
- Data quality failures and duplicate counts.

## 6. Recommended Benchmark Plan

Before making production throughput claims, run controlled tests using representative synthetic data.

Suggested test volumes:

| Test | Approximate member records |
|---|---:|
| Small | 100,000 |
| Medium | 1,000,000 |
| Large | 10,000,000 |
| Extended | 100,000,000 or more, subject to available resources |

For each run, record:

1. Warehouse size and configuration.
2. Input record count and batch size.
3. Number of inserted, updated, and unchanged records.
4. STG and CORE processing duration.
5. Bytes and micro-partitions scanned, where available in Query Profile.
6. Warehouse credits consumed.
7. Data quality results and SCD2 correctness.

Repeat tests under consistent conditions and compare both runtime and cost. Increase volumes gradually and only use production-scale claims when supported by measured results.

## 7. Current Limitations

- The current dataset is too small to establish throughput at millions or billions of records.
- The observed timings are from individual SQL statements, not a controlled end-to-end benchmark.
- The current batch-control design assumes a serialized processing run.
- Late-arriving historical member records are not fully supported by the current chronological processing logic.
- Clustering, multi-cluster compute, and other advanced performance features have not been justified by measured workload evidence.

## 8. Conclusion

The project implements incremental ingestion, change detection, batch-level processing control, SCD2 history, and data quality validation. These provide a foundation for scaling, but production readiness at billions of records per day would require representative load testing, concurrency coordination, late-arriving data handling, and workload-driven Snowflake tuning.
