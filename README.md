# Exercise 1 - Spark + Docker

## Prerequisites

- Docker and Docker Compose installed on your machine.

## Build and Start the Containers

The first step is to build and start the necessary containers for the project. To do this, run the following command in your terminal:

```bash
make down && docker-compose up --build
```

## Run the SMS Billing Calculation (Point 1)

Once the containers are running, you can execute the application to calculate the total SMS billing, as described in Point 1. To do this, run the following command in your terminal:

```bash
make submit app=src/events_sms.py
```
**The total SMS billing amount is: $391,367.00**

## Run the Top Users Calculation (Point 2)

After calculating the total SMS billing in Point 1, the next step is to generate a dataset containing the top 100 users with the highest SMS billing amounts, along with their hashed IDs and the total amount billed to each.

To execute this, use the following command in your terminal:

```bash
make submit app=src/top_users.py
```

![Top Users Dataset](images/top_users_dataset.png)

## Visualize the Calls Per Hour Histogram (Point 3)

To execute it, use the following command in your terminal:

```bash
make submit app=src/calls_per_hour_histogram.py
```

Here is the generated histogram:

![Calls Per Hour Histogram](images/calls_per_hour_histogram.png)

# Exercise 2 - General Questions

## Question 1: Process Prioritization and Management in a Hadoop Cluster

**a) How would you prioritize production processes over exploratory analysis processes?**

- Configure the Hadoop cluster with a resource queue system. Queues associated with production processes would receive a higher priority (a higher percentage of resources), while queues for exploratory analysis would have a lower resource limit.

- Define clear SLAs that establish guaranteed completion times for production processes. Exploratory analysis jobs would run only when production resources are not saturated.

**b) Strategy for managing CPU and memory-intensive processes during the day:**

- Schedule production processes during low-demand hours (e.g., at night or early in the morning). This reduces competition with other cluster processes.

- Set strict quotas for each team/process, ensuring that production processes have guaranteed resources.

**Scheduling tools:**

- Apache Airflow
- Prefect
- Dagster

## Question 2: High Transactional Data Lake Table

### Possible Causes of the Problem:

- The table may contain too many small files, affecting query performance due to metadata read overhead.

- If the data is not properly partitioned, queries scan more data than necessary.

- Lack of secondary indexes or updated statistics in the Data Lake to accelerate queries.

### Suggested Solutions:

- Ensure the table is partitioned by **low cardinality** fields that are frequently used in queries (such as relevant IDs or categories). This allows for **partition pruning**, improving query performance by reducing the number of partitions that need to be scanned.

- For Delta tables, run the `OPTIMIZE` command to consolidate small files and improve query performance. Additionally, applying **Z-Ordering** on columns frequently used in query filters can significantly enhance performance by organizing data on disk to minimize lookup costs.

- If possible, separate the transactional and query workloads into different layers of the Data Lake (raw, curated, data_product). This separation helps reduce workload contention since queries will not directly impact transactional operations and vice versa. Additionally, it optimizes performance by allowing greater specialization and scalability at each layer.

## Question 3: Configurations to Reserve Half of the Cluster in Spark

### Configuring Resources in SparkSession:

A Hadoop cluster with 3 nodes, 50 GB of memory, and 12 cores per node (150 GB and 36 cores total).

- Total assigned cores = 18 cores (50% of the cluster).
- Total assigned memory = 75 GB (50% of available memory).

```python
SparkSession.builder \
    .appName("SparkJob") \
    .config("spark.executor.instances", 6) \  # Total cores 18 / Total nodes 3
    .config("spark.executor.cores", 3) \     # Total cores 18 / Total executors 6
    .config("spark.executor.memory", "12g") \ # Total memory 75 / Total executors 6
    .config("spark.dynamicAllocation.enabled", "false") \  # Set resources as fixed, not dynamic
    .getOrCreate()
```

- **Number of executors (`spark.executor.instances`)**: 6 executors are assigned, utilizing all 18 assigned cores (3 cores per executor).

- **Cores per executor (`spark.executor.cores`)**: Each executor is assigned 3 cores (ensuring balanced workload distribution among executors without overloading any).

- **Memory per executor (`spark.executor.memory`)**: Each executor is assigned 12 GB of memory (effectively utilizing the allocated 75 GB of memory).

### Limiting Resources via YARN Queue Management

If **YARN** is used as the resource manager in your Hadoop cluster, you can configure queues with specific limits for different types of jobs, such as production and exploratory jobs. This helps efficiently manage resources and ensures that critical jobs are not interrupted by non-production processes.

Key configurations include:

- **memory-mb**: Defines the maximum memory that can be allocated to jobs in a specific queue. Setting a memory limit prevents a single job from consuming all available resources, ensuring that enough resources remain for other jobs.

- **vcores**: Specifies the maximum number of virtual cores that can be assigned to jobs within a queue. This is crucial for preventing node overload and maintaining a balanced use of processing resources.

Properly configuring YARN queues allows workloads to be separated by priority, ensuring that production jobs are not affected by exploratory or test processes.

