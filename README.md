# GD Challenge: Instructions for Running Spark Cluster and Spark Commands within Docker

## Prerequisites

- Docker and Docker Compose installed on your machine.

## Build and Start the Containers

The first step is to build and start the containers required for the project. To do this, run the following command in your terminal:

```bash
make down && docker-compose up --build
```

## Run the SMS Billing Calculation (Point 1)

Once the containers are up and running, you can execute the application to calculate the total SMS billing as described in Point 1. To do this, run the following command in your terminal:

```bash
make submit app=src/events_sms.py
```
**The total SMS revenue is: $391367.00**

## Run the Top Users Calculation (Point 2)

After calculating the total SMS billing in Point 1, the next step is to generate a dataset containing the 100 users with the highest SMS billing amounts, along with their hashed IDs and the total amount billed to each.

To execute this, run the following command in your terminal:

```bash
make submit app=src/top_users.py
```

![Top User Dataset](images/top_users_dataset.png)

## Visualizing the Histogram of Calls Per Hour (Point 3)

After executing the required scripts, you can visualize the histogram generated in Point 3.

To execute this, run the following command in your terminal:

```bash
make submit app=src/calls_per_hour_histogram.py
```

Here is the generated histogram:

![Calls per Hour Histogram](images/calls_per_hour_histogram.png)
