# Instructions for Running Docker and Spark Commands

This document provides the necessary steps to build Docker containers and run the `spark-submit` command in a Dockerized environment.

## Prerequisites

- Docker and Docker Compose installed on your machine.
- Access to the project source code and the `Makefile` with the relevant commands.

## Step 1: Build and Start the Containers

The first step is to build and start the containers required for the project. To do this, run the following command in your terminal:

```bash
make down && docker-compose up --build
```

## Step 2: Run Any Application

Once the containers are up and running, you can execute any application you have created within the Spark container using the `make submit` command. To do this, run the following command in your terminal:

```bash
make submit app=<path_to_your_app>
```
