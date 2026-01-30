# Kubernetes Deployment Guide

This guide describes how to deploy the JanusGraph CDC Extension on a Kubernetes cluster.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed.
- A running Kubernetes cluster (e.g., Minikube, Kind, or a cloud provider).
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured to communicate with your cluster.

## 1. Build the Docker Image

First, you need to build the Docker image for the JanusGraph CDC extension.

```bash
mvn clean package
docker build -t janusgraph-cdc-extension:latest .
```

*Note: If you are using Minikube, remember to point your shell to Minikube's docker-daemon (`eval $(minikube -p minikube docker-env)`) or load the image (`minikube image load janusgraph-cdc-extension:latest`).*

## 2. Prepare Configuration (Optional)

The sample manifests in the `k8s/` directory use a ConfigMap to inject the Log4j2 configuration. This allows you to modify logging settings without rebuilding the image.

The configuration files `log4j2-server.xml` and `cdc-log4j.properties` are located in `src/main/resources`.

## 3. Deploy to Kubernetes

Apply the manifests located in the `k8s/` directory.

```bash
# Create the ConfigMap
kubectl apply -f k8s/configmap.yaml

# Create the Service
kubectl apply -f k8s/service.yaml

# Create the Deployment
kubectl apply -f k8s/deployment.yaml
```

## 4. Verify Deployment

Check the status of the pods:

```bash
kubectl get pods
```

View the logs to verify the CDC appender is working correctly:

```bash
kubectl logs -f <pod-name>
```

You should see startup logs and no errors related to "CDC_LOG". The CDC events will be written to `/tmp/cdc-events.log` inside the container (or the path customized via `JANUSGRAPH_LOG_DIR`).

To check the CDC event log file inside the pod:

```bash
kubectl exec -it <pod-name> -- cat /tmp/cdc-events.log
```
