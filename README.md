# Terms of Service Watch (azure backend)

> Automated monitoring and analysis of major platform Terms of Service changes using Azure Durable Functions and Anthropic API

[![Azure Functions](https://img.shields.io/badge/Azure-Functions-blue)](https://azure.microsoft.com/en-us/services/functions/)
[![Python](https://img.shields.io/badge/Python-3.9+-green)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Overview

TOS Watch Azure monitors Terms of Service documents for changes, tracks these changes over time, and generates plain-English summaries of their
practical impact on user experience and rights.

## Key Features

- **Automated Scraping**: Monitors live websites for TOS changes (back-filled historical changes from Wayback Machine). 
- **Semantic Parsing**: Generates hierarchical diffs to contextualize changes and manage context window size.
- **AI Summarization**: Uses Claude API to sift through boilerplate legal language and deliver the important notes.
- **Prompt Engineering**: Iterates on new prompting strategies and integrates failures to improve summarization accuracy. 
- **Scalable Architecture**: Built on Azure Durable Functions for efficient orchestration and scaling.
- **Rate Limiting & Circuit Breaking**: Implements resilience patterns to handle failures and rate limits.
- **Blob Storage Pipeline**: Staged processing pipeline using Azure Blob Storage for input and output.

## Architecture

![Architecture Diagram](architecture-diagram.png)

### Pipeline Stages

1. **Stage 01 - Back-fill**: Scrapes snapshots from the Wayback Machine for each URL.
2. **Stage 02 - Snapshots**: Downloads HTML snapshots of live TOS pages.
3. **Stage 03 - Parsing**: Splits HTML into semantically cohesive sections for analysis.
5. **Stage 04 - Diffs**: Generates diffs between document versions to highlight changes.
7. **Stage 05 - Summaries**: Generates AI-powered summaries of the changes. 
7. **Stage 06 - Evaluation**: Human-in-the-loop rating of summarization accuracy.
7. **Stage 07 - Prompting**: Experiment with new prompting strategies

### Orchestration Workflows

- **Rate Limiting**: Manages task-level fan-out that hit a common resource (e.g. Wayback Machine API)
- **Circuit Breaker**: Communicates task-correlated errors to avoid overwhelming service with broken requests.

## Experimentation & Evaluation

The main goal is to classify ToS changes as "substantive" or not. Sub-goals are to classify the changes into specific topics (e.g. [HELM AIR 2024](https://arxiv.org/abs/2406.17864)). 

### Structured outputs

The model is instructed to emit JSON which is later extracted from the raw text response and validated according to a schema. Schemas are immutable and written into the metadata of each run, so that past inferences can be replayed as the schemas evolve.

### Prompt Engineering

Currently using in-context learning: known false positive input-output pairs are provided to the model before submitting the real prompt. In testing, using as few as 2-3 examples has improved 
precision from a baseline of 59% up to 80%.

### Gold Labels

The goal of this project is to highlight ToS changes that materially affect user experience, data rights, etc. I use a rubric of substantive and non-substantive edits to assign labels from a lay-user perspective. Using a sample size of 50 (20% of corpus). Experiments are run against this set (except for the few used as ICL examples).

**Roadmap:**

- Concretize rubric and decrease task ambiguity with user-driven stories and real-life cases of legal battles, data breaches, etc.

### Metrics

Models are evaluated on binary classification F1 score, as currently the highest driver of negativ user experience is when the website mistakenly shows non-substantive changes.

**Roadmap**:

- Empirically verify that answers are factually grounded in input documents. Censor un-verified answers.
- Re-implement topic modeling as separate prompt flow.

### Versioning and Reproducibility

See my opinionated write-up [here](https://eric-mc2.github.io/).

## Installation

### Prerequisites

- Azure subscription with:
  - Azure Functions
  - Azure Storage Account
- Azure Functions Core Tools
- Python 3.12

### Setup

```bash
# Clone the repository
git clone [repository-url]
cd tos-watch-az

# Install dependencies
pip install -r requirements.txt

pip install git+https://github.com/microsoft/python-type-stubs.git 

# Set the following environment variables in the shell or a .env file:
AzureWebJobsStorage="your-connection-string"
WEBSITE_HOSTNAME="your azure functions url"
ANTHROPIC_API_KEY="your api key"
AZURE_FUNCTION_MASTER_KEY="your api key"
ARGILLA_API_KEY="your api key"
HF_TOKEN="your api key"
```
## Development

### Local Development

```bash
# Start azurite service in VSCode
# ... business logic will not use it but orchestration services still need a full-feature table service
open vscode and use Azurite: Start command

# Start the Azure Functions runtime locally
task dev

# Run tests
pytest

# Deploy to Azure
func azure functionapp publish [function-app-name]
```

### Staging environment

```bash
# Start azurite service in VSCode
open vscode and use Azurite: Start command

# Start the Azure Functions runtime locally
task stage
```

## Usage

### Seeding URLs

```bash
# Trigger the seed_urls function
curl -X POST https://[your-function-app].azurewebsites.net/api/seed_urls?code=[function-key]

### Monitoring Changes

Audit the 'traces' logs in the Azure Functions App portal.

### Creating Gold Labels

Follow these [instructions](https://docs.argilla.io/latest/getting_started/quickstart/) to create an Argilla instance on HuggingFace.

Run command to seed the instance with random examples for labeling.

```bash
python labeling.py --action add
```

**Roadmap:**

[x] Run experiments from specific label sets
[ ] Implement different sampling strategies to ensure coverage

Label the examples in the Argilla UI. Then download with: 

```bash
python labeling.py --action download
```

### Running Experiments

It's best to do this in local dev instead of production... 
Write your new prompt in `src/summarizer.py`.
Write new immutable versioned Pydantic schema in `schemas/summary/v##.py`. 

```bash
curl -X POST https://[your-function-app].azurewebsites.net/api/prompt_experiment?labels=[label-list]&code=[function-key]
```

Evaluate (structural correctness, accuracy, precision, recall) against past prompt and schema versions:

```bash
curl -X POST https://[your-function-app].azurewebsites.net/api/evaluate_prompts?code=[function-key]
```

Commit and push the new version IF you want it to run in produciton.

### Checking System Health

```bash
# Check running / pending tasks
python health_checks.py --output [filename] tasks --env {DEV,PROD} --workflow_type [STAGE]

# Check missing blob outputs
python health_checks.py --output [filename] files

# Check circuit breaker status
curl https://[your-function-app].azurewebsites.net/api/check_circuit_breaker?code=[function-key]

# Reset a circuit breaker
curl -X POST https://[your-function-app].azurewebsites.net/api/reset_circuit_breaker?workflow_type=[type]&code=[function-key]

# Kill running / paused tasks in a given pipeline stage
python health_checks.py --output [filename] killall --env {DEV,PROD} --workflow_type [STAGE]
```