# Wayback Snapshot Scraper - Azure Functions

This Azure Functions project contains a scraper that collects historical snapshots of web pages from the Wayback Machine and stores them in Azure Blob Storage.

## Functions

### `wayback_scraper`

HTTP-triggered function that:
1. Loads a list of URLs from an input blob (JSON format)
2. For each URL, queries the Wayback Machine API for available snapshots
3. Downloads a sample of snapshots (up to 10 evenly distributed over time)
4. Extracts main content from each snapshot
5. Stores the cleaned HTML in Azure Blob Storage

## Setup

### Prerequisites

- Azure Functions Core Tools
- Python 3.8+
- Azure Storage Account

### Environment Variables

Set the following environment variable in your `local.settings.json` file:

```json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "your_azure_storage_connection_string",
  }
}
```

### Dependencies

The function requires these Python packages (installed via `requirements.txt`):
- azure-functions
- azure-storage-blob
- requests
- pandas
- beautifulsoup4

## Usage

### Input Format

Upload a JSON file to your input container with the following structure:

```json
{
  "company1": [
    "https://example1.com/page1",
    "https://example1.com/page2"
  ],
  "company2": [
    "https://example2.com/page1"
  ]
}
```

### API Endpoints

**POST/GET** `/api/scraper`

Parameters (query string or JSON body):
- `input_container` (optional, default: "input"): Name of the blob container containing the URLs file
- `input_blob` (optional, default: "static_urls.json"): Name of the blob containing the URLs
- `output_container` (optional, default: "wayback-snapshots"): Name of the container to store snapshots

### Example Requests

```bash
# Using default parameters
curl -X POST "https://your-function-app.azurewebsites.net/api/scraper?code=your-function-key"

# With custom parameters
curl -X POST "https://your-function-app.azurewebsites.net/api/scraper?code=your-function-key&input_container=my-urls&input_blob=urls.json&output_container=snapshots"

# With JSON body
curl -X POST "https://your-function-app.azurewebsites.net/api/scraper?code=your-function-key" \
  -H "Content-Type: application/json" \
  -d '{"input_container": "my-urls", "input_blob": "urls.json", "output_container": "snapshots"}'
```

### Output Structure

Snapshots are stored in the output container with the following structure:
```
company/url_path/timestamp.html
```

For example:
- `google/policies/20220315123456.html`
- `meta/terms/20220320654321.html`

## Local Development

1. Install dependencies:
   ```bash
   cd cloud-service/az-funcs
   pip install -r requirements.txt
   ```

2. Set up local settings:
   ```bash
   cp local.settings.json.template local.settings.json
   # Edit local.settings.json with your Azure Storage connection string
   ```

3. Run locally:
   ```bash
   func start
   ```

## Deployment

Deploy to Azure using the Azure Functions Core Tools:

```bash
func azure functionapp publish your-function-app-name
```

Make sure to set the `AzureWebJobsStorage` application setting in your Azure Function App configuration.