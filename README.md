# ATOM to ArchivesSpace Sync Tool

## Overview
This application is designed to synchronize archival descriptions and authority files between Access to Memory (ATOM) and ArchivesSpace. It automates the process of adding or updating thousands of descriptions, subjects, and named agents, and links them to the appropriate resources (descriptions). The synchronization runs weekly, ensuring that data remains consistent and up-to-date across systems.

---

## Developer Guide

### Architecture
The application is built using Python and Docker, with the following key components:
- **Python Scripts**: The core logic for transforming and syncing data resides in the `src` directory.
- **Docker**: The application is containerized for consistent deployment and execution across environments.
- **Supervisor**: Manages the execution of the Python script on a weekly schedule.

### Key Features
1. **Data Transformation**: Converts ATOM records into ArchivesSpace-compatible JSON using custom mapping logic.
2. **Weekly Execution**: Runs automatically once a week using a cron-like loop implemented in the Supervisor configuration.

---

### Setup Instructions

#### Prerequisites
- Docker or Podman installed on your system.
- Access to the ATOM and ArchivesSpace APIs.

#### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/citz-gim-archivesspace-atom-sync.git
   cd citz-gim-archivesspace-atom-sync
   ```

2. Build and start the Docker container:
   ```bash
   npm run up
   ```

3. Ensure the `.env` file is configured with the necessary API credentials and environment variables. You can reference the `.env.template` file to build the `.env`.

---

### File Structure
- **`src/mapping.py`**: Contains the core logic for transforming ATOM records into ArchivesSpace-compatible JSON.
- **`src/main.py`**: Serves as the entry point for the application, orchestrating the synchronization process.
- **`src/atom_helpers.py`**: Provides helper functions for interacting with the ATOM API.
- **`src/cache.py`**: Implements caching mechanisms to optimize data processing.
- **`src/state_manager.py`**: Manages the application's state, including tracking progress and handling retries.
- **`src/updater.py`**: Handles the logic for updating records in ArchivesSpace.
- **`src/state.json`**: Stores the application's state in JSON format for persistence.
- **`Dockerfile`**: Defines the container environment, including dependencies and configurations.
- **`supervisord.conf`**: Configures the Supervisor to run the Python script on a weekly schedule.
- **`compose.yml`**: Used locally to easily manage the container.
- **`.env`**: Stores environment variables such as API credentials.

---

### Troubleshooting
- **Logs**: Check logs in `/app/log.txt` inside the container for debugging.
- **Certificate Issues**: Ensure `atom.crt` is correctly configured and added to the trusted CA store.

---

### Retry and Rate Limiting
- **Retry Logic**: The application includes mechanisms to handle transient errors by retrying failed API calls with exponential backoff.
- **Rate Limiting**: Ensures compliance with API usage policies by throttling requests to avoid exceeding rate limits.
