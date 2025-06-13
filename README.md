# JobInsight Data Pipeline

## Description
JobInsight Data Pipeline is a robust framework designed to streamline the process of collecting, transforming, and analyzing job market data. It integrates seamlessly with Apache Airflow for workflow management, Docker for containerization, and utilizes SQL for data operations.

## Key Features
- **Automated Data Collection**: Efficiently gathers data from multiple job market sources using custom crawlers.
- **Scalable ETL Processes**: Transforms raw data into structured formats suitable for analysis and reporting.
- **Data Warehousing**: Organizes and stores processed data in a data warehouse for easy access and analysis.
- **Workflow Orchestration**: Leverages Airflow to automate and manage complex data workflows.
- **Comprehensive Testing**: Includes a suite of tests to ensure data accuracy and pipeline reliability.

## Installation Instructions
1. **Clone the repository**:
   ```bash
   git clone https://github.com/Trantuan24/JobInsight_Data_Pipeline.git
   ```
2. **Navigate to the project directory**:
   ```bash
   cd JobInsight_Data_Pipeline
   ```
3. **Set up the environment**:
   - Copy `env.example` to `.env` and configure the necessary environment variables.
4. **Build and start the Docker containers**:
   ```bash
   docker-compose up --build
   ```

## Usage Guide
- **Access Airflow**: Open the Airflow web interface at `http://localhost:8080` to monitor and manage your data pipelines.
- **Trigger Workflows**: Use the Airflow UI to manually trigger DAGs or let them run on a predefined schedule.

## Project Structure
- **src/**: Contains the core source code for data processing and ETL operations.
- **sql/**: Houses SQL scripts for database schema creation and data manipulation.
- **dags/**: Includes Airflow DAGs that define the data workflows.
- **tests/**: Contains test cases to validate the functionality and reliability of the pipeline.
- **data/**: Directory for input and output data files.
- **logs/**: Stores logs generated during pipeline execution.

## Configuration Details
- **Environment Variables**: Configure your environment by editing the `.env` file.
- **Airflow Settings**: Adjust `airflow.cfg` to customize Airflow's behavior.

## Testing Instructions
Run the test suite to verify the pipeline's functionality:
```bash
pytest tests/
```

## Contribution Guidelines
We welcome contributions! Please fork the repository and submit a pull request with your changes.

## License Information
This project is licensed under the MIT License. See the LICENSE file for more details.
