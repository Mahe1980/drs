# Neuron Data Retention Service

Contains source code for cloud functions to be deployed in Data Retention Service.

## Source

The `generate_report` folder contains source code to generate report for DRS.

The `send_report` folder contains source code to send the report on a slack channel.

The `trash` folder contains source code to trash data from BigQuery and GCS.

The `purge` folder contains source code to purge data from BigQuery and GCS.

The `tests` folder contains unit tests for the above components.


### Setting up the virtual environment

1. Update `PYTHONPATH` to include current project

       export PYTHONPATH=${PYTHONPATH}:<Project Root Directory>

2. Ensure that Python version 3.6+ is installed on the system.

3. Create a new virtual env specific to this project e.g.

        python -m venv  .venv

   where `.venv` is the folder the virtual environment is configured in.  `.venv` is only an example, it can be a path as well.

4. Activate the virtual environment e.g.

        source .venv/bin/activate

5. Navigate to project root directory and run the below command to install necessary python modules

        pip install -r requirements.txt

### Test
1) From the project root directory, run `python -m pytest tests`


### Execution (Standalone Components)
1) Update `config.yaml` store it in a GCS bucket

2) Generate Report: Navigate to `generate_report` and run `python main.py --config_file_path <Config Path in GCS>`

3) Send Report: Navigate to `send_report` and run `python main.py --config_file_path <Config Path in GCS> --report_file_path <Report Path in GCS>`

4) Trash : Navigate to `trash` and run `python main.py --config_file_path <Config Path in GCS> --report_file_path <Report Path in GCS>`

5) Purge : Navigate to `purge` and run `python main.py --config_file_path <Config Path in GCS> --report_file_path <Report Path in GCS>`

