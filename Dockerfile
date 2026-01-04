FROM apache/airflow:2.9.3

# Copy requirements.txt into the image
COPY requirements.txt .

# Install Python packages the Airflow-approved way
USER airflow
ENV AIRFLOW_PIP_REQUIREMENTS_FILE=requirements.txt

# Use entrypoint installation
RUN pip install --no-cache-dir -r requirements.txt
