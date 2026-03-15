# Azure Functions Python 3.11 base image (Functions Runtime v4)
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Functions host expects app code here
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

# Install dependencies first (layer cache)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application code
COPY . /home/site/wwwroot
