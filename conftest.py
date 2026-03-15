"""
Root conftest.py — seeds required environment variables BEFORE any app module
is imported.  app.config.Settings() is instantiated at module-import time via
`settings = Settings()`, so env vars must be present before the first
`from app import ...` statement runs anywhere in the test suite.

All values here are safe dummies suitable only for unit tests.
"""
import os

os.environ.setdefault("DEV_MODE",                "true")
os.environ.setdefault("USE_AZURE_IDENTITY",      "false")
os.environ.setdefault("AZURE_TENANT_ID",         "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("AZURE_CLIENT_ID",         "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("AZURE_CLIENT_SECRET",     "dummy-secret-for-tests")
os.environ.setdefault("AZURE_SUBSCRIPTION_ID",   "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("AZURE_RESOURCE_GROUP",    "rg-test")
os.environ.setdefault("SWAGGER_CLIENT_ID",       "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("FABRIC_BASE_URL",         "https://api.fabric.microsoft.com/v1")
os.environ.setdefault("ARM_BASE_URL",            "https://management.azure.com")
