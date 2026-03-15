from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # silently ignore unrecognised env vars (e.g. AZURE_LOCATION)
    )

    # Authentication
    USE_AZURE_IDENTITY: bool = Field(True, validation_alias="USE_AZURE_IDENTITY")
    AZURE_TENANT_ID: Optional[str] = Field(None, validation_alias="AZURE_TENANT_ID")
    AZURE_CLIENT_ID: Optional[str] = Field(None, validation_alias="AZURE_CLIENT_ID")
    AZURE_CLIENT_SECRET: Optional[str] = Field(None, validation_alias="AZURE_CLIENT_SECRET")

    # Dev mode — bypasses JWT auth entirely (never enable in production)
    DEV_MODE: bool = Field(False, validation_alias="DEV_MODE")

    # Swagger / Entra ID OAuth2 — required for the Swagger UI "Authorize" button
    # Register a SPA app registration in Entra ID and set its redirect URI to
    # http://localhost:8000/docs/oauth2-redirect
    SWAGGER_CLIENT_ID: Optional[str] = Field(None, validation_alias="SWAGGER_CLIENT_ID")
    SWAGGER_SCOPES: str = Field("openid profile email", validation_alias="SWAGGER_SCOPES")

    # Fabric / ARM
    FABRIC_BASE_URL: str = Field("https://api.fabric.microsoft.com/v1", validation_alias="FABRIC_BASE_URL")
    ARM_BASE_URL: str = Field("https://management.azure.com", validation_alias="ARM_BASE_URL")
    SUBSCRIPTION_ID: Optional[str] = Field(None, validation_alias="AZURE_SUBSCRIPTION_ID")
    RESOURCE_GROUP: Optional[str] = Field(None, validation_alias="AZURE_RESOURCE_GROUP")

    # Defaults
    DEFAULT_SKU: str = "F2"

settings = Settings()
