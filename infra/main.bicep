// =============================================================================
// EDP Fabric Control Plane — Azure Functions Infrastructure
// Deploy:  az deployment group create -g <rg> -f infra/main.bicep -p @infra/main.parameters.json
// =============================================================================

@description('Azure region for all resources (defaults to resource group location)')
param location string = resourceGroup().location

@description('Short environment tag: dev | uat | prod')
@allowed(['dev', 'uat', 'prod'])
param environment string = 'dev'

@description('Unique suffix appended to globally-unique resource names')
param suffix string = uniqueString(resourceGroup().id)

@description('Entra ID tenant ID (used as app setting)')
param tenantId string = subscription().tenantId

@description('Swagger UI SPA app registration client ID')
param swaggerClientId string = ''

@description('Azure subscription ID (used as app setting)')
param subscriptionId string = subscription().subscriptionId

// ---------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------
var storageAccountName  = 'stfabric${environment}${take(suffix, 6)}'
var appInsightsName     = 'appi-fabric-cp-${environment}'
var logAnalyticsName    = 'log-fabric-cp-${environment}'
var hostingPlanName     = 'asp-fabric-cp-${environment}'
var functionAppName     = 'func-fabric-cp-${environment}-${take(suffix, 6)}'
var keyVaultName        = 'kv-fabric-${environment}-${take(suffix, 4)}'

// ---------------------------------------------------------------------------
// Storage Account (mandatory for Functions runtime)
// ---------------------------------------------------------------------------
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

// ---------------------------------------------------------------------------
// Log Analytics Workspace
// ---------------------------------------------------------------------------
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 90
  }
}

// ---------------------------------------------------------------------------
// Application Insights
// ---------------------------------------------------------------------------
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    RetentionInDays: 90
  }
}

// ---------------------------------------------------------------------------
// App Service Plan — Consumption (Y1/Dynamic) — swap to EP1 for Premium
// ---------------------------------------------------------------------------
resource hostingPlan 'Microsoft.Web/serverfarms@2023-01-01' = {
  name: hostingPlanName
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {
    reserved: true  // Linux required for Python
  }
}

// ---------------------------------------------------------------------------
// Key Vault — for storing secrets referenced by app settings
// ---------------------------------------------------------------------------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    enabledForTemplateDeployment: false
  }
}

// ---------------------------------------------------------------------------
// Function App
// ---------------------------------------------------------------------------
resource functionApp 'Microsoft.Web/sites@2023-01-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'  // Managed Identity — used by DefaultAzureCredential
  }
  properties: {
    serverFarmId: hostingPlan.id
    httpsOnly: true
    siteConfig: {
      pythonVersion: '3.11'
      linuxFxVersion: 'Python|3.11'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      appSettings: [
        // --- Functions runtime ---
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${storage.listKeys().keys[0].value}'
        }
        { name: 'FUNCTIONS_EXTENSION_VERSION'; value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME';    value: 'python' }

        // --- Application Insights ---
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'; value: appInsights.properties.ConnectionString }
        { name: 'ApplicationInsightsAgent_EXTENSION_VERSION'; value: '~3' }

        // --- App config ---
        { name: 'USE_AZURE_IDENTITY';       value: 'true' }
        { name: 'AZURE_TENANT_ID';          value: tenantId }
        { name: 'AZURE_SUBSCRIPTION_ID';    value: subscriptionId }
        { name: 'AZURE_RESOURCE_GROUP';     value: resourceGroup().name }
        { name: 'SWAGGER_CLIENT_ID';        value: swaggerClientId }
        { name: 'FABRIC_BASE_URL';          value: 'https://api.fabric.microsoft.com/v1' }
        { name: 'ARM_BASE_URL';             value: 'https://management.azure.com' }
        { name: 'DEV_MODE';                 value: 'false' }
      ]
    }
  }
}

// ---------------------------------------------------------------------------
// Key Vault — grant Function App's Managed Identity "Secrets User" role
// ---------------------------------------------------------------------------
var kvSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'

resource kvRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, functionApp.id, kvSecretsUserRoleId)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsUserRoleId)
    principalId: functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output functionAppName        string = functionApp.name
output functionAppUrl         string = 'https://${functionApp.properties.defaultHostName}'
output functionAppPrincipalId string = functionApp.identity.principalId
output keyVaultName           string = keyVault.name
output appInsightsName        string = appInsights.name
