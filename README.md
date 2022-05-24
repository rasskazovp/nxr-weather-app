# Project Structure

.
- /app - aplication code
    - /nxr-weather-app-api  - APIs code 
    - /nxr-weather-app-test - Tests for APIs
- /.github - CI/CD related components - TODO

# How to build and run in your own environment

Clone repo to your local directory and open with Visual Studio

## To run app localy

Create file app\nxr-weather-app-api\nxr-weather-app-api\local.settings.json
with below content providing connection string to your storagae.
```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet",
    "sigma-iot-storage-conn-string": "<connection string>"
  }
}
```
**Run** app from Visual Studio.

## To run tests localy

Create file app\nxr-weather-app-test\Properties\launchSettings.json
with below content providing connection string to your storagae.
```
{
  "profiles": {
    "nxr-weather-app-test": {
      "commandName": "Project",
      "environmentVariables": {
        "sigma-iot-storage-conn-string": <connection string>
      }
    }
  }
}
```
**Execute** tests from Visual Studio.

## Deploy on azure functions

- **Publish** solution to you Azure Function App (Windows), .Net 6.0 or via on Azure Dunction App Containier using Docker.
- **Set** envarionmental variable "sigma-iot-storage-conn-string" providing connection string to storage as value.

# API usage

API exposes swagger UI under URI - 
http://localhost:7071/api/swagger/ui (you may have different port) for local or under http://\<function-app-name\>.azurewebsites.net/api/swagger/ui for azure function deployment. Go to that link to check available function, their usage, and check their behaviour.