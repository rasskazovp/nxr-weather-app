using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using TinyCsvParser;
using TinyCsvParser.Mapping;

namespace nxr_weather_app_api
{
    public class GetWeatherFunction
    {
        private readonly ILogger<GetWeatherFunction> _logger;

        public GetWeatherFunction(ILogger<GetWeatherFunction> log)
        {
            _logger = log;
        }

        [FunctionName("getData")]
        [OpenApiOperation(operationId: "getData", tags: new[] { "getSensorsData" })]
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiParameter(name: "sensorType", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "sensorType for which data should be provides. For example: humidity")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getData(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "v1/devices/{deviceId}/data/{date}/{sensorType}")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string filePath = $"{req.Query["deviceId"]}/{req.Query["sensorType"]}/{req.Query["date"]}.csv";

            dynamic objectResult;
            try
            {
                CloudBlobContainer IotContainier = getStorageContainier();

                CloudAppendBlob fileReference = IotContainier.GetAppendBlobReference(filePath);

                string fileContent = await fileReference.DownloadTextAsync();

                var data = parseCsvContent(fileContent).ToList();
                objectResult = JsonConvert.SerializeObject(data);
            }
            catch(Exception ex)
            {
                objectResult = new { status = "Fail",
                                     exceptionMessage = ex.Message};
            }

            return new OkObjectResult(objectResult);
        }

        [FunctionName("getDataForDevice")]
        [OpenApiOperation(operationId: "getDataForDevice", tags: new[] { "getSensorsData" })]
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getDataForDevice(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "v1/devices/{deviceId}/data/{date}")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            dynamic objectResult;
            try
            {
                CloudBlobContainer IotContainier = getStorageContainier();

                string deviceMetadata = $"{req.Query["deviceId"]}/metadata.csv";
                CloudAppendBlob deviceMetadataFileReference = IotContainier.GetAppendBlobReference(deviceMetadata);
                string fileContent = await deviceMetadataFileReference.DownloadTextAsync();
                
                // TODO: iterate over sensor types
                // Read data for all sensor types and enriech with sensor information
                // Calculate average daily and return

                var data = parseCsvContent(fileContent).ToList();
                objectResult = JsonConvert.SerializeObject(data);
            }
            catch (Exception ex)
            {
                objectResult = new
                {
                    status = "Fail",
                    exceptionMessage = ex.Message
                };
            }

            return new OkObjectResult(objectResult);
        }
 

        private ParallelQuery<SensorData> parseCsvContent(string csvContent, bool skipHeaderchar=false, char csvDelimiter=';')
        {
            _logger.LogInformation("Start parsing CSV content.");

            NumberFormatInfo numberFormatProvider = new NumberFormatInfo();
            numberFormatProvider.NumberDecimalSeparator = ",";

            CsvParserOptions csvParserOptions = new CsvParserOptions(skipHeaderchar, csvDelimiter);
            CsvReaderOptions csvReaderOptions = new CsvReaderOptions(new[] { Environment.NewLine });
            CsvSensorDataMapping csvMapper = new CsvSensorDataMapping(numberFormatProvider);
            CsvParser<SensorData> csvParser = new CsvParser<SensorData>(csvParserOptions, csvMapper);
            var mappingResults = csvParser.ReadFromString(csvReaderOptions, csvContent);
            var dataResult = mappingResults.Where(rec => rec.IsValid).Select(rec => rec.Result);

            return dataResult;
        }
        

        private CloudBlobContainer getStorageContainier()
        {
            _logger.LogInformation("Define IoT storage containier.");

            string connectionString = Environment.GetEnvironmentVariable("sigma-iot-storage-conn-string");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();

            return client.GetContainerReference("iotbackend");
        }
    }

    public class SensorData
    {
        public DateTime EventDateTime { get; set; }
        public float SensorValue { get; set; }
    } 

    public class CsvSensorDataMapping : CsvMapping<SensorData>
    {
        public CsvSensorDataMapping(NumberFormatInfo numberFormatProvider) : base()
        {
            MapProperty(0, x => x.EventDateTime);
            MapProperty(1, x => Convert.ToDouble(x.SensorValue, numberFormatProvider));
        }
    }
}

