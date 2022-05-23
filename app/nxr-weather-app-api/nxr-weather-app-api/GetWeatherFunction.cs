using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;
using TinyCsvParser;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace nxr_weather_app_api
{
    public class GetWeatherFunction
    {
        private readonly ILogger _logger;
        private readonly string _backEndStorageConnString;
        private readonly string _backEndContainierName;

        public GetWeatherFunction(ILogger<GetWeatherFunction> log)
        {
            _logger = log;
            _backEndStorageConnString = Environment.GetEnvironmentVariable("sigma-iot-storage-conn-string");
            _backEndContainierName = "iotbackend";
        }

        [FunctionName("getData")]
        [OpenApiOperation(operationId: "getData", tags: new[] { "getSensorsData" })]
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiParameter(name: "sensorType", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "sensorType for which data should be provides. For example: humidity")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getData(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req)
        {
            //Route = "v1/devices/{deviceId}/data/{date}/{sensorType}
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            BlobContainerClient iotContainier = new BlobContainerClient(_backEndStorageConnString, _backEndContainierName);

            try
            {
                List<SensorData> parsedDataResult = await processGetSensorDataRequest(iotContainier, req.Query["deviceId"], req.Query["sensorType"], req.Query["date"]);
                return new OkObjectResult(JsonConvert.SerializeObject(parsedDataResult));
            }
            catch (FileNotFoundException ex)
            {
                var responseObj = new { status = "Data Not Found", errorMsg = ex.Message };
                return new BadRequestObjectResult(JsonConvert.SerializeObject(responseObj));
            }
            catch (Exception ex)
            {
                var responseObj = new { status = "Failed", errorMsg = ex.Message };
                return new BadRequestObjectResult(JsonConvert.SerializeObject(responseObj));
            }
        }

        [FunctionName("getDataForDevice")]
        [OpenApiOperation(operationId: "getDataForDevice", tags: new[] { "getSensorsData" })]
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getDataForDevice(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req)
        {
            // Route = "v1/devices/{deviceId}/data/{date}"
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            BlobContainerClient iotContainier = new BlobContainerClient(_backEndStorageConnString, _backEndContainierName);
            string deviceId = req.Query["deviceId"];

            try
            {
                _logger.LogInformation($"Reading sensor types from metadata.");

                List<SensorData> parsedDataResult = new List<SensorData>();

                string metadataPath = $"metadata.csv";
                BlobClient metadataBlobClient = iotContainier.GetBlobClient(metadataPath);

                using (var metadataStream = new MemoryStream())
                {
                    await metadataBlobClient.DownloadToAsync(metadataStream);
                    metadataStream.Seek(0, SeekOrigin.Begin);
                    using (StreamReader sr = new StreamReader(metadataStream))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string [] lineElems = line.Split(';');
                            if (lineElems[0] == deviceId)
                            {
                                parsedDataResult.AddRange(await processGetSensorDataRequest(iotContainier, req.Query["deviceId"], lineElems[1], req.Query["date"]));
                            }
                        }
                    }
                    if (!parsedDataResult.Any()) throw new FileNotFoundException($"No data were found for {deviceId}.");
                }
                return new OkObjectResult(JsonConvert.SerializeObject(parsedDataResult));
            }
            catch (FileNotFoundException ex)
            {
                var responseObj = new { status = "Data Not Found", errorMsg = ex.Message };
                return new BadRequestObjectResult(JsonConvert.SerializeObject(responseObj));
            }
            catch (Exception ex)
            {
                var responseObj = new { status = "Failed", errorMsg = ex.Message };
                return new BadRequestObjectResult(JsonConvert.SerializeObject(responseObj));
            }
        }

        private async Task<List<SensorData>> processGetSensorDataRequest(BlobContainerClient iotContainier, string deviceId, string sensorType, string date)
        {
            List<SensorData> parsedDataResult;

            string dataFilePath = $"{deviceId}/{sensorType}/{date}.csv";
            BlobClient dataBlobClient = iotContainier.GetBlobClient(dataFilePath);

            if (await dataBlobClient.ExistsAsync()) // Check if exists uncompressed data file
            {
                _logger.LogInformation($"Reading {sensorType} sensor data from uncomressed file.");

                using (var dataFileStream = new MemoryStream())
                {
                    await dataBlobClient.DownloadToAsync(dataFileStream);
                    dataFileStream.Seek(0, SeekOrigin.Begin);
                    parsedDataResult = parseCsvContent(dataFileStream);
                }
            }
            else
            {
                string historicalDataArchiveBlobPath = $"{deviceId}/{sensorType}/historical.zip";
                BlobClient historicalDataArchiveBlobClient = iotContainier.GetBlobClient(historicalDataArchiveBlobPath);
                if (!await historicalDataArchiveBlobClient.ExistsAsync()) throw new FileNotFoundException($"{deviceId}/{sensorType}/historical.zip file not exists.");

                ZipArchiveEntry singleHistoricalFileArchive;
                using (var historicalDataArchiveStream = new MemoryStream())
                {
                    await historicalDataArchiveBlobClient.DownloadToAsync(historicalDataArchiveStream);
                    historicalDataArchiveStream.Seek(0, SeekOrigin.Begin);
                    ZipArchive historicalDataArchive = new ZipArchive(historicalDataArchiveStream);
                    singleHistoricalFileArchive = historicalDataArchive.GetEntry($"{date}.csv");

                    if (singleHistoricalFileArchive != null)
                    {
                        _logger.LogInformation($"Reading {sensorType} sensor data from comressed file.");

                        using (var singleHistoricalFileStream = singleHistoricalFileArchive.Open())
                        {
                            parsedDataResult = parseCsvContent(singleHistoricalFileStream);
                        }
                    }
                    else
                    {
                        throw new FileNotFoundException($"Data for requested device={deviceId}, sensor={sensorType} and date={date} not found");     
                    }
                }
            }
            return parsedDataResult;
        }

        private List<SensorData> parseCsvContent(Stream dataStream, bool skipHeaderchar=false, char csvDelimiter=';')
        {
            _logger.LogInformation("Start parsing CSV content.");

            NumberFormatInfo numberFormatProvider = new NumberFormatInfo();
            numberFormatProvider.NumberDecimalSeparator = ",";

            CsvParserOptions csvParserOptions = new CsvParserOptions(skipHeaderchar, csvDelimiter);
            CsvSensorDataMapping csvMapper = new CsvSensorDataMapping(numberFormatProvider);
            CsvParser<SensorData> csvParser = new CsvParser<SensorData>(csvParserOptions, csvMapper);
            var parsingResults = csvParser.ReadFromStream(dataStream, Encoding.UTF8);

            // Potential parsing error could be logged here.

            var dataResult = parsingResults.Where(rec => rec.IsValid).Select(rec => rec.Result);

            return dataResult.ToList(); // Not clear requirments regarding how output should look like, it could be adjuste here.
        }
    }

    public class SensorData 
    {
        // TODO: consider enriching model with sensor type filed
        public DateTime EventDateTime { get; set; }
        public double SensorValue { get; set; }
    } 

    public class CsvSensorDataMapping : CsvMapping<SensorData>
    {
        public CsvSensorDataMapping(NumberFormatInfo numberFormatProvider) : base()
        {
            MapProperty(0, x => x.EventDateTime);
            MapProperty(1, x => x.SensorValue, new DoubleConverter(numberFormatProvider));
        }
    }
}

