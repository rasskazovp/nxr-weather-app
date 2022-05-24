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
using nxr_weather_app_api.Models;
using TinyCsvParser;

namespace nxr_weather_app_api.APIs
{
    public class GetWeatherDataFunctions
    {
        private readonly ILogger _logger;
        private readonly string _backEndStorageConnString;
        private readonly string _backEndContainierName;

        public GetWeatherDataFunctions(ILogger<GetWeatherDataFunctions> log)
        {
            _logger = log;
            _backEndStorageConnString = Environment.GetEnvironmentVariable("sigma-iot-storage-conn-string");
            _backEndContainierName = "iotbackend";
        }




        [FunctionName("getData")]
        [OpenApiOperation(operationId: "getData", tags: new[] { "getSensorsData" })]
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiParameter(name: "sensorType", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "sensorType for which data should be provides. For example: humidity")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getData(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "v1/device/{deviceId}/data/{date}/{sensorType}")] HttpRequest req,
            string deviceId,
            string date,
            string sensorType)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            BlobContainerClient iotContainier = new BlobContainerClient(_backEndStorageConnString, _backEndContainierName);

            try
            {
                List<SensorData> parsedDataResult = await processGetSensorDataRequest(iotContainier, deviceId, sensorType, date);
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
        [OpenApiParameter(name: "deviceId", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "An Id of weather measuring device parameter")]
        [OpenApiParameter(name: "date", In = ParameterLocation.Path, Required = true, Type = typeof(string), Description = "Date for which weather summary should be provided in format YYYY-MM-DD")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string), Description = "All measurements for one day, one sensor type, and one unit")]
        public async Task<IActionResult> getDataForDevice(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "v1/devices/{deviceId}/data/{date}")] HttpRequest req,
            string deviceId,
            string date)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            BlobContainerClient iotContainier = new BlobContainerClient(_backEndStorageConnString, _backEndContainierName);

            try
            {
                _logger.LogInformation($"Reading sensor types from metadata.");
                List<SensorData> parsedDataResult = new List<SensorData>();
                BlobClient metadataBlobClient = iotContainier.GetBlobClient("metadata.csv");

                using (var metadataStream = new MemoryStream())
                {
                    await metadataBlobClient.DownloadToAsync(metadataStream);
                    metadataStream.Seek(0, SeekOrigin.Begin);

                    using (StreamReader sr = new StreamReader(metadataStream))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string[] lineElems = line.Split(';');
                            if (lineElems[0] == deviceId)
                            {
                                parsedDataResult.AddRange(
                                    await processGetSensorDataRequest(iotContainier, deviceId, lineElems[1], date)
                                );
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

            BlobClient dataBlobClient = iotContainier.GetBlobClient($"{deviceId}/{sensorType}/{date}.csv");
            if (await dataBlobClient.ExistsAsync())     // Check if exists uncompressed data file
            {
                parsedDataResult = await readAndParseData(dataBlobClient);
            }
            else
            {
                BlobClient historicalDataArchiveBlobClient = iotContainier.GetBlobClient($"{deviceId}/{sensorType}/historical.zip");

                if (!await historicalDataArchiveBlobClient.ExistsAsync())
                    throw new FileNotFoundException($"{deviceId}/{sensorType}/historical.zip file not exists.");

                using (var historicalDataArchiveStream = new MemoryStream())
                {
                    await historicalDataArchiveBlobClient.DownloadToAsync(historicalDataArchiveStream);
                    historicalDataArchiveStream.Seek(0, SeekOrigin.Begin);

                    ZipArchiveEntry singleHistoricalFileArchive = new ZipArchive(historicalDataArchiveStream).GetEntry($"{date}.csv");

                    if (singleHistoricalFileArchive != null)
                    {
                        _logger.LogInformation($"Reading {sensorType} sensor data from comressed file...");

                        using (var singleHistoricalFileStream = singleHistoricalFileArchive.Open())
                        {
                            parsedDataResult = parseCsvContent(singleHistoricalFileStream);
                        }
                    }
                    else throw new FileNotFoundException($"Data for requested device={deviceId}, sensor={sensorType} and date={date} not found");
                }
            }
            return parsedDataResult;
        }




        private async Task<List<SensorData>> readAndParseData(BlobClient blobClient)
        {
            _logger.LogInformation($"Reading blob from storage...");

            using (var csvDataStream = new MemoryStream())
            {
                await blobClient.DownloadToAsync(csvDataStream);
                csvDataStream.Seek(0, SeekOrigin.Begin);
                return parseCsvContent(csvDataStream);
            }
        }




        private List<SensorData> parseCsvContent(Stream dataStream, bool skipHeaderchar = false, char csvDelimiter = ';')
        {
            _logger.LogInformation("Parsing CSV content...");

            NumberFormatInfo numberFormatProvider = getNumberFormatingInfo();

            CsvParserOptions csvParserOptions = new CsvParserOptions(skipHeaderchar, csvDelimiter);
            SensorDataCsvMapping csvMapper = new SensorDataCsvMapping(numberFormatProvider);
            CsvParser<SensorData> csvParser = new CsvParser<SensorData>(csvParserOptions, csvMapper);

            var parsingResults = csvParser.ReadFromStream(dataStream, Encoding.UTF8);

            // Potential parsing error could be logged here.

            var dataResult = parsingResults.Where(rec => rec.IsValid).Select(rec => rec.Result);

            return dataResult.ToList(); // Not clear requirments regarding how output should look like, it could be adjuste here.
        }

        private NumberFormatInfo getNumberFormatingInfo()
        {
            NumberFormatInfo numberFormatProvider = new NumberFormatInfo();
            numberFormatProvider.NumberDecimalSeparator = ",";

            return numberFormatProvider;
        }
    }
}

