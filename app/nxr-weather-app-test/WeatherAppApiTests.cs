using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using nxr_weather_app_api.APIs;
using nxr_weather_app_api.Models;
using System.Collections;

namespace nxr_weather_app_test
{
    public class WeatherAppApiTests
    {
        ILogger<GetWeatherDataFunctions> _logger;

        public WeatherAppApiTests (){
            
            _logger = Mock.Of<ILogger<GetWeatherDataFunctions>>();

            using (var file = File.OpenText(@"Properties\launchSettings.json"))
            {
                var reader = new JsonTextReader(file);
                var jsonObj = JObject.Load(reader);

                Environment.SetEnvironmentVariable(
                    "sigma-iot-storage-conn-string", 
                    jsonObj["profiles"]["nxr-weather-app-test"]["environmentVariables"]["sigma-iot-storage-conn-string"].ToString()
                );                
            }       
        }

        [Theory]
        [InlineData("dockan", "humidity", "2019-01-10")]
        [InlineData("dockan", "temperature", "2019-01-08")]
        [InlineData("dockan", "rainfall", "2019-01-05")]
        public async void getData_checkIfAnyDataReturned(string deviceId, string sensorType, string date)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getData(request, deviceId, date, sensorType);

            Assert.IsType<OkObjectResult>(response);
            Assert.NotEmpty((IEnumerable)((OkObjectResult)response).Value);
        }

        [Theory]
        [InlineData("dockan", "humidity", "2018-12-31", 416464.64)]
        [InlineData("dockan", "temperature", "2019-01-15", 37696.34)]
        [InlineData("dockan", "rainfall", "2019-01-12", 15.4)]
        public async void getData_checkSums(string deviceId, string sensorType, string date, double expectedResult)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getData(request, deviceId, date, sensorType);

            Assert.IsType<OkObjectResult>(response);
            var responseValue = (string)((OkObjectResult)response).Value;
            var responseResult = JsonConvert.DeserializeObject<List<SensorData>>(responseValue);

            var checkSum = Math.Round(responseResult.Sum(x => x.SensorValue), 2);

            Assert.Equal(expectedResult, checkSum);
        }

        [Theory]
        [InlineData("dockan", "rainfall", "2022-01-05")]
        [InlineData("notExisting", "rainfall", "2022-01-05")]
        [InlineData("dockan", "notExisting", "2022-01-05")]
        public async void getData_checkNoDataException(string deviceId, string sensorType, string date)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getData(request, deviceId, date, sensorType);

            Assert.IsType<BadRequestObjectResult>(response);

            var responseValue = ((BadRequestObjectResult)response).Value;
            var responseResult = (JObject)JsonConvert.DeserializeObject(responseValue.ToString());

            Assert.Equal("Data Not Found", responseResult["status"]);
        }

        [Theory]
        [InlineData("dockan", "2019-01-10")]
        [InlineData("dockan", "2019-01-17")]
        [InlineData("dockan", "2019-01-05")]
        public async void getDataForDevice_checkIfAnyDataReturned(string deviceId, string date)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getDataForDevice(request, deviceId, date);

            Assert.IsType<OkObjectResult>(response);
            Assert.NotEmpty((IEnumerable)((OkObjectResult)response).Value);
        }

        [Theory]
        [InlineData("dockan", "2018-12-31", 484939.41)]
        [InlineData("dockan", "2019-01-15", 409586.85)]
        [InlineData("dockan", "2019-01-12", 426733.32)]
        public async void getDataForDevice_checkSums(string deviceId, string date, double expectedResult)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getDataForDevice(request, deviceId, date);

            Assert.IsType<OkObjectResult>(response);
            var responseValue = (string)((OkObjectResult)response).Value;
            var responseResult = JsonConvert.DeserializeObject<List<SensorData>>(responseValue);

            var checkSum = Math.Round(responseResult.Sum(x => x.SensorValue), 2);

            Assert.Equal(expectedResult, checkSum);
        }

        [Theory]
        [InlineData("dockan", "2022-01-05")]
        [InlineData("notExisting", "2022-01-05")]
        public async void getDataForDevice_checkNoDataException(string deviceId, string date)
        {
            var request = createHttpRequest();
            var response = await new GetWeatherDataFunctions(_logger).getDataForDevice(request, deviceId, date);

            Assert.IsType<BadRequestObjectResult>(response);
            var responseValue = ((BadRequestObjectResult)response).Value;
            var responseResult = (JObject)JsonConvert.DeserializeObject(responseValue.ToString());

            Assert.Equal("Data Not Found", responseResult["status"]);
        }

        private DefaultHttpRequest createHttpRequest()
        {
            return new DefaultHttpRequest(new DefaultHttpContext());
        }
    }
}