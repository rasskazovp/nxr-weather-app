using System;

namespace nxr_weather_app_api.Models
{
    /// <summary>
    /// Model for expected Sensor Data on back-end storge.
    /// </summary>
    public class SensorData
    {
        // TODO: consider enriching model with sensor type filed
        public DateTime EventDateTime { get; set; }
        public double SensorValue { get; set; }
    }
}
