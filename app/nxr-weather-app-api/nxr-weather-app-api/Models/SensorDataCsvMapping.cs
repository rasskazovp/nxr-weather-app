using System.Globalization;
using TinyCsvParser.Mapping;
using TinyCsvParser.TypeConverter;

namespace nxr_weather_app_api.Models
{
    public class SensorDataCsvMapping : CsvMapping<SensorData>
    {
        public SensorDataCsvMapping(NumberFormatInfo numberFormatProvider) : base()
        {
            MapProperty(0, x => x.EventDateTime);
            MapProperty(1, x => x.SensorValue, new DoubleConverter(numberFormatProvider));
        }
    }
}
