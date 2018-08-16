using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using NLog.Common;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace NLog.Kafka
{
    [Target("Kafka")]
    public class KafkaTarget : TargetWithLayout
    {

        private Producer<string, string> _producer = null;

        public KafkaTarget()
        {
            this.ProducerConfigs = new List<ProducerConfig>(10);
        }


        [RequiredParameter]
        [ArrayParameter(typeof(ProducerConfig), "producerConfig")]
        public IList<ProducerConfig> ProducerConfigs { get; set; }

        [RequiredParameter]
        public string appname { get; set; }

        [RequiredParameter]
        public string topic { get; set; }

        [RequiredParameter]
        public bool includeMdc { get; set; }



        protected override void Write(LogEventInfo logEvent)
        {
            IPHostEntry ipHost = Dns.GetHostEntryAsync(Dns.GetHostName()).Result;
            IPAddress ipAddr = ipHost.AddressList[0];

            Dictionary<string, object> formatLogEvent = new Dictionary<string, object>() {
                { "version"     , logEvent.SequenceID },
                { "@timestamp"  , DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture) },
                { "appname"     , this.appname },
                { "HOSTNAME"    , ipAddr.ToString() },
                { "thread_name" , System.Threading.Thread.CurrentThread.Name },
                { "level"       , logEvent.Level.Name },
                { "logger_name" , logEvent.LoggerName },
                { "message"     , logEvent.FormattedMessage },
            };

         

            if (logEvent.Exception != null)
            {
                formatLogEvent["message"] = logEvent.Exception.Message;
                formatLogEvent["stack_trace"] = logEvent.Exception.StackTrace;
            }


            if (includeMdc)
            {
                TransferContextDataToLogEventProperties(formatLogEvent);
            }

            string message = JsonConvert.SerializeObject(formatLogEvent);

            SendMessageToQueue(message);

            base.Write(logEvent);
        }


        private static void TransferContextDataToLogEventProperties(Dictionary<string, object> logDic)
        {
            foreach (var contextItemName in MappedDiagnosticsContext.GetNames())
            {
                var key = contextItemName;

                if (!logDic.ContainsKey(key))
                {
                    var value = MappedDiagnosticsContext.Get(key);
                    logDic.Add(key, value);
                }
            }
        }

        #region 创建 kafka 与 发现队列函数
        private Producer<string, string> GetProducer()
        {
            if (this.ProducerConfigs == null || this.ProducerConfigs.Count == 0) throw new Exception("ProducerConfigs is not found");

            if (_producer == null)
            {
                var config = new Dictionary<string, object>
                {
                    //{ "bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9092,127.0.0.1:9092" },
                    //{ "queue.buffering.max.messages", 2000000 },
                    //{ "message.send.max.retries", 3 },
                    //{ "retry.backoff.ms", 500 }
                };

                foreach (var pconfig in this.ProducerConfigs)
                {
                    config.Add(pconfig.Key, pconfig.value);
                }



                _producer = new Producer<string,string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));

                _producer.OnError += _producer_OnError;
                _producer.OnLog += _producer_OnLog;
                _producer.OnStatistics += _producer_OnStatistics;
            }
            return _producer;
        }

        private void SendMessageToQueue(string message)
        {

            if (string.IsNullOrEmpty(message))
                return;
            var producer = this.GetProducer();

            var key = "Multiple." + DateTime.Now.Ticks;

            //.ProduceAsync(topic, key, msg, null);
            var dr = producer.ProduceAsync(topic, key, message);

            dr.ContinueWith(task =>
            {
                Console.WriteLine($"Delivered '{task.Result.Value}' to: {task.Result.TopicPartitionOffset} message: {task.Result.Error.Reason}");
            });


           

        }

        private void _producer_OnStatistics(object sender, string e)
        {
            Console.WriteLine($"nlog.kafka statistics: {e}");
        }

        private void _producer_OnLog(object sender, LogMessage e)
        {
            Console.WriteLine($"nlog.kafka on log: [ Level: {e.Level} Facility:{e.Facility} Name:{e.Name} Message:{e.Message} ]");
        }

        private void _producer_OnError(object sender, Error e)
        {
            Console.WriteLine($"nlog.kafka error: [ Code:{e.Code} HasError:{e.HasError} IsBrokerError:{e.IsBrokerError} IsLocalError:{e.IsLocalError} Reason:{e.Reason} ]");
        }

        private void CloseProducer()
        {
            if (_producer != null)
            {
                _producer?.Flush(TimeSpan.FromSeconds(60));
                _producer?.Dispose();
            }
            _producer = null;
        }

        #endregion


        protected override void CloseTarget()
        {
            CloseProducer();
            base.CloseTarget();
        }

    }
}
