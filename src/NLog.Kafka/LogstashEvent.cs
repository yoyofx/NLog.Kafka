using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NLog.Kafka
{
    public class LogstashEvent
    {
        public string appname { set; get; }

        public string level { set; get; }

        public string logger_name { set; get; }


        public string message { set; get; }

        public string @timestamp { get; set; }

        public UInt64 version { set; get; }

        public string HOSTNAME { set; get; }

        public string thread_name { get; set; }

        public string stack_trace { set; get; }

        public string line_number { get; set; }

        public string @class { set; get;}

        public string method { set; get; }

    }
}
