using NLog;
using NLog.Config;
using NLog.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    class Program
    {


        static void Main(string[] args)
        {
            //ConfigurationItemFactory.Default.Targets.RegisterDefinition("kafka", typeof(KafkaTarget));

            //LogManager.ReconfigExistingLoggers();
            Logger logger = LogManager.GetCurrentClassLogger();

            MappedDiagnosticsContext.Set("item1", "haha");
            for(int i = 0; i < 10; i++)
            {
                logger.Error(new NotImplementedException("error"),"error");
                Console.WriteLine("sended");
            }

            Console.ReadKey();
        }
    }
}
