using System;
using DevLib.Azure.Logging;
using DevLib.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_TableLogger
    {
        [TestMethod]
        public void When_GetStackFrameInfo()
        {
            var result = TableLogger.GetStackFrameInfo();
        }

        [TestMethod]
        public void When_Log()
        {
            var logger = new TableLogger("logtest", "");
            //new TableLogger("", (TableClient)null).Log(LoggingLevel.ALL, "hello1", "a1", "b1", "c1");
            logger.Log(LogLevel.ALL, "hello2", "a2", "b2", "c2");
            logger.LogDebug(new[] { "a", "b", "c" });

            //var messageEntity1 = new LoggingMessageTableEntity();
            //var messageEntity2 = new LoggingMessageTableEntity();

            //TableClient.DevelopmentClient.GetTableStorage("log").Insert(messageEntity1);
            //TableClient.DevelopmentClient.GetTableStorage("log").Insert(messageEntity2);
        }

        [TestMethod]
        public void When_LogMessageTableEntity()
        {
            var logger = new TableLogger("logtest", StorageConstants.DevelopmentStorageConnectionString);
            var msg = new LogMessageTableEntity();
            msg["p1"] = "p1";
            msg["p2"] = 2;

            logger.Log(msg);
        }
    }
}
