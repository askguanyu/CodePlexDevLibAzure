using System;
using DevLib.Azure.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DevLib.Azure.Storage;

namespace DevLib.Azure.NET45.UnitTest.DevLib.Azure.Logging
{
    [TestClass]
    public class Given_Logger
    {
        [TestMethod]
        public void When_Log()
        {
            Logger logger = new Logger()
                .AddConsole()
                .AddDebug()
                .AddTableLogger(new TableLogger("logtest", StorageConstants.DevelopmentStorageConnectionString));

            logger.Log("Given_Logger_hello1");
            logger.Log("Given_Logger_hello2");
            logger.Log("Given_Logger_hello3");

        }
    }
}
