using System;
using DevLib.Azure.Logging;
using DevLib.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_BlobLogger
    {
        [TestMethod]
        public void When_Log()
        {
            ILogger logger = new BlobLogger("logtest","");

            logger.LogDebug("hello1");
            logger.LogDebug("hello2");
            logger.LogDebug("hello3");

        }
    }
}
