using System;
using DevLib.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_QueueStorage
    {
        [TestMethod]
        public void When_Enqueue()
        {
            var queueClient = QueueClient.DevelopmentClient;
            var queue = queueClient.GetQueueStorage("queue1");

            queue.Enqueue("msg1", TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(10));
        }
    }
}
