using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DevLib.Azure.Storage;
using Microsoft.WindowsAzure.Storage;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_BlobClient
    {
        [TestMethod]
        public void When_GetContainer()
        {
            try
            {
                var client = BlobClient.DevelopmentClient;// new BlobClient("accountName", "accessKey");

                var container = client.GetContainer("test1", false);
            }
            catch
            {
            }
        }
    }
}
