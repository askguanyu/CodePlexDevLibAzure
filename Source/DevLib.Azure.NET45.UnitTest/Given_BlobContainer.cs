using System;
using DevLib.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_BlobContainer
    {
        [TestMethod]
        public void When_CreateBlockBlob_Text()
        {
            var blobContainer = new BlobContainer("test1", "a", "key", isNewContainerPublic: false);

            var url = blobContainer.CreateBlockBlob("file1.txt", "hello");
        }

        [TestMethod]
        public void When_GetBlobUriWithSASReadOnly()
        {
            var blobContainer = new BlobContainer("test1", "a", "key", isNewContainerPublic: false);

            var url = blobContainer.GetBlobUriWithSasReadOnly("file1.txt", TimeSpan.FromMinutes(1));
        }
    }
}
