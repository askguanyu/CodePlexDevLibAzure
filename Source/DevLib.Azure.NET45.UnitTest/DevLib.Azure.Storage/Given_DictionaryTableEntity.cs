using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;

namespace DevLib.Azure.NET45.UnitTest.DevLib.Azure.Storage
{
    [TestClass]
    public class Given_DictionaryTableEntity
    {
        [TestMethod]
        public void When_CreateEntityPropertyFromObjectNull()
        {
            var entity = EntityProperty.CreateEntityPropertyFromObject(null);
        }
    }
}
