using System;
using DevLib.Azure.Storage;
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

        [TestMethod]
        public void When_InsertDictionaryTableEntity()
        {
            var table = new TableStorage("table2", StorageConstants.DevelopmentStorageConnectionString);

            var entity = new TestTableEntity { PartitionKey = "p1", RowKey = "r1", MyProperty = "hello" };

            table.InsertOrReplace(entity);
            var et1 = table.Retrieve<TestTableEntity>("p1", "r1");

        }
    }

    public class TestTableEntity : DictionaryTableEntity
    {
        public TestTableEntity()
        {
        }

        public string MyProperty { get; set; }

        public int MyProperty1 { get; }

        public int MyProperty2 { set { } }

        public string MyProperty3 { get; private set; }


    }
}
