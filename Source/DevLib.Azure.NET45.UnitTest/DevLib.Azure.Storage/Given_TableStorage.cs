using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DevLib.Azure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_TableStorage
    {
        private string _account = "";
        private string _key = "";

        [TestMethod]
        public void When_Insert()
        {
            var table = new TableStorage("table1", _account, _key);

            var entity = new DynamicTableEntity();
            entity["Value"] = new EntityProperty("hello");

            var result = table.Insert(entity, "p1", "r3", true);
        }

        [TestMethod]
        public void When_InsertOrReplace()
        {
            var table = new TableStorage("table1", _account, _key);

            var entity = new DynamicTableEntity();
            entity["Value"] = new EntityProperty("hello");

            var result = table.InsertOrReplace(entity, "p9", "r3");
        }

        [TestMethod]
        public void When_GetTableUriWithSASReadOnly()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.GetTableUriWithSasReadOnly(TimeSpan.FromDays(30));
        }

        [TestMethod]
        public void When_EntityExists()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.EntityExists("p2", "r1");
        }

        [TestMethod]
        public void When_PartitionKeyExists()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.PartitionKeyExists("p1");
        }

        [TestMethod]
        public void When_ListPartitionKeys()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.ListPartitionKeys();
        }

        [TestMethod]
        public void When_ListRowKeys()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.ListRowKeys();
        }

        [TestMethod]
        public void When_InsertOrReplace1()
        {
            var table = new TableStorage("table2", StorageConstants.DevelopmentStorageConnectionString);

            table.InsertOrMerge(new TestEntity { PartitionKey = "p1", RowKey = "r1", ColumnA = "a", ColumnB = "b" });
            table.InsertOrMerge(new TestEntity { PartitionKey = "p1", RowKey = "r1", ColumnA = "c", ColumnB = null });

        }
    }

    public class TestEntity : TableEntity
    {
        public string ColumnA { get; set; }

        public string ColumnB { get; set; }
    }
}
