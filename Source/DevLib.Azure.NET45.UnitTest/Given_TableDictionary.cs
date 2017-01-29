using System;
using DevLib.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_TableDictionary
    {
        private string _account = "";
        private string _key = "";

        [TestMethod]
        public void When_IsReadOnly()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.GetTableDictionary("dict1").IsReadOnly;
        }

        [TestMethod]
        public void When_Values()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.GetTableDictionary("dict1").Values;
        }

        [TestMethod]
        public void When_Keys()
        {
            var table = new TableStorage("table1", _account, _key);

            var result = table.GetTableDictionary("dict1").Keys;
        }

        [TestMethod]
        public void When_AddOrUpdate()
        {
            var dict = new TableDictionary("dict1", "table1", CloudStorageAccount.DevelopmentStorageAccount, true); //new TableStorage("table1", _account, _key);

            dict.AddOrUpdate("key1", TimeSpan.FromDays(1));

            var result = dict.Values;
        }
    }
}
