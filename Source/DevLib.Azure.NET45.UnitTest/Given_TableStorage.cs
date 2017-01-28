using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DevLib.Azure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DevLib.Azure.NET45.UnitTest
{
    [TestClass]
    public class Given_TableStorage
    {
        [TestMethod]
        public void When_Insert()
        {
            var table = new TableStorage("table1", "", "");

            var result = table.Insert(new TableEntity(), "p1", "r2", true);
        }

        [TestMethod]
        public void When_GetTableUriWithSASReadOnly()
        {
            var table = new TableStorage("table1", "", "");

            var result = table.GetTableUriWithSASReadOnly(TimeSpan.FromDays(30));
        }
    }
}
