using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DevLib.Azure.Storage;

namespace DevLib.Azure.NET45.UnitTest.DevLib.Azure.Storage
{
    [TestClass]
    public class Given_TableEntityExtensions
    {
        [TestMethod]
        public void When_ToTableEntity()
        {
            var a = new MyClass() { MyProperty1 = 1, MyProperty2 = "a" };
            var b = a.ToEntityProperty();
            var c = b.ToObject<MyClass>();
            var d = b.ToObject<string>();
            var e = b.ToObject();


        }
    }

    public class MyClass
    {
        public int MyProperty1 { get; set; }

        public string MyProperty2 { get; set; }
    }
}
