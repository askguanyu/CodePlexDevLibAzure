//-----------------------------------------------------------------------
// <copyright file="DynamicTableEntityExtensions.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// DynamicTableEntity extensions methods.
    /// </summary>
    public static class DynamicTableEntityExtensions
    {
        /// <summary>
        /// The value key.
        /// </summary>
        internal const string ValueKey = "Value_bcd3499d059f4971b10b512019738535";

        /// <summary>
        /// Converts object to DynamicTableEntity instance.
        /// </summary>
        /// <param name="source">The source object.</param>
        /// <param name="partitionKey">A string containing the partition key value for the entity.</param>
        /// <param name="rowKey">A string containing the row key value for the entity.</param>
        /// <returns>DynamicTableEntity instance.</returns>
        public static DynamicTableEntity ToTableEntity(this object source, string partitionKey, string rowKey)
        {
            var result = new DynamicTableEntity(partitionKey, rowKey);

            result.Properties[ValueKey] = EntityProperty.CreateEntityPropertyFromObject(source);

            return result;
        }

        /// <summary>
        /// Converts DynamicTableEntity instance to object.
        /// </summary>
        /// <param name="source">The source object.</param>
        /// <returns>The object from DynamicTableEntity.</returns>
        public static object TableEntityToObject(this DynamicTableEntity source)
        {
            if (source == null || !source.Properties.ContainsKey(ValueKey))
            {
                return null;
            }

            return source[ValueKey].PropertyAsObject;
        }

        /// <summary>
        /// Converts DynamicTableEntity instance to object.
        /// </summary>
        /// <typeparam name="T">The type of return object.</typeparam>
        /// <param name="source">The source object.</param>
        /// <returns>The object from DynamicTableEntity.</returns>
        public static T TableEntityToObject<T>(this DynamicTableEntity source)
        {
            if (source == null || !source.Properties.ContainsKey(ValueKey))
            {
                return default(T);
            }

            return (T)source[ValueKey].PropertyAsObject;
        }
    }
}
