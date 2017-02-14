//-----------------------------------------------------------------------
// <copyright file="TableEntityExtensions.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// Table entity extensions methods.
    /// </summary>
    public static class TableEntityExtensions
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
            if (source is DynamicTableEntity)
            {
                var resultSource = (DynamicTableEntity)source;
                resultSource.PartitionKey = partitionKey;
                resultSource.RowKey = rowKey;
                return resultSource;
            }

            var result = new DynamicTableEntity(partitionKey, rowKey);

            result.Properties[ValueKey] = source.ToEntityProperty();

            return result;
        }

        /// <summary>
        /// Converts DynamicTableEntity instance to object.
        /// </summary>
        /// <param name="source">The source object.</param>
        /// <returns>The object from DynamicTableEntity.</returns>
        public static object ToObject(this DynamicTableEntity source)
        {
            if (source == null || !source.Properties.ContainsKey(ValueKey))
            {
                return null;
            }

            return source[ValueKey].ToObject();
        }

        /// <summary>
        /// Converts DynamicTableEntity instance to object.
        /// </summary>
        /// <typeparam name="T">The type of return object.</typeparam>
        /// <param name="source">The source object.</param>
        /// <returns>The object from DynamicTableEntity.</returns>
        public static T ToObject<T>(this DynamicTableEntity source)
        {
            if (source == null || !source.Properties.ContainsKey(ValueKey))
            {
                return default(T);
            }

            return source[ValueKey].ToObject<T>();
        }

        /// <summary>
        /// Converts object to the EntityProperty.
        /// </summary>
        /// <param name="source">The source object.</param>
        /// <returns>EntityProperty instance.</returns>
        public static EntityProperty ToEntityProperty(this object source)
        {
            if (source is EntityProperty)
            {
                return (EntityProperty)source;
            }

            return EntityProperty.CreateEntityPropertyFromObject(
                source.IsEntityPropertyKnownType()
                ? source
                : JsonConvert.SerializeObject(source));
        }

        /// <summary>
        /// Converts EntityProperty to the object.
        /// </summary>
        /// <param name="source">The source EntityProperty.</param>
        /// <returns>The object from EntityProperty.</returns>
        public static object ToObject(this EntityProperty source)
        {
            if (source == null)
            {
                return null;
            }

            return source.PropertyAsObject;
        }

        /// <summary>
        /// Converts EntityProperty to the object.
        /// </summary>
        /// <typeparam name="T">The type of return object.</typeparam>
        /// <param name="source">The source EntityProperty.</param>
        /// <returns>The object from EntityProperty.</returns>
        public static T ToObject<T>(this EntityProperty source)
        {
            if (source == null)
            {
                return default(T);
            }

            if (source.PropertyType != EdmType.String)
            {
                return (T)source.PropertyAsObject;
            }

            if (typeof(T) == typeof(string))
            {
                return (T)source.PropertyAsObject;
            }

            return JsonConvert.DeserializeObject<T>((string)source.PropertyAsObject);
        }

        /// <summary>
        /// Determines whether the object is entity property known type.
        /// </summary>
        /// <param name="source">The source object.</param>
        /// <returns>true if the source object is entity property known type; otherwise, false.</returns>
        public static bool IsEntityPropertyKnownType(this object source)
        {
            if (source == null
                || source is string
                || source is byte[]
                || source is bool
                || source is bool?
                || source is DateTime
                || source is DateTime?
                || source is DateTimeOffset
                || source is DateTimeOffset?
                || source is double
                || source is double?
                || source is Guid?
                || source is Guid
                || source is int
                || source is int?
                || source is long
                || source is long?)
            {
                return true;
            }

            return false;
        }
    }
}
