//-----------------------------------------------------------------------
// <copyright file="TableDictionary.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents a Microsoft Azure table as a dictionary.
    /// </summary>
    public class TableDictionary : IDictionary<string, object>, ICollection<KeyValuePair<string, object>>, IReadOnlyCollection<KeyValuePair<string, object>>, IEnumerable<KeyValuePair<string, object>>, IEnumerable
    {
        /// <summary>
        /// The partition key prefix.
        /// </summary>
        internal const string PartitionKeyPrefix = "[abe0005754444cc5b3dacb28981a28c1]";

        /// <summary>
        /// The row key prefix.
        /// </summary>
        internal const string RowKeyPrefix = "[82588a3a96ec412497548831e55a096a]";

        /// <summary>
        /// The filter string format.
        /// </summary>
        internal const string FilterStringFormat = "(PartitionKey eq '[abe0005754444cc5b3dacb28981a28c1]{0}') and ((RowKey ge '[82588a3a96ec412497548831e55a096a]') and (RowKey lt '[82588a3a96ec412497548831e55a096a^'))";

        /// <summary>
        /// The value key string.
        /// </summary>
        internal const string ValueKey = "Value";

        /// <summary>
        /// The table storage.
        /// </summary>
        private readonly TableStorage _tableStorage;

        /// <summary>
        /// Field _dictionaryKeyIgnoreCase.
        /// </summary>
        private readonly bool _dictionaryKeyIgnoreCase;

        /// <summary>
        /// The dictionary name.
        /// </summary>
        private readonly string _dictionaryName;

        /// <summary>
        /// The dictionary partition key.
        /// </summary>
        private readonly string _dictionaryPartitionKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDictionary"/> class.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableStorage">The table storage.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        public TableDictionary(string dictionaryName, TableStorage tableStorage, bool dictionaryKeyIgnoreCase = false)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();
            tableStorage.ValidateNull();

            this._tableStorage = tableStorage;
            this._dictionaryName = dictionaryName;
            this._dictionaryKeyIgnoreCase = dictionaryKeyIgnoreCase;
            this._dictionaryPartitionKey = PartitionKeyPrefix + dictionaryName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDictionary" /> class.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        public TableDictionary(string dictionaryName, string tableName, string connectionString, bool dictionaryKeyIgnoreCase = false)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            this._tableStorage = new TableStorage(tableName, connectionString);
            this._dictionaryName = dictionaryName;
            this._dictionaryKeyIgnoreCase = dictionaryKeyIgnoreCase;
            this._dictionaryPartitionKey = PartitionKeyPrefix + dictionaryName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDictionary" /> class.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        public TableDictionary(string dictionaryName, string tableName, string accountName, string keyValue, bool useHttps = true, bool dictionaryKeyIgnoreCase = false)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            this._tableStorage = new TableStorage(tableName, accountName, keyValue, useHttps);
            this._dictionaryName = dictionaryName;
            this._dictionaryKeyIgnoreCase = dictionaryKeyIgnoreCase;
            this._dictionaryPartitionKey = PartitionKeyPrefix + dictionaryName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDictionary" /> class.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        public TableDictionary(string dictionaryName, string tableName, CloudStorageAccount cloudStorageAccount, bool dictionaryKeyIgnoreCase = false)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            this._tableStorage = new TableStorage(tableName, cloudStorageAccount);
            this._dictionaryName = dictionaryName;
            this._dictionaryKeyIgnoreCase = dictionaryKeyIgnoreCase;
            this._dictionaryPartitionKey = PartitionKeyPrefix + dictionaryName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableDictionary"/> class.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        private TableDictionary(string dictionaryName, string tableName, bool dictionaryKeyIgnoreCase = false)
            : this(dictionaryName, tableName, StorageConstants.DevelopmentStorageConnectionString, dictionaryKeyIgnoreCase)
        {
        }

        /// <summary>
        /// Gets the number of elements contained in the dictionary.
        /// </summary>
        /// <value>The number of elements contained in the dictionary.</value>
        public int Count
        {
            get
            {
                return this
                    ._tableStorage
                    .InnerCloudTable
                    .ExecuteQuery(new TableQuery { SelectColumns = new[] { "RowKey" } }.Where(string.Format(FilterStringFormat, this._dictionaryName)))
                    .Count();
            }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.IDictionary" /> object is read-only.
        /// </summary>
        /// <value>true if the <see cref="T:System.Collections.IDictionary" /> object is read-only; otherwise, false.</value>
        public bool IsReadOnly
        {
            get
            {
                var sharedAccessPolicies = this._tableStorage.InnerCloudTable.GetPermissions().SharedAccessPolicies;

                if (sharedAccessPolicies != null && sharedAccessPolicies.Count > 0)
                {
                    return !sharedAccessPolicies.Any(i => i.Value.Permissions.HasFlag(SharedAccessTablePermissions.Add | SharedAccessTablePermissions.Delete | SharedAccessTablePermissions.Update));
                }

                return false;
            }
        }

        /// <summary>
        /// Gets an <see cref="T:System.Collections.ICollection" /> object containing the keys of the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <value>An <see cref="T:System.Collections.ICollection" /> object containing the keys of the <see cref="T:System.Collections.IDictionary" /> object.</value>
        public ICollection<string> Keys
        {
            get
            {
                return this
                    ._tableStorage
                    .InnerCloudTable
                    .ExecuteQuery(new TableQuery { SelectColumns = new[] { "RowKey" } }.Where(string.Format(FilterStringFormat, this._dictionaryName)))
                    .Select(i => i.RowKey.Substring(34))
                    .ToList();
            }
        }

        /// <summary>
        /// Gets an <see cref="T:System.Collections.ICollection" /> object containing the values in the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <value>An <see cref="T:System.Collections.ICollection" /> object containing the values in the <see cref="T:System.Collections.IDictionary" /> object.</value>
        public ICollection<object> Values
        {
            get
            {
                return this
                    ._tableStorage
                    .InnerCloudTable
                    .ExecuteQuery(new TableQuery().Where(string.Format(FilterStringFormat, this._dictionaryName)))
                    .Select(i => i.Properties[ValueKey].PropertyAsObject)
                    .ToList();
            }
        }

        /// <summary>
        /// Gets all KeyValuePair in the dictionary.
        /// </summary>
        /// <value>A list of KeyValuePair.</value>
        public List<KeyValuePair<string, object>> KeyValuePairs
        {
            get
            {
                return this
                    ._tableStorage
                    .InnerCloudTable
                    .ExecuteQuery(new TableQuery().Where(string.Format(FilterStringFormat, this._dictionaryName)))
                    .Select(i => new KeyValuePair<string, object>(i.RowKey.Substring(34), i.Properties[ValueKey].PropertyAsObject))
                    .ToList();
            }
        }

        /// <summary>
        /// Gets or sets the element with the specified key.
        /// </summary>
        /// <param name="key">The key of the element to get or set.</param>
        /// <returns>The element with the specified key.</returns>
        public object this[string key]
        {
            get
            {
                return this.GetValue(key);
            }

            set
            {
                this.AddOrUpdate(key, value);
            }
        }

        /// <summary>
        /// Checks whether the table dictionary exists.
        /// </summary>
        /// <returns>true if table exists; otherwise, false.</returns>
        public bool DictionaryExists()
        {
            return this._tableStorage.PartitionKeyExists(this._dictionaryPartitionKey);
        }

        /// <summary>
        /// Gets the value by the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The value object.</returns>
        public object GetValue(string key)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                throw new KeyNotFoundException();
            }

            return entity[ValueKey].PropertyAsObject;
        }

        /// <summary>
        /// Gets the value by the specified key.
        /// </summary>
        /// <typeparam name="T">Type of the value object.</typeparam>
        /// <param name="key">The key.</param>
        /// <returns>The value object.</returns>
        public T GetValue<T>(string key)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                throw new KeyNotFoundException();
            }

            return (T)entity[ValueKey].PropertyAsObject;
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="key">The key to add or update.</param>
        /// <param name="value">The value to add or update.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult AddOrUpdate(string key, object value)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = new DynamicTableEntity();

            entity[ValueKey] = EntityProperty.CreateEntityPropertyFromObject(value);

            return this._tableStorage.InsertOrReplace(entity, this._dictionaryPartitionKey, key);
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="item">The item to add or update.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult AddOrUpdate(KeyValuePair<string, object> item)
        {
            return this.AddOrUpdate(item.Key, item.Value);
        }

        /// <summary>
        /// Adds an element with the provided item.
        /// </summary>
        /// <param name="item">The item to add.</param>
        public void Add(KeyValuePair<string, object> item)
        {
            this.Add(item.Key, item.Value);
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="key">The to use as the key of the element to add.</param>
        /// <param name="value">The <see cref="T:System.Object" /> to use as the value of the element to add.</param>
        public void Add(string key, object value)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity != null)
            {
                throw new ArgumentException("An element with the same key already exists.");
            }

            entity = new DynamicTableEntity();

            entity[ValueKey] = EntityProperty.CreateEntityPropertyFromObject(value);

            this._tableStorage.Insert(entity, this._dictionaryPartitionKey, key);
        }

        /// <summary>
        /// Removes all elements from the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        public void Clear()
        {
            var entities = this
                ._tableStorage
                .InnerCloudTable
                .ExecuteQuery(new TableQuery().Where(string.Format(FilterStringFormat, this._dictionaryName)));

            this._tableStorage.Delete(entities);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.IDictionary" /> object contains an element.
        /// </summary>
        /// <param name="item">The item to check.</param>
        /// <returns>true if the <see cref="T:System.Collections.IDictionary" /> contains the element; otherwise, false.</returns>
        public bool Contains(KeyValuePair<string, object> item)
        {
            item.Key.ValidateTableDictionaryPropertyValue();

            var key = RowKeyPrefix + item.Key;

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                return false;
            }

            return item.Value.Equals(entity[ValueKey].PropertyAsObject);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.IDictionary" /> object contains an element with the specified key.
        /// </summary>
        /// <param name="key">The key to locate in the <see cref="T:System.Collections.IDictionary" /> object.</param>
        /// <returns>true if the <see cref="T:System.Collections.IDictionary" /> contains an element with the key; otherwise, false.</returns>
        public bool ContainsKey(string key)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            return this._tableStorage.EntityExists(this._dictionaryPartitionKey, key);
        }

        /// <summary>
        /// Removes the element from the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="item">The item to remove.</param>
        /// <returns>true if the element did already exist and was removed; otherwise false.</returns>
        public bool Remove(KeyValuePair<string, object> item)
        {
            item.Key.ValidateTableDictionaryPropertyValue();

            var key = RowKeyPrefix + item.Key;

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                return false;
            }

            if (item.Value.Equals(entity[ValueKey].PropertyAsObject))
            {
                this._tableStorage.Delete(entity);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <returns>true if the element did already exist and was removed; otherwise false.</returns>
        public bool Remove(string key)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            return this._tableStorage.Delete(this._dictionaryPartitionKey, key).HttpStatusCode != (int)HttpStatusCode.NotFound;
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">When this method returns, contains the value associated with the specified key, if the key is found; otherwise, the default value for the type of the <paramref name="value" /> parameter. This parameter is passed uninitialized.</param>
        /// <returns>true if the dictionary contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue(string key, out object value)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                value = null;
                return false;
            }
            else
            {
                value = entity[ValueKey].PropertyAsObject;
                return true;
            }
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="T">Type of the value object.</typeparam>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">When this method returns, contains the value associated with the specified key, if the key is found; otherwise, the default value for the type of the <paramref name="value" /> parameter. This parameter is passed uninitialized.</param>
        /// <returns>true if the dictionary contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue<T>(string key, out T value)
        {
            key.ValidateTableDictionaryPropertyValue();

            if (this._dictionaryKeyIgnoreCase)
            {
                key = key.ToLowerInvariant();
            }

            key = RowKeyPrefix + key;

            var entity = this._tableStorage.Retrieve<DynamicTableEntity>(this._dictionaryPartitionKey, key);

            if (entity == null)
            {
                value = default(T);
                return false;
            }
            else
            {
                value = (T)entity[ValueKey].PropertyAsObject;
                return true;
            }
        }

        /// <summary>
        /// Copies the elements of the dictionary to an <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
        /// </summary>
        /// <param name="array">he one-dimensional <see cref="T:System.Array" /> that is the destination of the elements copied from the dictionary. The <see cref="T:System.Array" /> must have zero-based indexing.</param>
        /// <param name="arrayIndex">The zero-based index in <paramref name="array" /> at which copying begins.</param>
        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            array.ValidateNull();

            if (arrayIndex < 0 || arrayIndex > array.Length)
            {
                throw new ArgumentOutOfRangeException("Array index is out of range");
            }

            int count = this.Count;

            if (array.Length - arrayIndex < count)
            {
                throw new ArgumentException("The number of elements in the source dictionary is greater than the available space from array index to the end of the destination array");
            }

            var keyValuePairs = this.KeyValuePairs;

            for (int i = 0; i < count; i++)
            {
                array[arrayIndex++] = new KeyValuePair<string, object>(keyValuePairs[i].Key, keyValuePairs[i].Value);
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>A IEnumerator{KeyValuePair{string, object}} that can be used to iterate through the collection.</returns>
        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return this.KeyValuePairs.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return (this.KeyValuePairs as IEnumerable).GetEnumerator();
        }
    }
}
