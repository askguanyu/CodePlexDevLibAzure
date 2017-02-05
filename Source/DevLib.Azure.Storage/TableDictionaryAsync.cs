//-----------------------------------------------------------------------
// <copyright file="TableDictionaryAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents a Microsoft Azure table as a dictionary.
    /// </summary>
    public partial class TableDictionary
    {
        /// <summary>
        /// Checks whether the table dictionary exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if table exists; otherwise, false.</returns>
        public Task<bool> DictionaryExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._tableStorage.PartitionKeyExistsAsync(this._dictionaryPartitionKey, cancellationToken);
        }

        /// <summary>
        /// Gets the value by the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The value object.</returns>
        public Task<object> GetValueAsync(string key, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                var entity = await this._tableStorage.RetrieveAsync<DynamicTableEntity>(this._dictionaryPartitionKey, key, cancellationToken);

                if (entity == null)
                {
                    throw new KeyNotFoundException();
                }

                return entity[ValueKey].PropertyAsObject;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the value by the specified key.
        /// </summary>
        /// <typeparam name="T">Type of the value object.</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The value object.</returns>
        public Task<T> GetValueAsync<T>(string key, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                var entity = await this._tableStorage.RetrieveAsync<DynamicTableEntity>(this._dictionaryPartitionKey, key, cancellationToken);

                if (entity == null)
                {
                    throw new KeyNotFoundException();
                }

                return (T)entity[ValueKey].PropertyAsObject;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="key">The key to add or update.</param>
        /// <param name="value">The value to add or update.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> AddOrUpdateAsync(string key, object value, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(() =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                var entity = new DynamicTableEntity();

                entity[ValueKey] = EntityProperty.CreateEntityPropertyFromObject(value);

                return this._tableStorage.InsertOrReplaceAsync(entity, this._dictionaryPartitionKey, key, cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="item">The item to add or update.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> AddOrUpdateAsync(KeyValuePair<string, object> item, CancellationToken? cancellationToken = null)
        {
            return this.AddOrUpdateAsync(item.Key, item.Value, cancellationToken);
        }

        /// <summary>
        /// Adds an element with the provided item.
        /// </summary>
        /// <param name="item">The item to add.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>Current task.</returns>
        public Task AddAsync(KeyValuePair<string, object> item, CancellationToken? cancellationToken = null)
        {
            return this.AddAsync(item.Key, item.Value, cancellationToken);
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="key">The to use as the key of the element to add.</param>
        /// <param name="value">The <see cref="T:System.Object" /> to use as the value of the element to add.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>Current task.</returns>
        public Task AddAsync(string key, object value, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                var entity = await this._tableStorage.RetrieveAsync<DynamicTableEntity>(this._dictionaryPartitionKey, key, cancellationToken);

                if (entity != null)
                {
                    throw new ArgumentException("An element with the same key already exists.");
                }

                entity = new DynamicTableEntity();

                entity[ValueKey] = EntityProperty.CreateEntityPropertyFromObject(value);

                await this._tableStorage.InsertAsync(entity, this._dictionaryPartitionKey, key, cancellationToken: cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Removes all elements from the table dictionary.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>Current task.</returns>
        public Task ClearAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var entities = this
                    ._tableStorage
                    .InnerCloudTable
                    .ExecuteQuery(new TableQuery().Where(string.Format(FilterStringFormat, this._dictionaryName)));

                await this._tableStorage.DeleteAsync(entities, cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.IDictionary" /> object contains an element.
        /// </summary>
        /// <param name="item">The item to check.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the <see cref="T:System.Collections.IDictionary" /> contains the element; otherwise, false.</returns>
        public Task<bool> ContainsAsync(KeyValuePair<string, object> item, CancellationToken? cancellationToken = null)
        {
            item.Key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                var key = RowKeyPrefix + item.Key;

                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                var entity = await this._tableStorage.RetrieveAsync<DynamicTableEntity>(this._dictionaryPartitionKey, key, cancellationToken);

                if (entity == null)
                {
                    return false;
                }

                return item.Value.Equals(entity[ValueKey].PropertyAsObject);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.IDictionary" /> object contains an element with the specified key.
        /// </summary>
        /// <param name="key">The key to locate in the <see cref="T:System.Collections.IDictionary" /> object.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the <see cref="T:System.Collections.IDictionary" /> contains an element with the key; otherwise, false.</returns>
        public Task<bool> ContainsKeyAsync(string key, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(() =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                return this._tableStorage.EntityExistsAsync(this._dictionaryPartitionKey, key, cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Removes the element from the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="item">The item to remove.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the element did already exist and was removed; otherwise false.</returns>
        public Task<bool> RemoveAsync(KeyValuePair<string, object> item, CancellationToken? cancellationToken = null)
        {
            item.Key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                var key = RowKeyPrefix + item.Key;

                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                var entity = await this._tableStorage.RetrieveAsync<DynamicTableEntity>(this._dictionaryPartitionKey, key, cancellationToken);

                if (entity == null)
                {
                    return false;
                }

                if (item.Value.Equals(entity[ValueKey].PropertyAsObject))
                {
                    await this._tableStorage.DeleteAsync(entity, cancellationToken);
                    return true;
                }

                return false;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.IDictionary" /> object.
        /// </summary>
        /// <param name="key">The key of the element to remove.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the element did already exist and was removed; otherwise false.</returns>
        public Task<bool> RemoveAsync(string key, CancellationToken? cancellationToken = null)
        {
            key.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                if (this._dictionaryKeyIgnoreCase)
                {
                    key = key.ToLowerInvariant();
                }

                key = RowKeyPrefix + key;

                return (await this._tableStorage.DeleteAsync(this._dictionaryPartitionKey, key, cancellationToken)).HttpStatusCode != (int)HttpStatusCode.NotFound;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Copies the elements of the dictionary to an <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
        /// </summary>
        /// <param name="array">he one-dimensional <see cref="T:System.Array" /> that is the destination of the elements copied from the dictionary. The <see cref="T:System.Array" /> must have zero-based indexing.</param>
        /// <param name="arrayIndex">The zero-based index in <paramref name="array" /> at which copying begins.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>Current task.</returns>
        public Task CopyToAsync(KeyValuePair<string, object>[] array, int arrayIndex, CancellationToken? cancellationToken = null)
        {
            array.ValidateNull();

            return Task.Run(() =>
            {
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
            },
            cancellationToken ?? CancellationToken.None);
        }
    }
}
