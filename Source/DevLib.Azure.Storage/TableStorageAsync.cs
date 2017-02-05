//-----------------------------------------------------------------------
// <copyright file="TableStorageAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents a Microsoft Azure table.
    /// </summary>
    public partial class TableStorage
    {
        /// <summary>
        /// Creates the table if it does not already exist.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if table was created; otherwise, false.</returns>
        public Task<bool> CreateIfNotExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudTable.CreateIfNotExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the table exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if table exists; otherwise, false.</returns>
        public Task<bool> TableExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudTable.ExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the table if it exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the table exists and was deleted; otherwise, false.</returns>
        public Task<bool> DeleteTableIfExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudTable.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the table dictionary if exists.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the table dictionary exists and was deleted; otherwise, false.</returns>
        public Task<bool> DeleteTableDictionaryIfExistsAsync(string dictionaryName, CancellationToken? cancellationToken = null)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            return Task.Run(async () =>
            {
                var entities = this
                    ._cloudTable
                    .ExecuteQuery(new TableQuery().Where(string.Format(TableDictionary.FilterStringFormat, dictionaryName)));

                return (await this.DeleteAsync(entities, cancellationToken ?? CancellationToken.None)).Any();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the entity exists.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if entity exists; otherwise, false.</returns>
        public Task<bool> EntityExistsAsync(string partitionKey, string rowKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            return Task.Run(async () =>
            {
                var entity = await this._cloudTable.ExecuteAsync(TableOperation.Retrieve(partitionKey, rowKey), cancellationToken ?? CancellationToken.None);

                return entity != null;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the partition key exists.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the partition key exists; otherwise, false.</returns>
        public Task<bool> PartitionKeyExistsAsync(string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                return this._cloudTable.ExecuteQuery(query).Any();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the row key exists.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the row key exists; otherwise, false.</returns>
        public Task<bool> RowKeyExistsAsync(string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                return this._cloudTable.ExecuteQuery(query).Any();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the table dictionary.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="dictionaryKeyIgnoreCase">true if dictionary key string ignore case; otherwise, false.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>TableDictionary instance.</returns>
        public Task<TableDictionary> GetTableDictionaryAsync(string dictionaryName, bool dictionaryKeyIgnoreCase = false, CancellationToken? cancellationToken = null)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            return Task.Run(
                () => new TableDictionary(dictionaryName, this, dictionaryKeyIgnoreCase),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves the contents of the given entity in a table.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The entity instance.</returns>
        public Task<TElement> RetrieveAsync<TElement>(string partitionKey, string rowKey, CancellationToken? cancellationToken = null) where TElement : ITableEntity, new()
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            return Task.Run(async () =>
            {
                var result = (await this._cloudTable.ExecuteAsync(TableOperation.Retrieve<TElement>(partitionKey, rowKey), cancellationToken ?? CancellationToken.None)).Result;

                return (TElement)result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves the contents of the given entity in a table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The entity instance.</returns>
        public Task<object> RetrieveAsync(string partitionKey, string rowKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            return Task.Run(
                async () => (await this._cloudTable.ExecuteAsync(TableOperation.Retrieve(partitionKey, rowKey), cancellationToken ?? CancellationToken.None)).Result,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>An enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public Task<List<DynamicTableEntity>> RetrieveByPartitionKeyAsync(string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                return this._cloudTable.ExecuteQuery(query).ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the table by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the table.</returns>
        public Task<int> CountByPartitionKeyAsync(string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                return this._cloudTable.ExecuteQuery(query).Count();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the table by the row key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the table.</returns>
        public Task<int> CountByRowKeyAsync(string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                return this._cloudTable.ExecuteQuery(query).Count();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the entities in the table.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of all the entities in the table.</returns>
        public Task<List<DynamicTableEntity>> ListEntitiesAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudTable.ExecuteQuery(new TableQuery()).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the table.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the table.</returns>
        public Task<int> EntitiesCountAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudTable.ExecuteQuery(new TableQuery()).Count(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the partition keys in the table.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of all the partition keys in the table.</returns>
        public Task<List<string>> ListPartitionKeysAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this
                ._cloudTable
                .ExecuteQuery(new TableQuery { SelectColumns = new[] { "PartitionKey" } })
                .Select(i => i.PartitionKey)
                .Distinct()
                .ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the row keys in the table.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of all the row keys in the table.</returns>
        public Task<List<string>> ListRowKeysAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this
                ._cloudTable
                .ExecuteQuery(new TableQuery { SelectColumns = new[] { "RowKey" } })
                .Select(i => i.RowKey)
                .Distinct()
                .ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the row keys in the table by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of all the row keys in the table.</returns>
        public Task<List<string>> ListRowKeysByPartitionKeyAsync(string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery { SelectColumns = new[] { "RowKey" } }.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                return this
                ._cloudTable
                .ExecuteQuery(query)
                .Select(i => i.RowKey)
                .Distinct()
                .ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the partition keys in the table by the row key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of all the partition keys in the table.</returns>
        public Task<List<string>> ListPartitionKeysByRowKeyAsync(string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery { SelectColumns = new[] { "PartitionKey" } }.Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                return this
                ._cloudTable
                .ExecuteQuery(query)
                .Select(i => i.PartitionKey)
                .Distinct()
                .ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves an enumerable collection of ITableEntity objects by the partition key.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>An enumerable collection, specialized for type TElement, of the results of executing the query.</returns>
        public Task<List<TElement>> RetrieveByPartitionKeyAsync<TElement>(string partitionKey, CancellationToken? cancellationToken = null) where TElement : ITableEntity, new()
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery<TElement>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                return this._cloudTable.ExecuteQuery<TElement>(query).ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects by the partition key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>An enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public Task<List<DynamicTableEntity>> RetrieveByRowKeyAsync(string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                return this._cloudTable.ExecuteQuery(query).ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Retrieves an enumerable collection of ITableEntity objects by the partition key.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns> An enumerable collection, specialized for type TElement, of the results of executing the query.</returns>
        public Task<List<TElement>> RetrieveByRowKeyAsync<TElement>(string rowKey, CancellationToken? cancellationToken = null) where TElement : ITableEntity, new()
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery<TElement>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                return this._cloudTable.ExecuteQuery<TElement>(query).ToList();
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entity into the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be inserted into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> InsertAsync(ITableEntity entity, string partitionKey = null, string rowKey = null, bool echoContent = false, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            if (partitionKey != null)
            {
                partitionKey.ValidateTablePropertyValue();
                entity.PartitionKey = partitionKey;
            }

            if (rowKey != null)
            {
                rowKey.ValidateTablePropertyValue();
                entity.RowKey = rowKey;
            }

            return this._cloudTable.ExecuteAsync(TableOperation.Insert(entity, echoContent), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertAsync(IEnumerable<ITableEntity> entities, bool echoContent = false, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.Insert(entity, echoContent);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertByPartitionKeyAsync(IEnumerable<ITableEntity> entities, string partitionKey, bool echoContent = false, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.PartitionKey = partitionKey;
                }

                return this.InsertAsync(entities, echoContent, cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertByRowKeyAsync(IEnumerable<ITableEntity> entities, string rowKey, bool echoContent = false, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.RowKey = rowKey;
                }

                return this.InsertAsync(entities, echoContent, cancellationToken);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entity into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be insert or merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> InsertOrMergeAsync(ITableEntity entity, string partitionKey = null, string rowKey = null, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            if (partitionKey != null)
            {
                partitionKey.ValidateTablePropertyValue();
                entity.PartitionKey = partitionKey;
            }

            if (rowKey != null)
            {
                rowKey.ValidateTablePropertyValue();
                entity.RowKey = rowKey;
            }

            return this._cloudTable.ExecuteAsync(TableOperation.InsertOrMerge(entity), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrMergeAsync(IEnumerable<ITableEntity> entities, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.InsertOrMerge(entity);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrMergeByPartitionKeyAsync(IEnumerable<ITableEntity> entities, string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.PartitionKey = partitionKey;
                }

                return this.InsertOrMergeAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrMergeByRowKeyAsync(IEnumerable<ITableEntity> entities, string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.RowKey = rowKey;
                }

                return this.InsertOrMergeAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entity into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be insert or replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> InsertOrReplaceAsync(ITableEntity entity, string partitionKey = null, string rowKey = null, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            if (partitionKey != null)
            {
                partitionKey.ValidateTablePropertyValue();
                entity.PartitionKey = partitionKey;
            }

            if (rowKey != null)
            {
                rowKey.ValidateTablePropertyValue();
                entity.RowKey = rowKey;
            }

            return this._cloudTable.ExecuteAsync(TableOperation.InsertOrReplace(entity), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrReplaceAsync(IEnumerable<ITableEntity> entities, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.InsertOrReplace(entity);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrReplaceByPartitionKeyAsync(IEnumerable<ITableEntity> entities, string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.PartitionKey = partitionKey;
                }

                return this.InsertOrReplaceAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> InsertOrReplaceByRowKeyAsync(IEnumerable<ITableEntity> entities, string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.RowKey = rowKey;
                }

                return this.InsertOrReplaceAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Merges the contents of the given entity with the existing entity in a table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> MergeAsync(ITableEntity entity, string partitionKey = null, string rowKey = null, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            if (partitionKey != null)
            {
                partitionKey.ValidateTablePropertyValue();
                entity.PartitionKey = partitionKey;
            }

            if (rowKey != null)
            {
                rowKey.ValidateTablePropertyValue();
                entity.RowKey = rowKey;
            }

            return this._cloudTable.ExecuteAsync(TableOperation.Merge(entity), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> MergeAsync(IEnumerable<ITableEntity> entities, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.Merge(entity);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> MergeByPartitionKeyAsync(IEnumerable<ITableEntity> entities, string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.PartitionKey = partitionKey;
                }

                return this.MergeAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> MergeByRowKeyAsync(IEnumerable<ITableEntity> entities, string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.RowKey = rowKey;
                }

                return this.MergeAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Replaces the contents of the given entity in the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> ReplaceAsync(ITableEntity entity, string partitionKey = null, string rowKey = null, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            if (partitionKey != null)
            {
                partitionKey.ValidateTablePropertyValue();
                entity.PartitionKey = partitionKey;
            }

            if (rowKey != null)
            {
                rowKey.ValidateTablePropertyValue();
                entity.RowKey = rowKey;
            }

            return this._cloudTable.ExecuteAsync(TableOperation.Replace(entity), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> ReplaceAsync(IEnumerable<ITableEntity> entities, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.Replace(entity);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> ReplacePartitionKeyAsync(IEnumerable<ITableEntity> entities, string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.PartitionKey = partitionKey;
                }

                return this.ReplaceAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> ReplaceRowKeyAsync(IEnumerable<ITableEntity> entities, string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            return Task.Run(() =>
            {
                foreach (var entity in entities)
                {
                    entity.RowKey = rowKey;
                }

                return this.ReplaceAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the given entity from the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be delete into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> DeleteAsync(ITableEntity entity, CancellationToken? cancellationToken = null)
        {
            entity.ValidateNull();

            return this._cloudTable.ExecuteAsync(TableOperation.Delete(entity), cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the given entities from the table.
        /// </summary>
        /// <param name="entities">The entities to be delete into the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> DeleteAsync(IEnumerable<ITableEntity> entities, CancellationToken? cancellationToken = null)
        {
            entities.ValidateNull();

            return Task.Run(async () =>
            {
                var result = new List<TableResult>();

                var batchs = this.GroupBatchByPartitionKey(entities, BatchOperationSize);

                foreach (var batch in batchs)
                {
                    var batchOperation = new TableBatchOperation();

                    foreach (var entity in batch)
                    {
                        batchOperation.Delete(entity);
                    }

                    result.AddRange(await this._cloudTable.ExecuteBatchAsync(batchOperation, cancellationToken ?? CancellationToken.None));
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the entity from the table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<TableResult> DeleteAsync(string partitionKey, string rowKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var entity = this._cloudTable.Execute(TableOperation.Retrieve(partitionKey, rowKey)).Result as ITableEntity;

                if (entity != null)
                {
                    return this._cloudTable.ExecuteAsync(TableOperation.Delete(entity), cancellationToken ?? CancellationToken.None);
                }
                else
                {
                    return Task.FromResult(new TableResult { HttpStatusCode = (int)HttpStatusCode.NotFound });
                }
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes all the entities from the table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> DeleteByPartitionKeyAsync(string partitionKey, CancellationToken? cancellationToken = null)
        {
            partitionKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                var entities = this._cloudTable.ExecuteQuery(query);

                return this.DeleteAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes all the entities from the table.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public Task<List<TableResult>> DeleteByRowKeyAsync(string rowKey, CancellationToken? cancellationToken = null)
        {
            rowKey.ValidateTablePropertyValue();

            return Task.Run(() =>
            {
                var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

                var entities = this._cloudTable.ExecuteQuery(query);

                return this.DeleteAsync(entities, cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Removes all elements from the table.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current TableStorage instance.</returns>
        public Task<TableStorage> ClearAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var entities = this
                    ._cloudTable
                    .ExecuteQuery(new TableQuery());

                await this.DeleteAsync(entities, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Executes a query on the table and returns an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects.
        /// </summary>
        /// <param name="query">A Microsoft.WindowsAzure.Storage.Table.TableQuery representing the query to execute.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public Task<List<DynamicTableEntity>> ExecuteQueryAsync(TableQuery query, CancellationToken? cancellationToken = null)
        {
            query.ValidateNull();

            return Task.Run(
                () => this._cloudTable.ExecuteQuery(query).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Executes a query on the table.
        /// </summary>
        /// <typeparam name="TElement">The entity type of the query.</typeparam>
        /// <param name="query">A TableQuery instance specifying the table to query and the query parameters to use, specialized for a type TElement.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list, specialized for type TElement, of the results of executing the query.</returns>
        public Task<List<TElement>> ExecuteQueryAsync<TElement>(TableQuery<TElement> query, CancellationToken? cancellationToken = null) where TElement : ITableEntity, new()
        {
            query.ValidateNull();

            return Task.Run(
                () => this._cloudTable.ExecuteQuery<TElement>(query).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a shared access signature for the table.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetTableSasAsync(SharedAccessTablePermissions permissions = SharedAccessTablePermissions.Query, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(() => this._cloudTable.GetSharedAccessSignature(new SharedAccessTablePolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            }),
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a shared access signature for the table with query only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetTableSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetTableSasAsync(SharedAccessTablePermissions.Query, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Gets the table URI for the primary location.
        /// </summary>
        /// <returns>The table Uri.</returns>
        public Task<Uri> GetTableUriAsync()
        {
            return Task.FromResult(this._cloudTable.Uri);
        }

        /// <summary>
        /// Gets the table Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The table Uri with SAS.</returns>
        public Task<Uri> GetTableUriWithSasAsync(SharedAccessTablePermissions permissions = SharedAccessTablePermissions.Query, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(() =>
            {
                var uriBuilder = new UriBuilder(this._cloudTable.Uri);

                uriBuilder.Query = this.GetTableSas(permissions, startTime, endTime).TrimStart('?');

                return uriBuilder.Uri;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the table Uri with query only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The table Uri with query only SAS.</returns>
        public Task<Uri> GetTableUriWithSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetTableUriWithSasAsync(SharedAccessTablePermissions.Query, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }
    }
}
