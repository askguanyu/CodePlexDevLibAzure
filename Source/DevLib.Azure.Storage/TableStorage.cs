//-----------------------------------------------------------------------
// <copyright file="TableStorage.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents a Microsoft Azure table.
    /// </summary>
    public class TableStorage
    {
        /// <summary>
        /// Field _cloudTable.
        /// </summary>
        private readonly CloudTable _cloudTable;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public TableStorage(string tableName, string connectionString, bool createIfNotExists = true)
        {
            tableName.ValidateTableName();
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(cloudTableClient);

            this._cloudTable = cloudTableClient.GetTableReference(tableName);

            if (createIfNotExists)
            {
                this._cloudTable.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public TableStorage(string tableName, string accountName, string keyValue, bool useHttps = true, bool createIfNotExists = true)
        {
            tableName.ValidateTableName();
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(cloudTableClient);

            this._cloudTable = cloudTableClient.GetTableReference(tableName);

            if (createIfNotExists)
            {
                this._cloudTable.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage" /> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public TableStorage(string tableName, CloudStorageAccount cloudStorageAccount, bool createIfNotExists = true)
        {
            tableName.ValidateTableName();
            cloudStorageAccount.ValidateNull();

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(cloudTableClient);

            this._cloudTable = cloudTableClient.GetTableReference(tableName);

            if (createIfNotExists)
            {
                this._cloudTable.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableStorage"/> class.
        /// </summary>
        /// <param name="cloudTable">The CloudTable instance.</param>
        internal TableStorage(CloudTable cloudTable)
        {
            this._cloudTable = cloudTable;
        }

        /// <summary>
        /// Gets the inner cloud table.
        /// </summary>
        /// <value>The inner cloud table.</value>
        public CloudTable InnerCloudTable
        {
            get
            {
                return this._cloudTable;
            }
        }

        /// <summary>
        /// Creates the table if it does not already exist.
        /// </summary>
        /// <returns>TableStorage instance.</returns>
        public TableStorage CreateIfNotExists()
        {
            this._cloudTable.CreateIfNotExists();

            return this;
        }

        /// <summary>
        /// Checks whether the table exists.
        /// </summary>
        /// <returns>true if table exists; otherwise, false.</returns>
        public bool TableExists()
        {
            return this._cloudTable.Exists();
        }

        /// <summary>
        /// Deletes the table if it exists.
        /// </summary>
        /// <returns>true if the table exists and was deleted; otherwise, false.</returns>
        public bool DeleteTableIfExists()
        {
            return this._cloudTable.DeleteIfExists();
        }

        /// <summary>
        /// Deletes the table dictionary if exists.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <returns>true if the table dictionary exists and was deleted; otherwise, false.</returns>
        public bool DeleteTableDictionaryIfExists(string dictionaryName)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();

            var entities = this
                ._cloudTable
                .ExecuteQuery(new TableQuery().Where(string.Format(TableDictionary.FilterStringFormat, dictionaryName)));

            return this.Delete(entities).Any();
        }

        /// <summary>
        /// Checks whether the entity exists.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>true if entity exists; otherwise, false.</returns>
        public bool EntityExists(string partitionKey, string rowKey)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            var entity = this._cloudTable.Execute(TableOperation.Retrieve(partitionKey, rowKey)).Result;

            return entity != null;
        }

        /// <summary>
        /// Checks whether the partition key exists.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>true if the partition key exists; otherwise, false.</returns>
        public bool PartitionKeyExists(string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return this._cloudTable.ExecuteQuery(query).Any();
        }

        /// <summary>
        /// Checks whether the row key exists.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>true if the row key exists; otherwise, false.</returns>
        public bool RowKeyExists(string rowKey)
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return this._cloudTable.ExecuteQuery(query).Any();
        }

        /// <summary>
        /// Gets the table dictionary.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="dictionaryKeyIgnoreCase">true if dictionary key string ignore case; otherwise, false.</param>
        /// <returns>TableDictionary instance.</returns>
        public TableDictionary GetTableDictionary(string dictionaryName, bool dictionaryKeyIgnoreCase = false)
        {
            return new TableDictionary(dictionaryName, this, dictionaryKeyIgnoreCase);
        }

        /// <summary>
        /// Retrieves the contents of the given entity in a table.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>The entity instance.</returns>
        public TElement Retrieve<TElement>(string partitionKey, string rowKey) where TElement : ITableEntity, new()
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            var result = this._cloudTable.Execute(TableOperation.Retrieve<TElement>(partitionKey, rowKey)).Result;

            return (TElement)result;
        }

        /// <summary>
        /// Retrieves the contents of the given entity in a table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>The entity instance.</returns>
        public object Retrieve(string partitionKey, string rowKey)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            return this._cloudTable.Execute(TableOperation.Retrieve(partitionKey, rowKey)).Result;
        }

        /// <summary>
        /// Retrieves an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>An enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public List<DynamicTableEntity> RetrieveByPartitionKey(string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return this._cloudTable.ExecuteQuery(query).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the table by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>The number of elements contained in the table.</returns>
        public int CountByPartitionKey(string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return this._cloudTable.ExecuteQuery(query).Count();
        }

        /// <summary>
        /// Gets the number of elements contained in the table by the row key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>The number of elements contained in the table.</returns>
        public int CountByRowKey(string rowKey)
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return this._cloudTable.ExecuteQuery(query).Count();
        }

        /// <summary>
        /// Returns a list of all the entities in the table.
        /// </summary>
        /// <returns>A list of all the entities in the table.</returns>
        public List<DynamicTableEntity> ListEntities()
        {
            return this._cloudTable.ExecuteQuery(new TableQuery()).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the table.
        /// </summary>
        /// <returns>The number of elements contained in the table.</returns>
        public int EntitiesCount()
        {
            return this._cloudTable.ExecuteQuery(new TableQuery()).Count();
        }

        /// <summary>
        /// Returns a list of all the partition keys in the table.
        /// </summary>
        /// <returns>A list of all the partition keys in the table.</returns>
        public List<string> ListPartitionKeys()
        {
            return this
                ._cloudTable
                .ExecuteQuery(new TableQuery { SelectColumns = new[] { "PartitionKey" } })
                .Select(i => i.PartitionKey)
                .Distinct()
                .ToList();
        }

        /// <summary>
        /// Returns a list of all the row keys in the table.
        /// </summary>
        /// <returns>A list of all the row keys in the table.</returns>
        public List<string> ListRowKeys()
        {
            return this
                ._cloudTable
                .ExecuteQuery(new TableQuery { SelectColumns = new[] { "RowKey" } })
                .Select(i => i.RowKey)
                .Distinct()
                .ToList();
        }

        /// <summary>
        /// Returns a list of all the row keys in the table by the partition key.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of all the row keys in the table.</returns>
        public List<string> ListRowKeysByPartitionKey(string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery { SelectColumns = new[] { "RowKey" } }.Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return this
                ._cloudTable
                .ExecuteQuery(query)
                .Select(i => i.RowKey)
                .Distinct()
                .ToList();
        }

        /// <summary>
        /// Returns a list of all the partition keys in the table by the row key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of all the partition keys in the table.</returns>
        public List<string> ListPartitionKeysByRowKey(string rowKey)
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery { SelectColumns = new[] { "PartitionKey" } }.Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return this
                ._cloudTable
                .ExecuteQuery(query)
                .Select(i => i.PartitionKey)
                .Distinct()
                .ToList();
        }

        /// <summary>
        /// Retrieves an enumerable collection of ITableEntity objects by the partition key.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>An enumerable collection, specialized for type TElement, of the results of executing the query.</returns>
        public List<TElement> RetrieveByPartitionKey<TElement>(string partitionKey) where TElement : ITableEntity, new()
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery<TElement>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            return this._cloudTable.ExecuteQuery<TElement>(query).ToList();
        }

        /// <summary>
        /// Retrieves an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects by the partition key.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>An enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public List<DynamicTableEntity> RetrieveByRowKey(string rowKey)
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return this._cloudTable.ExecuteQuery(query).ToList();
        }

        /// <summary>
        /// Retrieves an enumerable collection of ITableEntity objects by the partition key.
        /// </summary>
        /// <typeparam name="TElement">The type of the element.</typeparam>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns> An enumerable collection, specialized for type TElement, of the results of executing the query.</returns>
        public List<TElement> RetrieveByRowKey<TElement>(string rowKey) where TElement : ITableEntity, new()
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery<TElement>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            return this._cloudTable.ExecuteQuery<TElement>(query).ToList();
        }

        /// <summary>
        /// Inserts the given entity into the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be inserted into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult Insert(ITableEntity entity, string partitionKey = null, string rowKey = null, bool echoContent = false)
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

            return this._cloudTable.Execute(TableOperation.Insert(entity, echoContent));
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> Insert(IEnumerable<ITableEntity> entities, bool echoContent = false)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.Insert(entity, echoContent);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertByPartitionKey(IEnumerable<ITableEntity> entities, string partitionKey, bool echoContent = false)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.PartitionKey = partitionKey;
            }

            return this.Insert(entities, echoContent);
        }

        /// <summary>
        /// Executes a batch inserts operation on the table.
        /// </summary>
        /// <param name="entities">The entities to be inserted into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <param name="echoContent">true if the message payload should be returned in the response to the insert operation. false otherwise.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertByRowKey(IEnumerable<ITableEntity> entities, string rowKey, bool echoContent = false)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.RowKey = rowKey;
            }

            return this.Insert(entities, echoContent);
        }

        /// <summary>
        /// Inserts the given entity into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be insert or merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult InsertOrMerge(ITableEntity entity, string partitionKey = null, string rowKey = null)
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

            return this._cloudTable.Execute(TableOperation.InsertOrMerge(entity));
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrMerge(IEnumerable<ITableEntity> entities)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.InsertOrMerge(entity);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrMergeByPartitionKey(IEnumerable<ITableEntity> entities, string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.PartitionKey = partitionKey;
            }

            return this.InsertOrMerge(entities);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are merged with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or merge into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrMergeByRowKey(IEnumerable<ITableEntity> entities, string rowKey)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.RowKey = rowKey;
            }

            return this.InsertOrMerge(entities);
        }

        /// <summary>
        /// Inserts the given entity into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be insert or replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult InsertOrReplace(ITableEntity entity, string partitionKey = null, string rowKey = null)
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

            return this._cloudTable.Execute(TableOperation.InsertOrReplace(entity));
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrReplace(IEnumerable<ITableEntity> entities)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.InsertOrReplace(entity);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrReplaceByPartitionKey(IEnumerable<ITableEntity> entities, string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.PartitionKey = partitionKey;
            }

            return this.InsertOrReplace(entities);
        }

        /// <summary>
        /// Inserts the given entities into the table if the entity does not exist;
        /// If the entity does exist then its contents are replaced with the provided entity.
        /// </summary>
        /// <param name="entities">The entities to be insert or replace into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> InsertOrReplaceByRowKey(IEnumerable<ITableEntity> entities, string rowKey)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.RowKey = rowKey;
            }

            return this.InsertOrReplace(entities);
        }

        /// <summary>
        /// Merges the contents of the given entity with the existing entity in a table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult Merge(ITableEntity entity, string partitionKey = null, string rowKey = null)
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

            return this._cloudTable.Execute(TableOperation.Merge(entity));
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> Merge(IEnumerable<ITableEntity> entities)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.Merge(entity);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> MergeByPartitionKey(IEnumerable<ITableEntity> entities, string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.PartitionKey = partitionKey;
            }

            return this.Merge(entities);
        }

        /// <summary>
        /// Merges the contents of the given entities with the existing entity in a table.
        /// </summary>
        /// <param name="entities">The entities to be merge into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> MergeByRowKey(IEnumerable<ITableEntity> entities, string rowKey)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.RowKey = rowKey;
            }

            return this.Merge(entities);
        }

        /// <summary>
        /// Replaces the contents of the given entity in the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult Replace(ITableEntity entity, string partitionKey = null, string rowKey = null)
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

            return this._cloudTable.Execute(TableOperation.Replace(entity));
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> Replace(IEnumerable<ITableEntity> entities)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.Replace(entity);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> ReplacePartitionKey(IEnumerable<ITableEntity> entities, string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.PartitionKey = partitionKey;
            }

            return this.Replace(entities);
        }

        /// <summary>
        ///  Replaces the contents of the given entities in the table.
        /// </summary>
        /// <param name="entities">The entities to be replace into the table.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> ReplaceRowKey(IEnumerable<ITableEntity> entities, string rowKey)
        {
            rowKey.ValidateTablePropertyValue();
            entities.ValidateNull();

            foreach (var entity in entities)
            {
                entity.RowKey = rowKey;
            }

            return this.Replace(entities);
        }

        /// <summary>
        /// Deletes the given entity from the table.
        /// </summary>
        /// <param name="entity">The Microsoft.WindowsAzure.Storage.Table.ITableEntity object to be delete into the table.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult Delete(ITableEntity entity)
        {
            entity.ValidateNull();

            return this._cloudTable.Execute(TableOperation.Delete(entity));
        }

        /// <summary>
        /// Deletes the given entities from the table.
        /// </summary>
        /// <param name="entities">The entities to be delete into the table.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> Delete(IEnumerable<ITableEntity> entities)
        {
            entities.ValidateNull();

            var result = new List<TableResult>();

            var batchs = this.GroupBatch(entities, 100);

            foreach (var batch in batchs)
            {
                var batchOperation = new TableBatchOperation();

                foreach (var entity in batch)
                {
                    batchOperation.Delete(entity);
                }

                result.AddRange(this._cloudTable.ExecuteBatch(batchOperation));
            }

            return result;
        }

        /// <summary>
        /// Deletes the entity from the table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public TableResult Delete(string partitionKey, string rowKey)
        {
            partitionKey.ValidateTablePropertyValue();
            rowKey.ValidateTablePropertyValue();

            var entity = this._cloudTable.Execute(TableOperation.Retrieve(partitionKey, rowKey)).Result as ITableEntity;

            if (entity != null)
            {
                return this.Delete(entity);
            }
            else
            {
                return new TableResult { HttpStatusCode = (int)HttpStatusCode.NotFound };
            }
        }

        /// <summary>
        /// Deletes all the entities from the table.
        /// </summary>
        /// <param name="partitionKey">A string containing the partition key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> DeleteByPartitionKey(string partitionKey)
        {
            partitionKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            var entities = this._cloudTable.ExecuteQuery(query);

            return this.Delete(entities);
        }

        /// <summary>
        /// Deletes all the entities from the table.
        /// </summary>
        /// <param name="rowKey">A string containing the row key of the entity to retrieve.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.TableResult object.</returns>
        public List<TableResult> DeleteByRowKey(string rowKey)
        {
            rowKey.ValidateTablePropertyValue();

            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            var entities = this._cloudTable.ExecuteQuery(query);

            return this.Delete(entities);
        }

        /// <summary>
        /// Executes a query on the table and returns an enumerable collection of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects.
        /// </summary>
        /// <param name="query">A Microsoft.WindowsAzure.Storage.Table.TableQuery representing the query to execute.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity objects, representing table entities returned by the query.</returns>
        public List<DynamicTableEntity> ExecuteQuery(TableQuery query)
        {
            query.ValidateNull();

            return this._cloudTable.ExecuteQuery(query).ToList();
        }

        /// <summary>
        /// Executes a query on the table.
        /// </summary>
        /// <typeparam name="TElement">The entity type of the query.</typeparam>
        /// <param name="query">A TableQuery instance specifying the table to query and the query parameters to use, specialized for a type TElement.</param>
        /// <returns>A list, specialized for type TElement, of the results of executing the query.</returns>
        public List<TElement> ExecuteQuery<TElement>(TableQuery<TElement> query) where TElement : ITableEntity, new()
        {
            query.ValidateNull();

            return this._cloudTable.ExecuteQuery<TElement>(query).ToList();
        }

        /// <summary>
        /// Returns a shared access signature for the table.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetSAS(SharedAccessTablePermissions permissions = SharedAccessTablePermissions.Query, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this._cloudTable.GetSharedAccessSignature(new SharedAccessTablePolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Returns a shared access signature for the table with query only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetSAS(SharedAccessTablePermissions.Query, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the table URI for the primary location.
        /// </summary>
        /// <returns>The table Uri.</returns>
        public Uri GetTableUri()
        {
            return this._cloudTable.Uri;
        }

        /// <summary>
        /// Gets the table Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The table Uri with SAS.</returns>
        public Uri GetTableUriWithSAS(SharedAccessTablePermissions permissions = SharedAccessTablePermissions.Query, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var uriBuilder = new UriBuilder(this._cloudTable.Uri);

            uriBuilder.Query = this.GetSAS(permissions, startTime, endTime).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the table Uri with query only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The table Uri with query only SAS.</returns>
        public Uri GetTableUriWithSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetTableUriWithSAS(SharedAccessTablePermissions.Query, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Sets the default retry.
        /// </summary>
        /// <param name="cloudTableClient">The CloudTableClient instance.</param>
        private void SetDefaultRetryIfNotExists(CloudTableClient cloudTableClient)
        {
            if (cloudTableClient.DefaultRequestOptions == null)
            {
                cloudTableClient.DefaultRequestOptions = new TableRequestOptions();
            }

            if (cloudTableClient.DefaultRequestOptions.RetryPolicy == null)
            {
                cloudTableClient.DefaultRequestOptions.RetryPolicy = StorageConstants.DefaultExponentialRetry;
            }
        }

        /// <summary>
        /// Returns a list of IEnumerable{T} that contains the sub collection of source that are split by fix size.
        /// </summary>
        /// <typeparam name="T">The type of elements in the list.</typeparam>
        /// <param name="source">Source IEnumerable{T}.</param>
        /// <param name="batchSize">The size of each group item.</param>
        /// <returns>A list of IEnumerable{T} that contains the sub collection of source.</returns>
        private List<List<T>> GroupBatch<T>(IEnumerable<T> source, int batchSize)
        {
            if (source == null)
            {
                return null;
            }

            List<List<T>> result = new List<List<T>>();

            int restCount = source.Count();

            if (batchSize < 1 || restCount < 1)
            {
                result.Add(source.ToList());
                return result;
            }

            int skipCount = 0;

            do
            {
                result.Add(source.Skip(skipCount).Take(batchSize).ToList());
                skipCount += batchSize;
                restCount -= batchSize;
            }
            while (restCount > 0);

            return result;
        }
    }
}
