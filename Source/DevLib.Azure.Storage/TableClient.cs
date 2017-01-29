//-----------------------------------------------------------------------
// <copyright file="TableClient.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure Table Service.
    /// </summary>
    public class TableClient
    {
        /// <summary>
        /// The dev store account table client.
        /// </summary>
        private static TableClient DevStoreAccountTableClient;

        /// <summary>
        /// Field _cloudTableClient.
        /// </summary>
        private readonly CloudTableClient _cloudTableClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableClient"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public TableClient(string connectionString)
        {
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            this._cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(this._cloudTableClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableClient"/> class.
        /// </summary>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public TableClient(string accountName, string keyValue, bool useHttps = true)
        {
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            this._cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(this._cloudTableClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableClient"/> class.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public TableClient(CloudStorageAccount cloudStorageAccount)
        {
            cloudStorageAccount.ValidateNull();

            this._cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            this.SetDefaultRetryIfNotExists(this._cloudTableClient);
        }

        /// <summary>
        /// Prevents a default instance of the <see cref="TableClient"/> class from being created.
        /// </summary>
        private TableClient()
            : this(StorageConstants.DevelopmentStorageConnectionString)
        {
        }

        /// <summary>
        /// Gets the development table client.
        /// </summary>
        /// <value>The development table client.</value>
        public static TableClient DevelopmentClient
        {
            get
            {
                if (DevStoreAccountTableClient == null)
                {
                    DevStoreAccountTableClient = new TableClient();
                }

                return DevStoreAccountTableClient;
            }
        }

        /// <summary>
        /// Gets the inner cloud table client.
        /// </summary>
        public CloudTableClient InnerCloudBlobClient
        {
            get
            {
                return this._cloudTableClient;
            }
        }

        /// <summary>
        /// Sets the table client CORS.
        /// </summary>
        /// <returns>The current TableClient.</returns>
        public TableClient SetCors()
        {
            var serviceProperties = this._cloudTableClient.GetServiceProperties();

            if (serviceProperties.Cors == null)
            {
                serviceProperties.Cors = new CorsProperties();
            }

            serviceProperties.Cors.CorsRules.Add(new CorsRule()
            {
                AllowedHeaders = new List<string>() { "*" },
                AllowedMethods = CorsHttpMethods.Put | CorsHttpMethods.Get | CorsHttpMethods.Head | CorsHttpMethods.Post,
                AllowedOrigins = new List<string>() { "*" },
                ExposedHeaders = new List<string>() { "*" },
            });

            this._cloudTableClient.SetServiceProperties(serviceProperties);

            return this;
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        /// <returns>TableStorage instance.</returns>
        public TableStorage GetTableStorage(string tableName, bool createIfNotExists = true)
        {
            tableName.ValidateTableName();

            var table = this._cloudTableClient.GetTableReference(tableName);

            if (createIfNotExists)
            {
                table.CreateIfNotExists();
            }

            return new TableStorage(table);
        }

        /// <summary>
        /// Gets the table dictionary.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        /// <returns>TableDictionary instance.</returns>
        public TableDictionary GetTableDictionary(string dictionaryName, string tableName, bool dictionaryKeyIgnoreCase = false)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();
            tableName.ValidateTableName();

            var table = this._cloudTableClient.GetTableReference(tableName);
            table.CreateIfNotExists();

            return new TableDictionary(dictionaryName, new TableStorage(table));
        }

        /// <summary>
        /// Deletes the table if it already exists.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>true if the table did not already exist and was created; otherwise false.</returns>
        public bool DeleteTableIfExists(string tableName)
        {
            tableName.ValidateTableName();

            var table = this._cloudTableClient.GetTableReference(tableName);

            return table.DeleteIfExists();
        }

        /// <summary>
        /// Checks whether the table exists.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>true if the table exists; otherwise false.</returns>
        public bool TableExists(string tableName)
        {
            tableName.ValidateTableName();

            var table = this._cloudTableClient.GetTableReference(tableName);

            return table.Exists();
        }

        /// <summary>
        /// Returns an enumerable collection of TableStorage.
        /// </summary>
        /// <param name="prefix">A string containing the table name prefix.</param>
        /// <returns>List of TableStorage.</returns>
        public List<TableStorage> ListTables(string prefix = null)
        {
            var tables = this._cloudTableClient.ListTables(prefix);

            return tables.Select(i => new TableStorage(i)).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the TableStorage.
        /// </summary>
        /// <param name="prefix">A string containing the table name prefix.</param>
        /// <returns>The number of elements contained in the TableStorage.</returns>
        public int TablesCount(string prefix = null)
        {
            return this._cloudTableClient.ListTables(prefix).Count();
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
    }
}
