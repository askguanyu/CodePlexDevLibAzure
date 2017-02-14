//-----------------------------------------------------------------------
// <copyright file="TableClientAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure Table Service.
    /// </summary>
    public partial class TableClient
    {
        /// <summary>
        /// Sets the table client CORS.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current TableClient.</returns>
        public Task<TableClient> SetCorsAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var serviceProperties = await this._cloudTableClient.GetServicePropertiesAsync(cancellationToken ?? CancellationToken.None);

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

                await this._cloudTableClient.SetServicePropertiesAsync(serviceProperties, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>TableStorage instance.</returns>
        public Task<TableStorage> GetTableStorageAsync(string tableName, bool createIfNotExists = true, CancellationToken? cancellationToken = null)
        {
            tableName.ValidateTableName();

            return Task.Run(() =>
            {
                var table = this._cloudTableClient.GetTableReference(tableName);

                if (createIfNotExists)
                {
                    table.CreateIfNotExists();
                }

                return new TableStorage(table);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the table dictionary.
        /// </summary>
        /// <param name="dictionaryName">Name of the dictionary.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="dictionaryKeyIgnoreCase">true to ignore case of dictionary key; otherwise, false.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>TableDictionary instance.</returns>
        public Task<TableDictionary> GetTableDictionaryAsync(string dictionaryName, string tableName, bool dictionaryKeyIgnoreCase = false, CancellationToken? cancellationToken = null)
        {
            dictionaryName.ValidateTableDictionaryPropertyValue();
            tableName.ValidateTableName();

            return Task.Run(() =>
            {
                var table = this._cloudTableClient.GetTableReference(tableName);
                table.CreateIfNotExists();

                return new TableDictionary(dictionaryName, new TableStorage(table));
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the table if it already exists.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the table did not already exist and was created; otherwise false.</returns>
        public Task<bool> DeleteTableIfExistsAsync(string tableName, CancellationToken? cancellationToken = null)
        {
            tableName.ValidateTableName();

            return Task.Run(() =>
            {
                var table = this._cloudTableClient.GetTableReference(tableName);

                return table.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the table exists.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the table exists; otherwise false.</returns>
        public Task<bool> TableExistsAsync(string tableName, CancellationToken? cancellationToken = null)
        {
            tableName.ValidateTableName();

            return Task.Run(() =>
            {
                var table = this._cloudTableClient.GetTableReference(tableName);

                return table.ExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns an enumerable collection of TableStorage.
        /// </summary>
        /// <param name="prefix">A string containing the table name prefix.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>List of TableStorage.</returns>
        public Task<List<TableStorage>> ListTablesAsync(string prefix = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudTableClient.ListTables(prefix).Select(i => new TableStorage(i)).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the table client.
        /// </summary>
        /// <param name="prefix">A string containing the table name prefix.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the table client.</returns>
        public Task<int> TablesCountAsync(string prefix = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudTableClient.ListTables(prefix).Count(),
                cancellationToken ?? CancellationToken.None);
        }
    }
}
