//-----------------------------------------------------------------------
// <copyright file="TableLogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using DevLib.Azure.Storage;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Class Logger.
    /// </summary>
    public class TableLogger : LoggerBase, ILogger
    {
        /// <summary>
        /// The table storage.
        /// </summary>
        private readonly TableStorage _tableStorage;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableStorage">The table storage.</param>
        public TableLogger(TableStorage tableStorage)
        {
            this._tableStorage = tableStorage;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="cloudTable">The cloud table.</param>
        public TableLogger(CloudTable cloudTable)
        {
            try
            {
                this._tableStorage = new TableStorage(cloudTable);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="tableClient">The table client.</param>
        public TableLogger(string tableName, TableClient tableClient)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, tableClient);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudTableClient">The cloud table client.</param>
        public TableLogger(string tableName, CloudTableClient cloudTableClient)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, cloudTableClient);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="connectionString">The connection string.</param>
        public TableLogger(string tableName, string connectionString)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, connectionString);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public TableLogger(string tableName, string accountName, string keyValue, bool useHttps = true)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, accountName, keyValue);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public TableLogger(string tableName, StorageCredentials storageCredentials, bool useHttps = true)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, storageCredentials);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableLogger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public TableLogger(string tableName, CloudStorageAccount cloudStorageAccount)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, cloudStorageAccount);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LoggingMessageTableEntity messageEntity)
        {
            if (this._tableStorage != null)
            {
                try
                {
                    this._tableStorage.Insert(messageEntity);
                }
                catch (Exception e)
                {
                    InternalLogger.Log(e);

                    try
                    {
                        messageEntity.RowKey = Guid.NewGuid().ToString();
                        this._tableStorage.Insert(messageEntity);
                    }
                    catch (Exception ex)
                    {
                        InternalLogger.Log(ex);
                    }
                }
            }
            else
            {
                InternalLogger.Log("DevLib.Azure.Logging.Logger._tableStorage is null");
            }
        }
    }
}
