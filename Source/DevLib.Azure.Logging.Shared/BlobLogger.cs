//-----------------------------------------------------------------------
// <copyright file="BlobLogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Globalization;
    using DevLib.Azure.Storage;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Class BlobLogger.
    /// </summary>
    public partial class BlobLogger : LoggerBase, ILogger
    {
        /// <summary>
        /// The logs root folder.
        /// </summary>
        private const string LogsRootFolder = "logs[1a9f9a6b9a604e1ab650fafdf04cd9dd]";

        /// <summary>
        /// The year format.
        /// </summary>
        private const string YearFormat = "yyyyUz";

        /// <summary>
        /// The month format.
        /// </summary>
        private const string MonthFormat = "yyyy-MMUz";

        /// <summary>
        /// The day format.
        /// </summary>
        private const string DayFormat = "yyyy-MM-ddUz";

        /// <summary>
        /// The time format.
        /// </summary>
        private const string TimeFormat = "yyyy-MM-ddTHHUz";

        /// <summary>
        /// The table storage.
        /// </summary>
        private readonly BlobContainer _blobContainer;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="blobContainer">The blob container.</param>
        public BlobLogger(BlobContainer blobContainer)
        {
            this._blobContainer = blobContainer;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="cloudBlobContainer">The cloud blob container.</param>
        public BlobLogger(CloudBlobContainer cloudBlobContainer)
        {
            try
            {
                this._blobContainer = new BlobContainer(cloudBlobContainer);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="blobClient">The blob client.</param>
        public BlobLogger(string containerName, BlobClient blobClient)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, blobClient, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudBlobClient">The cloud blob client.</param>
        public BlobLogger(string containerName, CloudBlobClient cloudBlobClient)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, cloudBlobClient, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="connectionString">The connection string.</param>
        public BlobLogger(string containerName, string connectionString)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, connectionString, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public BlobLogger(string containerName, string accountName, string keyValue, bool useHttps = true)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, accountName, keyValue, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public BlobLogger(string containerName, StorageCredentials storageCredentials, bool useHttps = true)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, storageCredentials, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobLogger"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public BlobLogger(string containerName, CloudStorageAccount cloudStorageAccount)
        {
            try
            {
                this._blobContainer = new BlobContainer(containerName, cloudStorageAccount, true, false);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LogLevel level, LogMessageTableEntity messageEntity)
        {
            if (this._blobContainer != null)
            {
                try
                {
                    string logFile = $"{LogsRootFolder}/{messageEntity.Timestamp.ToString(YearFormat, CultureInfo.InvariantCulture)}/{messageEntity.Timestamp.ToString(MonthFormat, CultureInfo.InvariantCulture)}/{messageEntity.Timestamp.ToString(DayFormat, CultureInfo.InvariantCulture)}/{messageEntity.Timestamp.ToString(TimeFormat, CultureInfo.InvariantCulture)}.log";
                    this._blobContainer.AppendBlobAppendText(logFile, messageEntity.ToString() + Environment.NewLine);
                }
                catch (Exception e)
                {
                    var internalError = InternalLogger.Log(e);

                    try
                    {
                        string logFile = $"{LogsRootFolder}/internal/{messageEntity.Timestamp.ToString(TimeFormat, CultureInfo.InvariantCulture)}.log";
                        this._blobContainer.AppendBlobAppendText(logFile, internalError + Environment.NewLine);
                        this._blobContainer.AppendBlobAppendText(logFile, messageEntity.ToString() + Environment.NewLine);
                    }
                    catch (Exception ex)
                    {
                        InternalLogger.Log(ex);
                    }
                }
            }
            else
            {
                InternalLogger.Log("DevLib.Azure.Logging.BlobLogger._blobContainer is null");
            }
        }
    }
}
