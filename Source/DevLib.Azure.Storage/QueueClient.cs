//-----------------------------------------------------------------------
// <copyright file="QueueClient.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Queue.Protocol;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure Queue service. This client is used to configure and execute requests against the Queue service.
    /// </summary>
    public class QueueClient
    {
        /// <summary>
        /// The dev store account queue client.
        /// </summary>
        private static QueueClient DevStoreAccountQueueClient;

        /// <summary>
        /// Field _cloudQueueClient.
        /// </summary>
        private readonly CloudQueueClient _cloudQueueClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueClient" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public QueueClient(string connectionString)
        {
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            this._cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this.SetDefaultRetryIfNotExists(this._cloudQueueClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueClient" /> class.
        /// </summary>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public QueueClient(string accountName, string keyValue, bool useHttps = true)
        {
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            this._cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this.SetDefaultRetryIfNotExists(this._cloudQueueClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueClient" /> class.
        /// </summary>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public QueueClient(StorageCredentials storageCredentials, bool useHttps = true)
        {
            storageCredentials.ValidateNull();

            var cloudStorageAccount = new CloudStorageAccount(storageCredentials, useHttps);
            this._cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this.SetDefaultRetryIfNotExists(this._cloudQueueClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueClient" /> class.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public QueueClient(CloudStorageAccount cloudStorageAccount)
        {
            cloudStorageAccount.ValidateNull();

            this._cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this.SetDefaultRetryIfNotExists(this._cloudQueueClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueClient"/> class.
        /// </summary>
        /// <param name="cloudQueueClient">The QueueClient instance.</param>
        public QueueClient(CloudQueueClient cloudQueueClient)
        {
            cloudQueueClient.ValidateNull();

            this._cloudQueueClient = cloudQueueClient;
            this.SetDefaultRetryIfNotExists(this._cloudQueueClient);
        }

        /// <summary>
        /// Prevents a default instance of the <see cref="QueueClient" /> class from being created.
        /// </summary>
        private QueueClient()
            : this(StorageConstants.DevelopmentStorageConnectionString)
        {
        }

        /// <summary>
        /// Gets the development queue client.
        /// </summary>
        /// <value>The development queue client.</value>
        public static QueueClient DevelopmentClient
        {
            get
            {
                if (DevStoreAccountQueueClient == null)
                {
                    DevStoreAccountQueueClient = new QueueClient();
                }

                return DevStoreAccountQueueClient;
            }
        }

        /// <summary>
        /// Gets the inner cloud queue client.
        /// </summary>
        /// <value>The inner cloud queue client.</value>
        public CloudQueueClient InnerCloudQueueClient
        {
            get
            {
                return this._cloudQueueClient;
            }
        }

        /// <summary>
        /// Sets the queue client CORS.
        /// </summary>
        /// <returns>The current QueueClient.</returns>
        public QueueClient SetCors()
        {
            var serviceProperties = this._cloudQueueClient.GetServiceProperties();

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

            this._cloudQueueClient.SetServiceProperties(serviceProperties);

            return this;
        }

        /// <summary>
        /// Gets the queue.
        /// </summary>
        /// <param name="queueName">A string containing the name of the queue.</param>
        /// <param name="createIfNotExists">true to creates the queue if it does not already exist; otherwise, false.</param>
        /// <returns>QueueStorage instance.</returns>
        public QueueStorage GetQueueStorage(string queueName, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();

            var queue = this._cloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                queue.CreateIfNotExists();
            }

            return new QueueStorage(queue);
        }

        /// <summary>
        /// Deletes the queue if it already exists.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns>true if the queue did not already exist and was created; otherwise false.</returns>
        public bool DeleteQueueIfExists(string queueName)
        {
            queueName.ValidateQueueName();

            var queue = this._cloudQueueClient.GetQueueReference(queueName);

            return queue.DeleteIfExists();
        }

        /// <summary>
        /// Checks whether the queue exists.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <returns>true if the queue exists; otherwise false.</returns>
        public bool QueueExists(string queueName)
        {
            queueName.ValidateQueueName();

            var queue = this._cloudQueueClient.GetQueueReference(queueName);

            return queue.Exists();
        }

        /// <summary>
        /// Returns an enumerable collection of QueueStorage.
        /// </summary>
        /// <param name="prefix">A string containing the queue name prefix.</param>
        /// <param name="queueListingDetails">A Microsoft.WindowsAzure.Storage.Queue.Protocol.QueueListingDetails enumeration value that indicates which details to include in the listing.</param>
        /// <returns>List of QueueStorage.</returns>
        public List<QueueStorage> ListQueues(string prefix = null, QueueListingDetails queueListingDetails = QueueListingDetails.None)
        {
            var queues = this._cloudQueueClient.ListQueues(prefix, queueListingDetails);

            return queues.Select(i => new QueueStorage(i)).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the queue client.
        /// </summary>
        /// <param name="prefix">A string containing the queue name prefix.</param>
        /// <returns>The number of elements contained in the queue client.</returns>
        public int QueuesCount(string prefix = null)
        {
            return this._cloudQueueClient.ListQueues(prefix).Count();
        }

        /// <summary>
        /// Sets the default retry.
        /// </summary>
        /// <param name="cloudQueueClient">The CloudQueueClient instance.</param>
        private void SetDefaultRetryIfNotExists(CloudQueueClient cloudQueueClient)
        {
            if (cloudQueueClient.DefaultRequestOptions == null)
            {
                cloudQueueClient.DefaultRequestOptions = new QueueRequestOptions();
            }

            if (cloudQueueClient.DefaultRequestOptions.RetryPolicy == null)
            {
                cloudQueueClient.DefaultRequestOptions.RetryPolicy = StorageConstants.DefaultExponentialRetry;
            }
        }
    }
}
