//-----------------------------------------------------------------------
// <copyright file="QueueStorage.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// This class represents a queue in the Microsoft Azure Queue service.
    /// </summary>
    public class QueueStorage
    {
        /// <summary>
        /// Field _cloudQueue.
        /// </summary>
        private readonly CloudQueue _cloudQueue;

        /// <summary>
        /// Field _queueClient.
        /// </summary>
        private readonly QueueClient _queueClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="queueClient">The queue client.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, QueueClient queueClient, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            queueClient.ValidateNull();

            this._queueClient = queueClient;

            this._cloudQueue = this._queueClient.InnerCloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="cloudQueueClient">The cloud queue client.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, CloudQueueClient cloudQueueClient, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            cloudQueueClient.ValidateNull();

            this._queueClient = new QueueClient(cloudQueueClient);

            this._cloudQueue = this._queueClient.InnerCloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, string connectionString, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this._queueClient = new QueueClient(cloudQueueClient);
            this.SetDefaultRetryIfNotExists(cloudQueueClient);

            this._cloudQueue = cloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, string accountName, string keyValue, bool useHttps = true, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this._queueClient = new QueueClient(cloudQueueClient);
            this.SetDefaultRetryIfNotExists(cloudQueueClient);

            this._cloudQueue = cloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, StorageCredentials storageCredentials, bool useHttps = true, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            storageCredentials.ValidateNull();

            var cloudStorageAccount = new CloudStorageAccount(storageCredentials, useHttps);
            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this._queueClient = new QueueClient(cloudQueueClient);
            this.SetDefaultRetryIfNotExists(cloudQueueClient);

            this._cloudQueue = cloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="createIfNotExists">true to creates the table if it does not already exist; otherwise, false.</param>
        public QueueStorage(string queueName, CloudStorageAccount cloudStorageAccount, bool createIfNotExists = true)
        {
            queueName.ValidateQueueName();
            cloudStorageAccount.ValidateNull();

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            this._queueClient = new QueueClient(cloudQueueClient);
            this.SetDefaultRetryIfNotExists(cloudQueueClient);

            this._cloudQueue = cloudQueueClient.GetQueueReference(queueName);

            if (createIfNotExists)
            {
                this._cloudQueue.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueStorage" /> class.
        /// </summary>
        /// <param name="cloudQueue">The cloud queue.</param>
        public QueueStorage(CloudQueue cloudQueue)
        {
            this._cloudQueue = cloudQueue;
            this._queueClient = new QueueClient(cloudQueue.ServiceClient);
        }

        /// <summary>
        /// Gets the inner cloud queue.
        /// </summary>
        /// <value>The inner cloud queue.</value>
        public CloudQueue InnerCloudQueue
        {
            get
            {
                return this._cloudQueue;
            }
        }

        /// <summary>
        /// Gets the service client.
        /// </summary>
        public QueueClient ServiceClient
        {
            get
            {
                return this._queueClient;
            }
        }

        /// <summary>
        /// Gets the approximate message count for the queue.
        /// </summary>
        public int Count
        {
            get
            {
                this._cloudQueue.FetchAttributes();
                return this._cloudQueue.ApproximateMessageCount ?? 0;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether to apply base64 encoding when adding or retrieving messages.
        /// </summary>
        public bool EncodeMessage
        {
            get
            {
                return this._cloudQueue.EncodeMessage;
            }

            set
            {
                this._cloudQueue.EncodeMessage = value;
            }
        }

        /// <summary>
        /// Creates the queue if it does not already exist.
        /// </summary>
        /// <returns>QueueStorage instance.</returns>
        public QueueStorage CreateIfNotExists()
        {
            this._cloudQueue.CreateIfNotExists();

            return this;
        }

        /// <summary>
        /// Checks whether the queue exists.
        /// </summary>
        /// <returns>true if queue exists; otherwise, false.</returns>
        public bool QueueExists()
        {
            return this._cloudQueue.Exists();
        }

        /// <summary>
        /// Deletes the queue if it exists.
        /// </summary>
        /// <returns>true if the queue exists and was deleted; otherwise, false.</returns>
        public bool DeleteQueueIfExists()
        {
            return this._cloudQueue.DeleteIfExists();
        }

        /// <summary>
        /// Clears all messages from the queue.
        /// </summary>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Clear()
        {
            this._cloudQueue.Clear();

            return this;
        }

        /// <summary>
        /// Adds a message to the queue.
        /// </summary>
        /// <param name="content">The content of the message as a byte array.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Enqueue(byte[] content, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            var cloudQueueMessage = new CloudQueueMessage(content);

            this._cloudQueue.AddMessage(cloudQueueMessage, timeToLive, initialVisibilityDelay);

            return this;
        }

        /// <summary>
        /// Adds a message to the queue.
        /// </summary>
        /// <param name="content">The content of the message as a string of text.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Enqueue(string content, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            var cloudQueueMessage = new CloudQueueMessage(content);

            this._cloudQueue.AddMessage(cloudQueueMessage, timeToLive, initialVisibilityDelay);

            return this;
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage EnqueueMany(IEnumerable<CloudQueueMessage> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            messages.ValidateNull();

            foreach (var message in messages)
            {
                this._cloudQueue.AddMessage(message, timeToLive, initialVisibilityDelay);
            }

            return this;
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage EnqueueMany(IEnumerable<string> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            messages.ValidateNull();

            foreach (var message in messages)
            {
                this.Enqueue(message, timeToLive, initialVisibilityDelay);
            }

            return this;
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage EnqueueMany(IEnumerable<byte[]> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null)
        {
            messages.ValidateNull();

            foreach (var message in messages)
            {
                this.Enqueue(message, timeToLive, initialVisibilityDelay);
            }

            return this;
        }

        /// <summary>
        /// Peeks a single message from the queue. A peek request retrieves a message from the queue without changing its visibility.
        /// </summary>
        /// <returns>A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</returns>
        public CloudQueueMessage Peek()
        {
            return this._cloudQueue.PeekMessage();
        }

        /// <summary>
        /// Peeks messages from the queue. A peek request retrieves a message from the queue without changing its visibility.
        /// </summary>
        /// <param name="messageCount">The number of messages to peek.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage objects.</returns>
        public List<CloudQueueMessage> PeekMany(int messageCount)
        {
            return this._cloudQueue.PeekMessages(messageCount).ToList();
        }

        /// <summary>
        /// Gets a message from the queue. This operation marks the retrieved message as invisible in the queue for the default visibility timeout period.
        /// </summary>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</returns>
        public CloudQueueMessage Dequeue(TimeSpan? visibilityTimeout = null)
        {
            return this._cloudQueue.GetMessage(visibilityTimeout);
        }

        /// <summary>
        /// Gets the specified number of messages from the queue. This operation marks the retrieved messages as invisible in the queue for the default visibility timeout period.
        /// </summary>
        /// <param name="messageCount">The number of messages to retrieve.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <returns>A list of messages.</returns>
        public List<CloudQueueMessage> DequeueMany(int messageCount, TimeSpan? visibilityTimeout = null)
        {
            return this._cloudQueue.GetMessages(messageCount, visibilityTimeout).ToList();
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="content">The message content to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Update(string content, TimeSpan? visibilityTimeout = null)
        {
            var message = this._cloudQueue.GetMessage();

            message.SetMessageContent(content);

            this._cloudQueue.UpdateMessage(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility);

            return this;
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="content">The message content to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Update(byte[] content, TimeSpan? visibilityTimeout = null)
        {
            var message = this._cloudQueue.GetMessage();

            message.SetMessageContent(content);

            this._cloudQueue.UpdateMessage(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility);

            return this;
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="message">A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="updateFields">Flags of Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields values that specifies which parts of the message are to be updated.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Update(CloudQueueMessage message, TimeSpan? visibilityTimeout = null, MessageUpdateFields updateFields = MessageUpdateFields.Content | MessageUpdateFields.Visibility)
        {
            message.ValidateNull();

            this._cloudQueue.UpdateMessage(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, updateFields);

            return this;
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="updateFields">Flags of Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields values that specifies which parts of the message are to be updated.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage UpdateMany(IEnumerable<CloudQueueMessage> messages, TimeSpan? visibilityTimeout = null, MessageUpdateFields updateFields = MessageUpdateFields.Content | MessageUpdateFields.Visibility)
        {
            messages.ValidateNull();

            foreach (var message in messages)
            {
                this._cloudQueue.UpdateMessage(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, updateFields);
            }

            return this;
        }

        /// <summary>
        /// Deletes a message.
        /// </summary>
        /// <param name="message">A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage Delete(CloudQueueMessage message)
        {
            message.ValidateNull();

            this._cloudQueue.DeleteMessage(message);

            return this;
        }

        /// <summary>
        /// Deletes a message.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public QueueStorage DeleteMany(IEnumerable<CloudQueueMessage> messages)
        {
            messages.ValidateNull();

            foreach (var message in messages)
            {
                this._cloudQueue.DeleteMessage(message);
            }

            return this;
        }

        /// <summary>
        /// Returns a shared access signature for the queue.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetQueueSAS(SharedAccessQueuePermissions permissions = SharedAccessQueuePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this._cloudQueue.GetSharedAccessSignature(new SharedAccessQueuePolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Returns a shared access signature for the queue with query only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetQueueSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetQueueSAS(SharedAccessQueuePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the queue URI for the primary location.
        /// </summary>
        /// <returns>The queue Uri.</returns>
        public Uri GetQueueUri()
        {
            return this._cloudQueue.Uri;
        }

        /// <summary>
        /// Gets the queue Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The queue Uri with SAS.</returns>
        public Uri GetQueueUriWithSAS(SharedAccessQueuePermissions permissions = SharedAccessQueuePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var uriBuilder = new UriBuilder(this._cloudQueue.Uri);

            uriBuilder.Query = this.GetQueueSAS(permissions, startTime, endTime).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the queue Uri with query only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The queue Uri with query only SAS.</returns>
        public Uri GetQueueUriWithSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetQueueUriWithSAS(SharedAccessQueuePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
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
