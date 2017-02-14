//-----------------------------------------------------------------------
// <copyright file="QueueStorageAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// This class represents a queue in the Microsoft Azure Queue service.
    /// </summary>
    public partial class QueueStorage
    {
        /// <summary>
        /// Creates the queue if it does not already exist.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>QueueStorage instance.</returns>
        public Task<bool> CreateIfNotExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.CreateIfNotExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the queue exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if queue exists; otherwise, false.</returns>
        public Task<bool> QueueExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.ExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the queue if it exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the queue exists and was deleted; otherwise, false.</returns>
        public Task<bool> DeleteQueueIfExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Clears all messages from the queue.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public async Task<QueueStorage> ClearAsync(CancellationToken? cancellationToken = null)
        {
            await this._cloudQueue.ClearAsync(cancellationToken ?? CancellationToken.None);

            return this;
        }

        /// <summary>
        /// Adds a message to the queue.
        /// </summary>
        /// <param name="content">The content of the message as a byte array.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> EnqueueAsync(byte[] content, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var cloudQueueMessage = new CloudQueueMessage(content);

                await this._cloudQueue.AddMessageAsync(cloudQueueMessage, timeToLive, initialVisibilityDelay, null, null, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Adds a message to the queue.
        /// </summary>
        /// <param name="content">The content of the message as a string of text.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> EnqueueAsync(string content, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var cloudQueueMessage = new CloudQueueMessage(content);

                await this._cloudQueue.AddMessageAsync(cloudQueueMessage, timeToLive, initialVisibilityDelay, null, null, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> EnqueueManyAsync(IEnumerable<CloudQueueMessage> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, CancellationToken? cancellationToken = null)
        {
            messages.ValidateNull();

            return Task.Run(async () =>
            {
                foreach (var message in messages)
                {
                    await this._cloudQueue.AddMessageAsync(message, timeToLive, initialVisibilityDelay, null, null, cancellationToken ?? CancellationToken.None);
                }

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> EnqueueManyAsync(IEnumerable<string> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, CancellationToken? cancellationToken = null)
        {
            messages.ValidateNull();

            return Task.Run(async () =>
            {
                foreach (var message in messages)
                {
                    await this._cloudQueue.AddMessageAsync(new CloudQueueMessage(message), timeToLive, initialVisibilityDelay, null, null, cancellationToken ?? CancellationToken.None);
                }

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Adds messages to the queue.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="timeToLive">A System.TimeSpan specifying the maximum time to allow the message to be in the queue, or null.</param>
        /// <param name="initialVisibilityDelay">A System.TimeSpan specifying the interval of time from now during which the message will be invisible. If null then the message will be visible immediately.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> EnqueueManyAsync(IEnumerable<byte[]> messages, TimeSpan? timeToLive = null, TimeSpan? initialVisibilityDelay = null, CancellationToken? cancellationToken = null)
        {
            messages.ValidateNull();

            return Task.Run(async () =>
            {
                foreach (var message in messages)
                {
                    await this._cloudQueue.AddMessageAsync(new CloudQueueMessage(message), timeToLive, initialVisibilityDelay, null, null, cancellationToken ?? CancellationToken.None);
                }

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Peeks a single message from the queue. A peek request retrieves a message from the queue without changing its visibility.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</returns>
        public Task<CloudQueueMessage> PeekAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.PeekMessageAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Peeks messages from the queue. A peek request retrieves a message from the queue without changing its visibility.
        /// </summary>
        /// <param name="messageCount">The number of messages to peek.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage objects.</returns>
        public Task<IEnumerable<CloudQueueMessage>> PeekManyAsync(int messageCount, CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.PeekMessagesAsync(messageCount, cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a message from the queue. This operation marks the retrieved message as invisible in the queue for the default visibility timeout period.
        /// </summary>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</returns>
        public Task<CloudQueueMessage> DequeueAsync(TimeSpan? visibilityTimeout = null, CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.GetMessageAsync(visibilityTimeout, null, null, cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the specified number of messages from the queue. This operation marks the retrieved messages as invisible in the queue for the default visibility timeout period.
        /// </summary>
        /// <param name="messageCount">The number of messages to retrieve.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A list of messages.</returns>
        public Task<IEnumerable<CloudQueueMessage>> DequeueManyAsync(int messageCount, TimeSpan? visibilityTimeout = null, CancellationToken? cancellationToken = null)
        {
            return this._cloudQueue.GetMessagesAsync(messageCount, visibilityTimeout, null, null, cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="content">The message content to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> UpdateAsync(string content, TimeSpan? visibilityTimeout = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var message = await this._cloudQueue.GetMessageAsync(cancellationToken ?? CancellationToken.None);

                message.SetMessageContent(content);

                await this._cloudQueue.UpdateMessageAsync(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="content">The message content to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> UpdateAsync(byte[] content, TimeSpan? visibilityTimeout = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var message = await this._cloudQueue.GetMessageAsync(cancellationToken ?? CancellationToken.None);

                message.SetMessageContent(content);

                await this._cloudQueue.UpdateMessageAsync(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="message">A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object to update.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="updateFields">Flags of Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields values that specifies which parts of the message are to be updated.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> UpdateAsync(CloudQueueMessage message, TimeSpan? visibilityTimeout = null, MessageUpdateFields updateFields = MessageUpdateFields.Content | MessageUpdateFields.Visibility, CancellationToken? cancellationToken = null)
        {
            message.ValidateNull();

            return Task.Run(async () =>
            {
                await this._cloudQueue.UpdateMessageAsync(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Updates the visibility timeout and optionally the content of a message.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="visibilityTimeout">A System.TimeSpan specifying the visibility timeout interval.</param>
        /// <param name="updateFields">Flags of Microsoft.WindowsAzure.Storage.Queue.MessageUpdateFields values that specifies which parts of the message are to be updated.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> UpdateManyAsync(IEnumerable<CloudQueueMessage> messages, TimeSpan? visibilityTimeout = null, MessageUpdateFields updateFields = MessageUpdateFields.Content | MessageUpdateFields.Visibility, CancellationToken? cancellationToken = null)
        {
            messages.ValidateNull();

            return Task.Run(async () =>
            {
                foreach (var message in messages)
                {
                    await this._cloudQueue.UpdateMessageAsync(message, visibilityTimeout.HasValue ? visibilityTimeout.Value : TimeSpan.Zero, MessageUpdateFields.Content | MessageUpdateFields.Visibility, cancellationToken ?? CancellationToken.None);
                }

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes a message.
        /// </summary>
        /// <param name="message">A Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage object.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> DeleteAsync(CloudQueueMessage message, CancellationToken? cancellationToken = null)
        {
            message.ValidateNull();

            return Task.Run(async () =>
            {
                await this._cloudQueue.DeleteMessageAsync(message, cancellationToken ?? CancellationToken.None);
                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes a message.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueStorage instance.</returns>
        public Task<QueueStorage> DeleteManyAsync(IEnumerable<CloudQueueMessage> messages, CancellationToken? cancellationToken = null)
        {
            messages.ValidateNull();

            return Task.Run(async () =>
            {
                foreach (var message in messages)
                {
                    await this._cloudQueue.DeleteMessageAsync(message, cancellationToken ?? CancellationToken.None);
                }

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a shared access signature for the queue.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetQueueSasAsync(SharedAccessQueuePermissions permissions = SharedAccessQueuePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudQueue.GetSharedAccessSignature(new SharedAccessQueuePolicy
                {
                    Permissions = permissions,
                    SharedAccessStartTime = startTime,
                    SharedAccessExpiryTime = endTime
                }),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a shared access signature for the queue with query only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetQueueSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetQueueSasAsync(SharedAccessQueuePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Gets the queue URI for the primary location.
        /// </summary>
        /// <returns>The queue Uri.</returns>
        public Task<Uri> GetQueueUriAsync()
        {
            return Task.FromResult(this._cloudQueue.Uri);
        }

        /// <summary>
        /// Gets the queue Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The queue Uri with SAS.</returns>
        public Task<Uri> GetQueueUriWithSasAsync(SharedAccessQueuePermissions permissions = SharedAccessQueuePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(() =>
            {
                var uriBuilder = new UriBuilder(this._cloudQueue.Uri);

                uriBuilder.Query = this.GetQueueSas(permissions, startTime, endTime).TrimStart('?');

                return uriBuilder.Uri;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the queue Uri with query only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The queue Uri with query only SAS.</returns>
        public Task<Uri> GetQueueUriWithSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetQueueUriWithSasAsync(SharedAccessQueuePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }
    }
}
