//-----------------------------------------------------------------------
// <copyright file="QueueClientAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Queue.Protocol;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure Queue service. This client is used to configure and execute requests against the Queue service.
    /// </summary>
    public partial class QueueClient
    {
        /// <summary>
        /// Sets the queue client CORS.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The current QueueClient.</returns>
        public Task<QueueClient> SetCorsAsync(CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var serviceProperties = await this._cloudQueueClient.GetServicePropertiesAsync(cancellationToken ?? CancellationToken.None);

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

                await this._cloudQueueClient.SetServicePropertiesAsync(serviceProperties, cancellationToken ?? CancellationToken.None);

                return this;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the queue.
        /// </summary>
        /// <param name="queueName">A string containing the name of the queue.</param>
        /// <param name="createIfNotExists">true to creates the queue if it does not already exist; otherwise, false.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>QueueStorage instance.</returns>
        public Task<QueueStorage> GetQueueStorageAsync(string queueName, bool createIfNotExists = true, CancellationToken? cancellationToken = null)
        {
            queueName.ValidateQueueName();

            return Task.Run(() =>
            {
                var queue = this._cloudQueueClient.GetQueueReference(queueName);

                if (createIfNotExists)
                {
                    queue.CreateIfNotExists();
                }

                return new QueueStorage(queue);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the queue if it already exists.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the queue did not already exist and was created; otherwise false.</returns>
        public Task<bool> DeleteQueueIfExistsAsync(string queueName, CancellationToken? cancellationToken = null)
        {
            queueName.ValidateQueueName();

            return Task.Run(() =>
            {
                var queue = this._cloudQueueClient.GetQueueReference(queueName);
                return queue.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the queue exists.
        /// </summary>
        /// <param name="queueName">Name of the queue.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the queue exists; otherwise false.</returns>
        public Task<bool> QueueExistsAsync(string queueName, CancellationToken? cancellationToken = null)
        {
            queueName.ValidateQueueName();

            return Task.Run(() =>
            {
                var queue = this._cloudQueueClient.GetQueueReference(queueName);
                return queue.ExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns an enumerable collection of QueueStorage.
        /// </summary>
        /// <param name="prefix">A string containing the queue name prefix.</param>
        /// <param name="queueListingDetails">A Microsoft.WindowsAzure.Storage.Queue.Protocol.QueueListingDetails enumeration value that indicates which details to include in the listing.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>List of QueueStorage.</returns>
        public Task<List<QueueStorage>> ListQueuesAsync(string prefix = null, QueueListingDetails queueListingDetails = QueueListingDetails.None, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudQueueClient.ListQueues(prefix, queueListingDetails).Select(i => new QueueStorage(i)).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the queue client.
        /// </summary>
        /// <param name="prefix">A string containing the queue name prefix.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the queue client.</returns>
        public Task<int> QueuesCountAsync(string prefix = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudQueueClient.ListQueues(prefix).Count(),
                cancellationToken ?? CancellationToken.None);
        }
    }
}
