//-----------------------------------------------------------------------
// <copyright file="BlobClientAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Provides a client-side logical representation of Microsoft Azure Blob storage.
    /// </summary>
    public partial class BlobClient
    {
        /// <summary>
        /// Gets the blob container.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>BlobContainer instance.</returns>
        public Task<BlobContainer> GetContainerAsync(string containerName, bool createIfNotExists = true, bool isNewContainerPublic = true, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this.GetContainer(containerName, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the container if it already exists.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public Task<bool> DeleteContainerIfExistsAsync(string containerName, CancellationToken? cancellationToken = null)
        {
            containerName.ValidateContainerName();

            return Task.Run(() =>
            {
                var container = this._cloudBlobClient.GetContainerReference(containerName);
                return container.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the container exists.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container exists; otherwise false.</returns>
        public Task<bool> ContainerExistsAsync(string containerName, CancellationToken? cancellationToken = null)
        {
            containerName.ValidateContainerName();

            return Task.Run(() =>
            {
                var container = this._cloudBlobClient.GetContainerReference(containerName);
                return container.ExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns an enumerable collection of BlobContainer.
        /// </summary>
        /// <param name="prefix">A string containing the container name prefix.</param>
        /// <param name="detailsIncluded">A Microsoft.WindowsAzure.Storage.Blob.ContainerListingDetails enumeration value that indicates whether to return container metadata with the listing.</param>
        /// <returns>List of BlobContainer.</returns>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        public Task<List<BlobContainer>> ListContainersAsync(string prefix = null, ContainerListingDetails detailsIncluded = ContainerListingDetails.None, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudBlobClient.ListContainers(prefix, detailsIncluded).Select(i => new BlobContainer(i)).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the BlobClient.
        /// </summary>
        /// <param name="prefix">A string containing the container name prefix.</param>
        /// <param name="detailsIncluded">A Microsoft.WindowsAzure.Storage.Blob.ContainerListingDetails enumeration value that indicates whether to return container metadata with the listing.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the BlobClient.</returns>
        public Task<int> ContainersCountAsync(string prefix = null, ContainerListingDetails detailsIncluded = ContainerListingDetails.None, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudBlobClient.ListContainers(prefix, detailsIncluded).Count(),
                cancellationToken ?? CancellationToken.None);
        }
    }
}
