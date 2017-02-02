//-----------------------------------------------------------------------
// <copyright file="FileClientAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure File service. This client is used to configure and execute requests against the File service.
    /// </summary>
    public partial class FileClient
    {
        /// <summary>
        /// Gets the file share.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>FileShare instance.</returns>
        public Task<FileStorage> GetShareAsync(string shareName, bool createIfNotExists = true, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this.GetShare(shareName, createIfNotExists),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the share if it already exists.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the share did not already exist and was created; otherwise false.</returns>
        public async Task<bool> DeleteShareIfExistsAsync(string shareName, CancellationToken? cancellationToken = null)
        {
            shareName.ValidateShareName();

            var share = await Task.Run(
                () => this._cloudFileClient.GetShareReference(shareName),
                cancellationToken ?? CancellationToken.None);

            return await share.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks whether the share exists.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the share exists; otherwise false.</returns>
        public async Task<bool> ShareExistsAsync(string shareName, CancellationToken? cancellationToken = null)
        {
            shareName.ValidateShareName();

            var share = await Task.Run(
                () => this._cloudFileClient.GetShareReference(shareName),
                cancellationToken ?? CancellationToken.None);

            return await share.ExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns an enumerable collection of FileShare.
        /// </summary>
        /// <param name="prefix">The share name prefix.</param>
        /// <param name="detailsIncluded">A value that indicates whether to return share metadata with the listing.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>List of FileShare.</returns>
        public Task<List<FileStorage>> ListSharesAsync(string prefix = null, ShareListingDetails detailsIncluded = ShareListingDetails.None, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudFileClient.ListShares(prefix, detailsIncluded).Select(i => new FileStorage(i)).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the FileShare.
        /// </summary>
        /// <param name="prefix">The share name prefix.</param>
        /// <param name="detailsIncluded">A value that indicates whether to return share metadata with the listing.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the FileShare.</returns>
        public Task<int> SharesCountAsync(string prefix = null, ShareListingDetails detailsIncluded = ShareListingDetails.None, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudFileClient.ListShares(prefix, detailsIncluded).Count(),
                cancellationToken ?? CancellationToken.None);
        }
    }
}
