//-----------------------------------------------------------------------
// <copyright file="BlobContainerAsync.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Represents a container in the Microsoft Azure Blob service.
    /// </summary>
    public partial class BlobContainer
    {
        /// <summary>
        /// Begins an operation to start copying source block blob's contents, properties, and metadata to the destination block blob.
        /// </summary>
        /// <param name="sourceBlob">The source blob.</param>
        /// <param name="destBlob">The destination blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source blob does not exist.</returns>
        public static Task<string> StartCopyBlockBlobAsync(CloudBlockBlob sourceBlob, CloudBlockBlob destBlob, CancellationToken? cancellationToken = null)
        {
            sourceBlob.ValidateNull();
            destBlob.ValidateNull();

            if (sourceBlob.Exists())
            {
                return destBlob.StartCopyAsync(sourceBlob, cancellationToken ?? CancellationToken.None);
            }
            else
            {
                return Task.FromResult(string.Empty);
            }
        }

        /// <summary>
        /// Creates the container if it does not already exist.
        /// </summary>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public Task<bool> CreateIfNotExistsAsync(BlobContainerPublicAccessType newContainerAccessType, CancellationToken? cancellationToken = null)
        {
            return Task.Run(async () =>
            {
                var result = await this._cloudBlobContainer.CreateIfNotExistsAsync(cancellationToken ?? CancellationToken.None);

                if (result)
                {
                    this.SetAccessPermission(newContainerAccessType);
                }

                return result;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Creates the container if it does not already exist.
        /// </summary>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public Task<bool> CreateIfNotExistsAsync(bool isNewContainerPublic = true, CancellationToken? cancellationToken = null)
        {
            return this.CreateIfNotExistsAsync(isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off, cancellationToken);
        }

        /// <summary>
        /// Checks whether the container exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container exists; otherwise false.</returns>
        public Task<bool> ContainerExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudBlobContainer.ExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Checks existence of the blob.
        /// </summary>
        /// <param name="blobName">Name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container exists; otherwise false.</returns>
        public Task<bool> ExistsAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var blob = this._cloudBlobContainer.GetBlobReference(blobName);
                return blob.ExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Returns a list of all the blobs in the container.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>List of all the blobs in the container.</returns>
        public Task<List<IListBlobItem>> ListBlobsAsync(bool useFlatBlobListing = false, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudBlobContainer.ListBlobs(useFlatBlobListing: useFlatBlobListing).ToList(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a reference to a virtual blob directory beneath this container.
        /// </summary>
        /// <param name="relativeAddress">A string containing the name of the virtual blob directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudBlobDirectory object.</returns>
        public Task<CloudBlobDirectory> GetDirectory(string relativeAddress, CancellationToken? cancellationToken = null)
        {
            return Task.Run(() =>
            {
                return this._cloudBlobContainer.GetDirectoryReference(relativeAddress);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the number of elements contained in the container.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the container.</returns>
        public Task<int> BlobsCountAsync(bool useFlatBlobListing = false, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this._cloudBlobContainer.ListBlobs(useFlatBlobListing: useFlatBlobListing).Count(),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the container if it already exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the container did not already exist and was deleted; otherwise false.</returns>
        public Task<bool> DeleteContainerIfExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudBlobContainer.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Deletes the blob if it already exists.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the blob did already exist and was deleted; otherwise false.</returns>
        public Task<bool> DeleteBlobIfExistsAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var blob = this._cloudBlobContainer.GetBlobReference(blobName);
                return blob.DeleteIfExistsAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Uploads a stream to a block blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A System.IO.Stream object providing the blob content.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public Task<CloudBlockBlob> CreateBlockBlobAsync(string blobName, Stream data, string contentType = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                if (!string.IsNullOrWhiteSpace(contentType))
                {
                    blob.Properties.ContentType = contentType;
                }

                await blob.UploadFromStreamAsync(data, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Uploads the contents of a byte array to a blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">An array of bytes.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public Task<CloudBlockBlob> CreateBlockBlobAsync(string blobName, byte[] data, string contentType = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                if (!string.IsNullOrWhiteSpace(contentType))
                {
                    blob.Properties.ContentType = contentType;
                }

                await blob.UploadFromByteArrayAsync(data, 0, data.Length, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Uploads a string of text to a blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A string containing the text to upload.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public Task<CloudBlockBlob> CreateBlockBlobAsync(string blobName, string data, string contentType = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                if (!string.IsNullOrWhiteSpace(contentType))
                {
                    blob.Properties.ContentType = contentType;
                }

                await blob.UploadTextAsync(data, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Uploads a file to the Blob service. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="filePath">A string containing the file path providing the blob content.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public Task<CloudBlockBlob> UploadBlockBlobAsync(string blobName, string filePath, string contentType = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();
            filePath.ValidateNull();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                if (!string.IsNullOrWhiteSpace(contentType))
                {
                    blob.Properties.ContentType = contentType;
                }

                await blob.UploadFromFileAsync(filePath, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a reference to a block blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob object.</returns>
        public Task<CloudBlockBlob> GetBlockBlobAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(
                () => this._cloudBlobContainer.GetBlockBlobReference(blobName),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Downloads the blob's contents as a string.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The contents of the blob, as a string.</returns>
        public Task<string> DownloadBlockBlobTextAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);
                return blob.DownloadTextAsync(cancellationToken ?? CancellationToken.None);
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Downloads the contents of a blob to a stream.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A System.IO.Stream object representing the target stream.</returns>
        public Task<MemoryStream> DownloadBlockBlobToStreamAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                var stream = new MemoryStream();
                await blob.DownloadToStreamAsync(stream, cancellationToken ?? CancellationToken.None);
                stream.Seek(0, SeekOrigin.Begin);

                return stream;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Downloads the contents of a blob to a file.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="localFilePath">A string containing the path to the target file.</param>
        /// <param name="mode">A System.IO.FileMode enumeration value that determines how to open or create the file.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public Task<CloudBlockBlob> DownloadBlockBlobToFileAsync(string blobName, string localFilePath, FileMode mode = FileMode.Create, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

                var stream = new MemoryStream();
                await blob.DownloadToFileAsync(localFilePath, mode, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Appends a string of text to an append blob.
        /// This API should be used strictly in a single writer scenario because the API internally uses the append-offset conditional header to avoid duplicate blocks which does not work in a multiple writer scenario.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A string containing the text to upload.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudAppendBlob object.</returns>
        public Task<CloudAppendBlob> AppendBlobAppendTextAsync(string blobName, string data, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            return Task.Run(async () =>
            {
                var blob = this._cloudBlobContainer.GetAppendBlobReference(blobName);

                if (!await blob.ExistsAsync(cancellationToken ?? CancellationToken.None))
                {
                    await blob.CreateOrReplaceAsync(cancellationToken ?? CancellationToken.None);
                }

                await blob.AppendTextAsync(data, cancellationToken ?? CancellationToken.None);

                return blob;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a reference to an append blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudAppendBlob object.</returns>
        public Task<CloudAppendBlob> GetAppendBlobAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(
                () => this._cloudBlobContainer.GetAppendBlobReference(blobName),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a reference to a blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudBlob object.</returns>
        public Task<CloudBlob> GetBlobAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(
                () => this._cloudBlobContainer.GetBlobReference(blobName),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a shared access signature for the blob.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetBlobSasAsync(string blobName, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var blob = this._cloudBlobContainer.GetBlobReference(blobName);

                return blob.GetSharedAccessSignature(new SharedAccessBlobPolicy
                {
                    Permissions = permissions,
                    SharedAccessStartTime = startTime,
                    SharedAccessExpiryTime = endTime
                });
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a shared access signature for the blob with read only access.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetBlobSasReadOnlyAsync(string blobName, TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetBlobSasAsync(blobName, SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Gets the blob Uri.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The blob Uri.</returns>
        public Task<Uri> GetBlobUriAsync(string blobName, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(
                () => this._cloudBlobContainer.GetBlobReference(blobName).Uri,
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the blob Uri with SAS.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The blob Uri with SAS.</returns>
        public Task<Uri> GetBlobUriWithSasAsync(string blobName, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            blobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var blob = this._cloudBlobContainer.GetBlobReference(blobName);

                var uriBuilder = new UriBuilder(blob.Uri);

                uriBuilder.Query = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
                {
                    Permissions = permissions,
                    SharedAccessStartTime = startTime,
                    SharedAccessExpiryTime = endTime
                }).TrimStart('?');

                return uriBuilder.Uri;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the blob Uri with read only SAS.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The blob Uri with read only SAS.</returns>
        public Task<Uri> GetBlobUriWithSasReadOnlyAsync(string blobName, TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetBlobUriWithSasAsync(blobName, SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Gets a shared access signature for the container.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetContainerSasAsync(SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(
                () => this.GetContainerSas(permissions, startTime, endTime),
                cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets a shared access signature for the container with read only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public Task<string> GetContainerSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetContainerSasAsync(SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Gets the container Uri.
        /// </summary>
        /// <returns>The container Uri.</returns>
        public Task<Uri> GetContainerUriAsync()
        {
            return Task.FromResult(this._cloudBlobContainer.Uri);
        }

        /// <summary>
        /// Gets the container Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The container Uri with SAS.</returns>
        public Task<Uri> GetContainerUriWithSasAsync(SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
        {
            return Task.Run(() =>
            {
                var uriBuilder = new UriBuilder(this._cloudBlobContainer.Uri);

                uriBuilder.Query = this.GetContainerSas(permissions, startTime, endTime).TrimStart('?');

                return uriBuilder.Uri;
            },
            cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Gets the container Uri with read only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The container Uri with read only SAS.</returns>
        public Task<Uri> GetContainerUriWithSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetContainerUriWithSasAsync(SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan), cancellationToken);
        }

        /// <summary>
        /// Begins an operation to start copying source block blob's contents, properties, and metadata to the destination block blob.
        /// </summary>
        /// <param name="sourceBlobName">Name of the source blob.</param>
        /// <param name="destBlobName">Name of the destination blob.</param>
        /// <param name="destContainer">The destination container.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source blob does not exist.</returns>
        public Task<string> StartCopyBlockBlobAsync(string sourceBlobName, string destBlobName, BlobContainer destContainer = null, CancellationToken? cancellationToken = null)
        {
            sourceBlobName.ValidateBlobName();
            destBlobName.ValidateBlobName();

            return Task.Run(() =>
            {
                var sourceBlob = this.GetBlockBlob(sourceBlobName);

                if (sourceBlob.Exists())
                {
                    var destBlob = (destContainer ?? this).GetBlockBlob(destBlobName);

                    return destBlob.StartCopyAsync(sourceBlob, cancellationToken ?? CancellationToken.None);
                }
                else
                {
                    return Task.FromResult(string.Empty);
                }
            },
            cancellationToken ?? CancellationToken.None);
        }
    }
}
