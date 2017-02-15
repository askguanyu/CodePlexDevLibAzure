//-----------------------------------------------------------------------
// <copyright file="BlobContainer.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    /// <summary>
    /// Represents a container in the Microsoft Azure Blob service.
    /// </summary>
    public partial class BlobContainer
    {
        /// <summary>
        /// Field _cloudBlobContainer.
        /// </summary>
        private readonly CloudBlobContainer _cloudBlobContainer;

        /// <summary>
        /// Field _blobClient.
        /// </summary>
        private readonly BlobClient _blobClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="blobClient">The blob client.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, BlobClient blobClient, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            blobClient.ValidateNull();

            this._blobClient = blobClient;

            this._cloudBlobContainer = this._blobClient.InnerCloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="blobClient">The blob client.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, BlobClient blobClient, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, blobClient, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudBlobClient">The cloud blob client.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, CloudBlobClient cloudBlobClient, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            cloudBlobClient.ValidateNull();

            this._blobClient = new BlobClient(cloudBlobClient);

            this._cloudBlobContainer = this._blobClient.InnerCloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudBlobClient">The cloud blob client.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, CloudBlobClient cloudBlobClient, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, cloudBlobClient, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, string connectionString, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this._blobClient = new BlobClient(cloudBlobClient);
            this.SetDefaultRetryIfNotExists(cloudBlobClient);

            this._cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, string connectionString, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, connectionString, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, string accountName, string keyValue, bool useHttps, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this._blobClient = new BlobClient(cloudBlobClient);
            this.SetDefaultRetryIfNotExists(cloudBlobClient);

            this._cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer"/> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, string accountName, string keyValue, bool useHttps = true, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, accountName, keyValue, useHttps, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, StorageCredentials storageCredentials, bool useHttps, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            storageCredentials.ValidateNull();

            var cloudStorageAccount = new CloudStorageAccount(storageCredentials, useHttps);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this._blobClient = new BlobClient(cloudBlobClient);
            this.SetDefaultRetryIfNotExists(cloudBlobClient);

            this._cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, StorageCredentials storageCredentials, bool useHttps = true, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, storageCredentials, useHttps, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        public BlobContainer(string containerName, CloudStorageAccount cloudStorageAccount, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();
            cloudStorageAccount.ValidateNull();

            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this._blobClient = new BlobClient(cloudBlobClient);
            this.SetDefaultRetryIfNotExists(cloudBlobClient);

            this._cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (this._cloudBlobContainer.CreateIfNotExists())
                {
                    var permissions = this._cloudBlobContainer.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    this._cloudBlobContainer.SetPermissions(permissions);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer" /> class.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        public BlobContainer(string containerName, CloudStorageAccount cloudStorageAccount, bool createIfNotExists = true, bool isNewContainerPublic = true)
            : this(containerName, cloudStorageAccount, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobContainer"/> class.
        /// </summary>
        /// <param name="cloudBlobContainer">The CloudBlobContainer instance.</param>
        public BlobContainer(CloudBlobContainer cloudBlobContainer)
        {
            this._cloudBlobContainer = cloudBlobContainer;
            this._blobClient = new BlobClient(cloudBlobContainer.ServiceClient);
        }

        /// <summary>
        /// Gets the inner cloud blob container.
        /// </summary>
        /// <value>The inner cloud blob container.</value>
        public CloudBlobContainer InnerCloudBlobContainer
        {
            get
            {
                return this._cloudBlobContainer;
            }
        }

        /// <summary>
        /// Gets the service client.
        /// </summary>
        public BlobClient ServiceClient
        {
            get
            {
                return this._blobClient;
            }
        }

        /// <summary>
        /// Begins an operation to start copying source block blob's contents, properties, and metadata to the destination block blob.
        /// </summary>
        /// <param name="sourceBlob">The source blob.</param>
        /// <param name="destBlob">The destination blob.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source blob does not exist.</returns>
        public static string StartCopyBlockBlob(CloudBlockBlob sourceBlob, CloudBlockBlob destBlob)
        {
            sourceBlob.ValidateNull();
            destBlob.ValidateNull();

            if (sourceBlob.Exists())
            {
                return destBlob.StartCopy(sourceBlob);
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Sets permissions for the container.
        /// </summary>
        /// <param name="accessType">The public access setting for the container.</param>
        /// <returns>BlobContainer instance.</returns>
        public BlobContainer SetAccessPermission(BlobContainerPublicAccessType accessType)
        {
            var permissions = this._cloudBlobContainer.GetPermissions();
            permissions.PublicAccess = accessType;
            this._cloudBlobContainer.SetPermissions(permissions);

            return this;
        }

        /// <summary>
        /// Sets permissions for the container.
        /// </summary>
        /// <param name="isPublic">true to set the container to public; false to set it to private.</param>
        /// <returns>BlobContainer instance.</returns>
        public BlobContainer SetAccessPermission(bool isPublic)
        {
            return this.SetAccessPermission(isPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off);
        }

        /// <summary>
        /// Creates the container if it does not already exist.
        /// </summary>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public bool CreateIfNotExists(BlobContainerPublicAccessType newContainerAccessType)
        {
            var result = this._cloudBlobContainer.CreateIfNotExists();

            if (result)
            {
                this.SetAccessPermission(newContainerAccessType);
            }

            return result;
        }

        /// <summary>
        /// Creates the container if it does not already exist.
        /// </summary>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public bool CreateIfNotExists(bool isNewContainerPublic = true)
        {
            return this.CreateIfNotExists(isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off);
        }

        /// <summary>
        /// Checks whether the container exists.
        /// </summary>
        /// <returns>true if the container exists; otherwise false.</returns>
        public bool ContainerExists()
        {
            return this._cloudBlobContainer.Exists();
        }

        /// <summary>
        /// Checks existence of the blob.
        /// </summary>
        /// <param name="blobName">Name of the blob.</param>
        /// <returns>true if the container exists; otherwise false.</returns>
        public bool Exists(string blobName)
        {
            blobName.ValidateBlobName();

            var blob = this._cloudBlobContainer.GetBlobReference(blobName);

            return blob.Exists();
        }

        /// <summary>
        /// Returns a list of all the blobs in the container.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <returns>List of all the blobs in the container.</returns>
        public List<IListBlobItem> ListBlobs(bool useFlatBlobListing = false)
        {
            return this._cloudBlobContainer.ListBlobs(useFlatBlobListing: useFlatBlobListing).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the container.
        /// </summary>
        /// <param name="useFlatBlobListing">A boolean value that specifies whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        /// <returns>The number of elements contained in the container.</returns>
        public int BlobsCount(bool useFlatBlobListing = false)
        {
            return this._cloudBlobContainer.ListBlobs(useFlatBlobListing: useFlatBlobListing).Count();
        }

        /// <summary>
        /// Deletes the container if it already exists.
        /// </summary>
        /// <returns>true if the container did not already exist and was deleted; otherwise false.</returns>
        public bool DeleteContainerIfExists()
        {
            return this._cloudBlobContainer.DeleteIfExists();
        }

        /// <summary>
        /// Deletes the blob if it already exists.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>true if the blob did already exist and was deleted; otherwise false.</returns>
        public bool DeleteBlobIfExists(string blobName)
        {
            blobName.ValidateBlobName();

            var blob = this._cloudBlobContainer.GetBlobReference(blobName);
            return blob.DeleteIfExists();
        }

        /// <summary>
        /// Uploads a stream to a block blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A System.IO.Stream object providing the blob content.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public CloudBlockBlob CreateBlockBlob(string blobName, Stream data, string contentType = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            if (!string.IsNullOrWhiteSpace(contentType))
            {
                blob.Properties.ContentType = contentType;
            }

            blob.UploadFromStream(data);

            return blob;
        }

        /// <summary>
        /// Uploads the contents of a byte array to a blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">An array of bytes.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public CloudBlockBlob CreateBlockBlob(string blobName, byte[] data, string contentType = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            if (!string.IsNullOrWhiteSpace(contentType))
            {
                blob.Properties.ContentType = contentType;
            }

            blob.UploadFromByteArray(data, 0, data.Length);

            return blob;
        }

        /// <summary>
        /// Uploads a string of text to a blob. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A string containing the text to upload.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public CloudBlockBlob CreateBlockBlob(string blobName, string data, string contentType = null)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            if (!string.IsNullOrWhiteSpace(contentType))
            {
                blob.Properties.ContentType = contentType;
            }

            blob.UploadText(data);

            return blob;
        }

        /// <summary>
        /// Uploads a file to the Blob service. If the blob already exists, it will be overwritten.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="filePath">A string containing the file path providing the blob content.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public CloudBlockBlob UploadBlockBlob(string blobName, string filePath, string contentType = null)
        {
            blobName.ValidateBlobName();
            filePath.ValidateNull();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            if (!string.IsNullOrWhiteSpace(contentType))
            {
                blob.Properties.ContentType = contentType;
            }

            blob.UploadFromFile(filePath);

            return blob;
        }

        /// <summary>
        /// Gets a reference to a block blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob object.</returns>
        public CloudBlockBlob GetBlockBlob(string blobName)
        {
            blobName.ValidateBlobName();

            return this._cloudBlobContainer.GetBlockBlobReference(blobName);
        }

        /// <summary>
        /// Downloads the blob's contents as a string.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>The contents of the blob, as a string.</returns>
        public string DownloadBlockBlobText(string blobName)
        {
            blobName.ValidateBlobName();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            return blob.DownloadText();
        }

        /// <summary>
        /// Downloads the contents of a blob to a stream.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>A System.IO.Stream object representing the target stream.</returns>
        public MemoryStream DownloadBlockBlobToStream(string blobName)
        {
            blobName.ValidateBlobName();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);

            var stream = new MemoryStream();
            blob.DownloadToStream(stream);
            stream.Seek(0, SeekOrigin.Begin);

            return stream;
        }

        /// <summary>
        /// Downloads the contents of a blob to a file.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="localFilePath">A string containing the path to the target file.</param>
        /// <param name="mode">A System.IO.FileMode enumeration value that determines how to open or create the file.</param>
        /// <returns>The CloudBlockBlob instance.</returns>
        public CloudBlockBlob DownloadBlockBlobToFile(string blobName, string localFilePath, FileMode mode = FileMode.Create)
        {
            blobName.ValidateBlobName();

            var blob = this._cloudBlobContainer.GetBlockBlobReference(blobName);
            blob.DownloadToFile(localFilePath, mode);

            return blob;
        }

        /// <summary>
        /// Appends a string of text to an append blob.
        /// This API should be used strictly in a single writer scenario because the API internally uses the append-offset conditional header to avoid duplicate blocks which does not work in a multiple writer scenario.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="data">A string containing the text to upload.</param>
        /// <returns>The CloudAppendBlob instance.</returns>
        public CloudAppendBlob AppendBlobAppendText(string blobName, string data)
        {
            blobName.ValidateBlobName();
            data.ValidateNull();

            var blob = this._cloudBlobContainer.GetAppendBlobReference(blobName);

            if (!blob.Exists())
            {
                blob.CreateOrReplace();
            }

            blob.AppendText(data);

            return blob;
        }

        /// <summary>
        /// Gets a reference to an append blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudAppendBlob object.</returns>
        public CloudAppendBlob GetAppendBlob(string blobName)
        {
            blobName.ValidateBlobName();

            return this._cloudBlobContainer.GetAppendBlobReference(blobName);
        }

        /// <summary>
        /// Gets a reference to a blob in this container.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.Blob.CloudBlob object.</returns>
        public CloudBlob GetBlob(string blobName)
        {
            blobName.ValidateBlobName();

            return this._cloudBlobContainer.GetBlobReference(blobName);
        }

        /// <summary>
        /// Gets a shared access signature for the blob.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetBlobSas(string blobName, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this.GetBlob(blobName).GetSharedAccessSignature(new SharedAccessBlobPolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Gets a shared access signature for the blob with read only access.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetBlobSasReadOnly(string blobName, TimeSpan expiryTimeSpan)
        {
            return this.GetBlobSas(blobName, SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the blob Uri.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <returns>The blob Uri.</returns>
        public Uri GetBlobUri(string blobName)
        {
            return this.GetBlob(blobName).Uri;
        }

        /// <summary>
        /// Gets the blob Uri with SAS.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The blob Uri with SAS.</returns>
        public Uri GetBlobUriWithSas(string blobName, SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var blob = this.GetBlob(blobName);

            var uriBuilder = new UriBuilder(blob.Uri);

            uriBuilder.Query = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            }).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the blob Uri with read only SAS.
        /// </summary>
        /// <param name="blobName">A string containing the name of the blob.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The blob Uri with read only SAS.</returns>
        public Uri GetBlobUriWithSasReadOnly(string blobName, TimeSpan expiryTimeSpan)
        {
            return this.GetBlobUriWithSas(blobName, SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets a shared access signature for the container.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetContainerSas(SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this._cloudBlobContainer.GetSharedAccessSignature(new SharedAccessBlobPolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Gets a shared access signature for the container with read only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetContainerSasReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetContainerSas(SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the container Uri.
        /// </summary>
        /// <returns>The container Uri.</returns>
        public Uri GetContainerUri()
        {
            return this._cloudBlobContainer.Uri;
        }

        /// <summary>
        /// Gets the container Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The container Uri with SAS.</returns>
        public Uri GetContainerUriWithSas(SharedAccessBlobPermissions permissions = SharedAccessBlobPermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var uriBuilder = new UriBuilder(this._cloudBlobContainer.Uri);

            uriBuilder.Query = this.GetContainerSas(permissions, startTime, endTime).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the container Uri with read only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The container Uri with read only SAS.</returns>
        public Uri GetContainerUriWithSasReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetContainerUriWithSas(SharedAccessBlobPermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Begins an operation to start copying source block blob's contents, properties, and metadata to the destination block blob.
        /// </summary>
        /// <param name="sourceBlobName">Name of the source blob.</param>
        /// <param name="destBlobName">Name of the destination blob.</param>
        /// <param name="destContainer">The destination container.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source blob does not exist.</returns>
        public string StartCopyBlockBlob(string sourceBlobName, string destBlobName, BlobContainer destContainer = null)
        {
            sourceBlobName.ValidateBlobName();
            destBlobName.ValidateBlobName();

            var sourceBlob = this._cloudBlobContainer.GetBlockBlobReference(sourceBlobName);

            if (sourceBlob.Exists())
            {
                var destBlob = (destContainer ?? this).GetBlockBlob(destBlobName);

                return destBlob.StartCopy(sourceBlob);
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Sets the default retry.
        /// </summary>
        /// <param name="cloudBlobClient">The CloudBlobClient instance.</param>
        private void SetDefaultRetryIfNotExists(CloudBlobClient cloudBlobClient)
        {
            if (cloudBlobClient.DefaultRequestOptions == null)
            {
                cloudBlobClient.DefaultRequestOptions = new BlobRequestOptions();
            }

            if (cloudBlobClient.DefaultRequestOptions.RetryPolicy == null)
            {
                cloudBlobClient.DefaultRequestOptions.RetryPolicy = StorageConstants.DefaultExponentialRetry;
            }
        }
    }
}
