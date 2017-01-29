//-----------------------------------------------------------------------
// <copyright file="FileClient.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.File;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Provides a client-side logical representation of the Microsoft Azure File service. This client is used to configure and execute requests against the File service.
    /// </summary>
    public class FileClient
    {
        /// <summary>
        /// The dev store account file client.
        /// </summary>
        private static FileClient DevStoreAccountFileClient;

        /// <summary>
        /// Field _cloudFileClient.
        /// </summary>
        private readonly CloudFileClient _cloudFileClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileClient"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public FileClient(string connectionString)
        {
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            this._cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this.SetDefaultRetryIfNotExists(this._cloudFileClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileClient"/> class.
        /// </summary>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public FileClient(string accountName, string keyValue, bool useHttps = true)
        {
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            this._cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this.SetDefaultRetryIfNotExists(this._cloudFileClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileClient"/> class.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public FileClient(CloudStorageAccount cloudStorageAccount)
        {
            cloudStorageAccount.ValidateNull();

            this._cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this.SetDefaultRetryIfNotExists(this._cloudFileClient);
        }

        /// <summary>
        /// Prevents a default instance of the <see cref="FileClient"/> class from being created.
        /// </summary>
        private FileClient()
            : this(StorageConstants.DevelopmentStorageConnectionString)
        {
        }

        /// <summary>
        /// Gets the development file client.
        /// </summary>
        /// <value>The development file client.</value>
        public static FileClient DevelopmentClient
        {
            get
            {
                if (DevStoreAccountFileClient == null)
                {
                    DevStoreAccountFileClient = new FileClient();
                }

                return DevStoreAccountFileClient;
            }
        }

        /// <summary>
        /// Gets the inner cloud file client.
        /// </summary>
        public CloudFileClient InnerCloudFileClient
        {
            get
            {
                return this._cloudFileClient;
            }
        }

        /// <summary>
        /// Sets the file client CORS.
        /// </summary>
        /// <returns>The current FileClient.</returns>
        public FileClient SetCors()
        {
            var serviceProperties = this._cloudFileClient.GetServiceProperties();

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

            this._cloudFileClient.SetServiceProperties(serviceProperties);

            return this;
        }

        /// <summary>
        /// Gets the file share.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        /// <returns>FileShare instance.</returns>
        public FileShare GetShare(string shareName, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();

            var share = this._cloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                share.CreateIfNotExists();
            }

            return new FileShare(share);
        }

        /// <summary>
        /// Deletes the share if it already exists.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <returns>true if the share did not already exist and was created; otherwise false.</returns>
        public bool DeleteContainerIfExists(string shareName)
        {
            shareName.ValidateShareName();

            var share = this._cloudFileClient.GetShareReference(shareName);

            return share.DeleteIfExists();
        }

        /// <summary>
        /// Checks whether the share exists.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <returns>true if the share exists; otherwise false.</returns>
        public bool ShareExists(string shareName)
        {
            shareName.ValidateShareName();

            var share = this._cloudFileClient.GetShareReference(shareName);

            return share.Exists();
        }

        /// <summary>
        /// Returns an enumerable collection of FileShare.
        /// </summary>
        /// <param name="prefix">The share name prefix.</param>
        /// <param name="detailsIncluded">A value that indicates whether to return share metadata with the listing.</param>
        /// <returns>List of FileShare.</returns>
        public List<FileShare> ListShares(string prefix = null, ShareListingDetails detailsIncluded = ShareListingDetails.None)
        {
            var shares = this._cloudFileClient.ListShares(prefix, detailsIncluded);

            return shares.Select(i => new FileShare(i)).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the FileShare.
        /// </summary>
        /// <param name="prefix">The share name prefix.</param>
        /// <param name="detailsIncluded">A value that indicates whether to return share metadata with the listing.</param>
        /// <returns>The number of elements contained in the FileShare.</returns>
        public int SharesCount(string prefix = null, ShareListingDetails detailsIncluded = ShareListingDetails.None)
        {
            return this._cloudFileClient.ListShares(prefix, detailsIncluded).Count();
        }

        /// <summary>
        /// Sets the default retry.
        /// </summary>
        /// <param name="cloudFileClient">The CloudFileClient instance.</param>
        private void SetDefaultRetryIfNotExists(CloudFileClient cloudFileClient)
        {
            if (cloudFileClient.DefaultRequestOptions == null)
            {
                cloudFileClient.DefaultRequestOptions = new FileRequestOptions();
            }

            if (cloudFileClient.DefaultRequestOptions.RetryPolicy == null)
            {
                cloudFileClient.DefaultRequestOptions.RetryPolicy = StorageConstants.DefaultExponentialRetry;
            }
        }
    }
}
