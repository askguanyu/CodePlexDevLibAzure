//-----------------------------------------------------------------------
// <copyright file="BlobClient.cs" company="YuGuan Corporation">
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
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Shared.Protocol;

    /// <summary>
    /// Provides a client-side logical representation of Microsoft Azure Blob storage.
    /// </summary>
    public class BlobClient
    {
        /// <summary>
        /// The dev store account blob client.
        /// </summary>
        private static BlobClient DevStoreAccountBlobClient;

        /// <summary>
        /// Field _cloudBlobClient.
        /// </summary>
        private readonly CloudBlobClient _cloudBlobClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobClient"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public BlobClient(string connectionString)
        {
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            this._cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.SetDefaultRetryIfNotExists(this._cloudBlobClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobClient"/> class.
        /// </summary>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public BlobClient(string accountName, string keyValue, bool useHttps = true)
        {
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            this._cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.SetDefaultRetryIfNotExists(this._cloudBlobClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlobClient"/> class.
        /// </summary>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public BlobClient(CloudStorageAccount cloudStorageAccount)
        {
            cloudStorageAccount.ValidateNull();

            this._cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.SetDefaultRetryIfNotExists(this._cloudBlobClient);
        }

        /// <summary>
        /// Prevents a default instance of the <see cref="BlobClient"/> class from being created.
        /// </summary>
        private BlobClient()
            : this(StorageConstants.DevelopmentStorageConnectionString)
        {
        }

        /// <summary>
        /// Gets the development blob client.
        /// </summary>
        /// <value>The development blob client.</value>
        public static BlobClient DevelopmentClient
        {
            get
            {
                if (DevStoreAccountBlobClient == null)
                {
                    DevStoreAccountBlobClient = new BlobClient();
                }

                return DevStoreAccountBlobClient;
            }
        }

        /// <summary>
        /// Gets the inner cloud blob client.
        /// </summary>
        public CloudBlobClient InnerCloudBlobClient
        {
            get
            {
                return this._cloudBlobClient;
            }
        }

        /// <summary>
        /// Sets the blob client CORS.
        /// </summary>
        /// <returns>The current BlobClient.</returns>
        public BlobClient SetCors()
        {
            var serviceProperties = this._cloudBlobClient.GetServiceProperties();

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

            this._cloudBlobClient.SetServiceProperties(serviceProperties);

            return this;
        }

        /// <summary>
        /// Gets the blob container.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="newContainerAccessType">Access type for the newly created container.</param>
        /// <returns>BlobContainer instance.</returns>
        public BlobContainer GetContainer(string containerName, bool createIfNotExists, BlobContainerPublicAccessType newContainerAccessType)
        {
            containerName.ValidateContainerName();

            var container = this._cloudBlobClient.GetContainerReference(containerName);

            if (createIfNotExists)
            {
                if (container.CreateIfNotExists())
                {
                    var permissions = container.GetPermissions();
                    permissions.PublicAccess = newContainerAccessType;
                    container.SetPermissions(permissions);
                }
            }

            return new BlobContainer(container);
        }

        /// <summary>
        /// Gets the blob container.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="createIfNotExists">true to creates the container if it does not already exist; otherwise, false.</param>
        /// <param name="isNewContainerPublic">true to set the newly created container to public; false to set it to private.</param>
        /// <returns>BlobContainer instance.</returns>
        public BlobContainer GetContainer(string containerName, bool createIfNotExists = true, bool isNewContainerPublic = true)
        {
            return this.GetContainer(containerName, createIfNotExists, isNewContainerPublic ? BlobContainerPublicAccessType.Container : BlobContainerPublicAccessType.Off);
        }

        /// <summary>
        /// Deletes the container if it already exists.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <returns>true if the container did not already exist and was created; otherwise false.</returns>
        public bool DeleteContainerIfExists(string containerName)
        {
            containerName.ValidateContainerName();

            var container = this._cloudBlobClient.GetContainerReference(containerName);

            return container.DeleteIfExists();
        }

        /// <summary>
        /// Checks whether the container exists.
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <returns>true if the container exists; otherwise false.</returns>
        public bool ContainerExists(string containerName)
        {
            containerName.ValidateContainerName();

            var container = this._cloudBlobClient.GetContainerReference(containerName);

            return container.Exists();
        }

        /// <summary>
        /// Returns an enumerable collection of BlobContainer.
        /// </summary>
        /// <param name="prefix">A string containing the container name prefix.</param>
        /// <param name="detailsIncluded">A Microsoft.WindowsAzure.Storage.Blob.ContainerListingDetails enumeration value that indicates whether to return container metadata with the listing.</param>
        /// <returns>List of BlobContainer.</returns>
        public List<BlobContainer> ListContainers(string prefix = null, ContainerListingDetails detailsIncluded = ContainerListingDetails.None)
        {
            var containers = this._cloudBlobClient.ListContainers(prefix, detailsIncluded);

            return containers.Select(i => new BlobContainer(i)).ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the BlobClient.
        /// </summary>
        /// <param name="prefix">A string containing the container name prefix.</param>
        /// <param name="detailsIncluded">A Microsoft.WindowsAzure.Storage.Blob.ContainerListingDetails enumeration value that indicates whether to return container metadata with the listing.</param>
        /// <returns>The number of elements contained in the BlobClient.</returns>
        public int ContainersCount(string prefix = null, ContainerListingDetails detailsIncluded = ContainerListingDetails.None)
        {
            return this._cloudBlobClient.ListContainers(prefix, detailsIncluded).Count();
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
