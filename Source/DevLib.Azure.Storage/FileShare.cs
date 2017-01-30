//-----------------------------------------------------------------------
// <copyright file="fileshare.cs" company="YuGuan Corporation">
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
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Represents a share in the Microsoft Azure File service.
    /// </summary>
    public class FileShare
    {
        /// <summary>
        /// Field _cloudFileShare.
        /// </summary>
        private readonly CloudFileShare _cloudFileShare;

        /// <summary>
        /// Field _fileClient.
        /// </summary>
        private readonly FileClient _fileClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare" /> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="fileClient">The file client.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, FileClient fileClient, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            fileClient.ValidateNull();

            this._fileClient = fileClient;

            this._cloudFileShare = this._fileClient.InnerCloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare" /> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="cloudFileClient">The cloud file client.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, CloudFileClient cloudFileClient, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            cloudFileClient.ValidateNull();

            this._fileClient = new FileClient(cloudFileClient);

            this._cloudFileShare = this._fileClient.InnerCloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare"/> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, string connectionString, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            connectionString.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            var cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this._fileClient = new FileClient(cloudFileClient);
            this.SetDefaultRetryIfNotExists(cloudFileClient);

            this._cloudFileShare = cloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare"/> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, string accountName, string keyValue, bool useHttps, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            accountName.ValidateNullOrWhiteSpace();
            keyValue.ValidateNullOrWhiteSpace();

            var cloudStorageAccount = new CloudStorageAccount(new StorageCredentials(accountName, keyValue), useHttps);
            var cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this._fileClient = new FileClient(cloudFileClient);
            this.SetDefaultRetryIfNotExists(cloudFileClient);

            this._cloudFileShare = cloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare" /> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, StorageCredentials storageCredentials, bool useHttps, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            storageCredentials.ValidateNull();

            var cloudStorageAccount = new CloudStorageAccount(storageCredentials, useHttps);
            var cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this._fileClient = new FileClient(cloudFileClient);
            this.SetDefaultRetryIfNotExists(cloudFileClient);

            this._cloudFileShare = cloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare" /> class.
        /// </summary>
        /// <param name="shareName">A string containing the name of the share.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        /// <param name="createIfNotExists">true to creates the share if it does not already exist; otherwise, false.</param>
        public FileShare(string shareName, CloudStorageAccount cloudStorageAccount, bool createIfNotExists = true)
        {
            shareName.ValidateShareName();
            cloudStorageAccount.ValidateNull();

            var cloudFileClient = cloudStorageAccount.CreateCloudFileClient();
            this._fileClient = new FileClient(cloudFileClient);
            this.SetDefaultRetryIfNotExists(cloudFileClient);

            this._cloudFileShare = cloudFileClient.GetShareReference(shareName);

            if (createIfNotExists)
            {
                this._cloudFileShare.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileShare"/> class.
        /// </summary>
        /// <param name="fileShare">The CloudFileShare instance.</param>
        public FileShare(CloudFileShare fileShare)
        {
            this._cloudFileShare = fileShare;
            this._fileClient = new FileClient(fileShare.ServiceClient);
        }

        /// <summary>
        /// Gets the inner cloud file share.
        /// </summary>
        /// <value>The inner cloud file share.</value>
        public CloudFileShare InnerCloudFileShare
        {
            get
            {
                return this._cloudFileShare;
            }
        }

        /// <summary>
        /// Gets the service client.
        /// </summary>
        public FileClient ServiceClient
        {
            get
            {
                return this._fileClient;
            }
        }

        /// <summary>
        /// Returns a reference to the directory for this share.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.File.CloudFileDirectory object.</returns>
        public CloudFileDirectory GetDirectory(string directoryName = null)
        {
            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();

            return directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
        }

        /// <summary>
        /// Returns a Microsoft.WindowsAzure.Storage.File.CloudFile object that represents a file in this directory.
        /// </summary>
        /// <param name="fileName">A System.String containing the name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.File.CloudFile object.</returns>
        public CloudFile GetFile(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            return file;
        }

        /// <summary>
        /// Creates the share if it does not already exist.
        /// </summary>
        /// <returns>FileShare instance.</returns>
        public FileShare CreateIfNotExists()
        {
            this._cloudFileShare.CreateIfNotExists();

            return this;
        }

        /// <summary>
        /// Checks whether the share exists.
        /// </summary>
        /// <returns>true if the share exists; otherwise, false.</returns>
        public bool ShareExists()
        {
            return this._cloudFileShare.Exists();
        }

        /// <summary>
        /// Checks whether the directory exists.
        /// </summary>
        /// <param name="directoryName">Name of the directory.</param>
        /// <returns>true if the directory exists; otherwise, false.</returns>
        public bool DirectoryExists(string directoryName)
        {
            directoryName.ValidateDirectoryName();

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = rootDirectory.GetDirectoryReference(directoryName);

            return directory.Exists();
        }

        /// <summary>
        /// Checks existence of the file.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>true if the file exists; otherwise, false.</returns>
        public bool FileExists(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            return file.Exists();
        }

        /// <summary>
        /// Creates the directory if it does not already exist.
        /// </summary>
        /// <param name="directoryName">Name of the directory.</param>
        /// <returns>true if the directory did not already exist and was created; otherwise false.</returns>
        public bool CreateDirectoryIfNotExists(string directoryName)
        {
            directoryName.ValidateDirectoryName();

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = rootDirectory.GetDirectoryReference(directoryName);

            return directory.CreateIfNotExists();
        }

        /// <summary>
        /// Creates a file. If the file already exists, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void OverwriteFile(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            file.Create(long.MaxValue);
        }

        /// <summary>
        /// Creates a file if it does not already exist.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void CreateFileIfNotExists(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            if (!file.Exists())
            {
                file.Create(long.MaxValue);
            }
        }

        /// <summary>
        /// Deletes the directory if it already exists.
        /// </summary>
        /// <param name="directoryName">Name of the directory.</param>
        /// <returns>true if the directory did already exist and was deleted; otherwise false.</returns>
        public bool DeleteDirectoryIfExists(string directoryName)
        {
            directoryName.ValidateDirectoryName();

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = rootDirectory.GetDirectoryReference(directoryName);

            return directory.DeleteIfExists();
        }

        /// <summary>
        /// Deletes the file if it already exists.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>true if the file did already exist and was deleted; otherwise false.</returns>
        public bool DeleteFileIfExists(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            return file.DeleteIfExists();
        }

        /// <summary>
        /// Deletes the share if it already exists.
        /// </summary>
        /// <returns>true if the share did not already exist and was created; otherwise false.</returns>
        public bool DeleteShareIfExists()
        {
            return this._cloudFileShare.DeleteIfExists();
        }

        /// <summary>
        /// Uploads a string of text to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The text to upload.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void CreateFile(string fileName, string data, string directoryName = null)
        {
            fileName.ValidateFileName();
            data.ValidateNull();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            if (!file.Exists())
            {
                file.Create(long.MaxValue);
            }

            file.UploadText(data);
        }

        /// <summary>
        /// Uploads a stream to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The stream providing the file content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void CreateFile(string fileName, Stream data, string directoryName = null)
        {
            fileName.ValidateFileName();
            data.ValidateNull();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            if (!file.Exists())
            {
                file.Create(long.MaxValue);
            }

            file.UploadFromStream(data);
        }

        /// <summary>
        /// Uploads the contents of a byte array to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The byte array providing the file content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void CreateFile(string fileName, byte[] data, string directoryName = null)
        {
            fileName.ValidateFileName();
            data.ValidateNull();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            if (!file.Exists())
            {
                file.Create(long.MaxValue);
            }

            file.UploadFromByteArray(data, 0, data.Length);
        }

        /// <summary>
        /// Uploads a file to the File service. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="sourceFilePath">The file providing the content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        public void UploadFile(string fileName, string sourceFilePath, string directoryName = null)
        {
            fileName.ValidateFileName();
            sourceFilePath.ValidateNull();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            if (!file.Exists())
            {
                file.Create(long.MaxValue);
            }

            file.UploadFromFile(sourceFilePath);
        }

        /// <summary>
        /// Downloads the file's contents as a string.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The contents of the file, as a string.</returns>
        public string DownloadText(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            return file.DownloadText();
        }

        /// <summary>
        /// Downloads the contents of a file to a stream.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The contents of the file, as MemoryStream.</returns>
        public MemoryStream DownloadToStream(string fileName, string directoryName = null)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            var stream = new MemoryStream();
            file.DownloadToStream(stream);
            stream.Seek(0, SeekOrigin.Begin);

            return stream;
        }

        /// <summary>
        /// Downloads the contents of a file in the File service to a local file.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="localFilePath">The path to the target file in the local file system.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="mode">A System.IO.FileMode enumeration value that determines how to open or create the file.</param>
        public void DownloadToFile(string fileName, string localFilePath, string directoryName = null, FileMode mode = FileMode.Create)
        {
            fileName.ValidateFileName();

            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;
            var file = directory.GetFileReference(fileName);

            file.DownloadToFile(localFilePath, mode);
        }

        /// <summary>
        /// Returns an enumerable collection of the files in the share.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>An enumerable collection of objects that implement Microsoft.WindowsAzure.Storage.File.IListFileItem.</returns>
        public List<IListFileItem> ListFilesAndDirectories(string directoryName = null)
        {
            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;

            return directory.ListFilesAndDirectories().ToList();
        }

        /// <summary>
        /// Gets the number of elements contained in the share.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The number of elements contained in the share.</returns>
        public int FilesAndDirectoriesCount(string directoryName = null)
        {
            if (directoryName != null)
            {
                directoryName.ValidateDirectoryName();
            }

            var rootDirectory = this._cloudFileShare.GetRootDirectoryReference();
            var directory = directoryName != null ? rootDirectory.GetDirectoryReference(directoryName) : rootDirectory;

            return directory.ListFilesAndDirectories().Count();
        }

        /// <summary>
        /// Returns a shared access signature for the file.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileSAS(string fileName, string directoryName = null, SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this.GetFile(fileName, directoryName).GetSharedAccessSignature(new SharedAccessFilePolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Returns a shared access signature for the file with read only access.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileSASReadOnly(string fileName, TimeSpan expiryTimeSpan, string directoryName = null)
        {
            return this.GetFileSAS(fileName, directoryName, SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the file URI.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The file URI.</returns>
        public Uri GetFileUri(string fileName, string directoryName = null)
        {
            return this.GetFile(fileName, directoryName).Uri;
        }

        /// <summary>
        /// Gets the directory URI for the primary location.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The directory URI for the primary location.</returns>
        public Uri GetDirectoryUri(string directoryName = null)
        {
            return this.GetDirectory(directoryName).Uri;
        }

        /// <summary>
        /// Gets the file Uri with SAS.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The file Uri with SAS.</returns>
        public Uri GetFileUriWithSAS(string fileName, string directoryName = null, SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var file = this.GetFile(fileName, directoryName);

            var uriBuilder = new UriBuilder(file.Uri);

            uriBuilder.Query = file.GetSharedAccessSignature(new SharedAccessFilePolicy()
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            }).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the file Uri with read only SAS.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <returns>The file Uri with read only SAS.</returns>
        public Uri GetFileUriWithSASReadOnly(string fileName, TimeSpan expiryTimeSpan, string directoryName = null)
        {
            return this.GetFileUriWithSAS(fileName, directoryName, SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Returns a shared access signature for the file share.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileShareSAS(SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            return this._cloudFileShare.GetSharedAccessSignature(new SharedAccessFilePolicy
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            });
        }

        /// <summary>
        /// Returns a shared access signature for the file share with read only access.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileShareSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetFileShareSAS(SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the file share URI.
        /// </summary>
        /// <returns>The file share URI.</returns>
        public Uri GetFileShareUri()
        {
            return this._cloudFileShare.Uri;
        }

        /// <summary>
        /// Gets the file share Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <returns>The file share Uri with SAS.</returns>
        public Uri GetFileShareUriWithSAS(SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null)
        {
            var uriBuilder = new UriBuilder(this._cloudFileShare.Uri);

            uriBuilder.Query = this._cloudFileShare.GetSharedAccessSignature(new SharedAccessFilePolicy()
            {
                Permissions = permissions,
                SharedAccessStartTime = startTime,
                SharedAccessExpiryTime = endTime
            }).TrimStart('?');

            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets the file share Uri with read only SAS.
        /// </summary>
        /// <param name="expiryTimeSpan">The expiry time span.</param>
        /// <returns>The file share Uri with read only SAS.</returns>
        public Uri GetFileShareUriWithSASReadOnly(TimeSpan expiryTimeSpan)
        {
            return this.GetFileShareUriWithSAS(SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Begins an operation to start copying another file's contents, properties, and metadata to this file.
        /// </summary>
        /// <param name="sourceFileName">Name of the source file.</param>
        /// <param name="destFileName">Name of the destination file.</param>
        /// <param name="sourceDirectoryName">Name of the source directory; null will get the root directory.</param>
        /// <param name="destDirectoryName">Name of the destination directory; null will get the root directory.</param>
        /// <param name="destFileShare">The destination file share.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source file does not exist.</returns>
        public string StartCopyFile(string sourceFileName, string destFileName, string sourceDirectoryName = null, string destDirectoryName = null, FileShare destFileShare = null)
        {
            sourceFileName.ValidateFileName();

            if (sourceDirectoryName != null)
            {
                sourceDirectoryName.ValidateDirectoryName();
            }

            destFileName.ValidateFileName();

            if (destDirectoryName != null)
            {
                destDirectoryName.ValidateDirectoryName();
            }

            var sourceFile = this.GetFile(sourceFileName, sourceDirectoryName);

            if (sourceFile.Exists())
            {
                var destFile = (destFileShare ?? this).GetFile(destFileName, destDirectoryName);

                return destFile.StartCopy(sourceFile);
            }
            else
            {
                return string.Empty;
            }
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
