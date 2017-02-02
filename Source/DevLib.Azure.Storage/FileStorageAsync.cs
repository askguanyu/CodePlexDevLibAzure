//-----------------------------------------------------------------------
// <copyright file="FileStorageAsync.cs" company="YuGuan Corporation">
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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.File;

    /// <summary>
    /// Represents a share in the Microsoft Azure File service.
    /// </summary>
    public partial class FileStorage
    {
        /// <summary>
        /// Returns a reference to the directory for this share.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.File.CloudFileDirectory object.</returns>
        public CloudFileDirectory GetDirectoryAsync(string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>A Microsoft.WindowsAzure.Storage.File.CloudFile object.</returns>
        public CloudFile GetFileAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>FileShare instance.</returns>
        public FileStorage CreateIfNotExistsAsync(CancellationToken? cancellationToken = null)
        {
            this._cloudFileShare.CreateIfNotExists();

            return this;
        }

        /// <summary>
        /// Checks whether the share exists.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the share exists; otherwise, false.</returns>
        public bool ShareExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudFileShare.Exists();
        }

        /// <summary>
        /// Checks whether the directory exists.
        /// </summary>
        /// <param name="directoryName">Name of the directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the directory exists; otherwise, false.</returns>
        public bool DirectoryExistsAsync(string directoryName, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the file exists; otherwise, false.</returns>
        public bool FileExistsAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the directory did not already exist and was created; otherwise false.</returns>
        public bool CreateDirectoryIfNotExistsAsync(string directoryName, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile OverwriteFileAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Creates a file if it does not already exist.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile CreateFileIfNotExistsAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Deletes the directory if it already exists.
        /// </summary>
        /// <param name="directoryName">Name of the directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the directory did already exist and was deleted; otherwise false.</returns>
        public bool DeleteDirectoryIfExistsAsync(string directoryName, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the file did already exist and was deleted; otherwise false.</returns>
        public bool DeleteFileIfExistsAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>true if the share did not already exist and was created; otherwise false.</returns>
        public bool DeleteShareIfExistsAsync(CancellationToken? cancellationToken = null)
        {
            return this._cloudFileShare.DeleteIfExists();
        }

        /// <summary>
        /// Uploads a string of text to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The text to upload.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile CreateFileAsync(string fileName, string data, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Uploads a stream to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The stream providing the file content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile CreateFileAsync(string fileName, Stream data, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Uploads the contents of a byte array to a file. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="data">The byte array providing the file content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile CreateFileAsync(string fileName, byte[] data, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Uploads a file to the File service. If the file already exists on the service, it will be overwritten.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="sourceFilePath">The file providing the content.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile UploadFileAsync(string fileName, string sourceFilePath, string directoryName = null, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Downloads the file's contents as a string.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The contents of the file, as a string.</returns>
        public string DownloadTextAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The contents of the file, as MemoryStream.</returns>
        public MemoryStream DownloadToStreamAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The CloudFile instance.</returns>
        public CloudFile DownloadToFileAsync(string fileName, string localFilePath, string directoryName = null, FileMode mode = FileMode.Create, CancellationToken? cancellationToken = null)
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

            return file;
        }

        /// <summary>
        /// Returns an enumerable collection of the files in the share.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>An enumerable collection of objects that implement Microsoft.WindowsAzure.Storage.File.IListFileItem.</returns>
        public List<IListFileItem> ListFilesAndDirectoriesAsync(string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The number of elements contained in the share.</returns>
        public int FilesAndDirectoriesCountAsync(string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileSasAsync(string fileName, string directoryName = null, SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileSasReadOnlyAsync(string fileName, TimeSpan expiryTimeSpan, string directoryName = null, CancellationToken? cancellationToken = null)
        {
            return this.GetFileSas(fileName, directoryName, SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the file URI.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file URI.</returns>
        public Uri GetFileUriAsync(string fileName, string directoryName = null, CancellationToken? cancellationToken = null)
        {
            return this.GetFile(fileName, directoryName).Uri;
        }

        /// <summary>
        /// Gets the directory URI for the primary location.
        /// </summary>
        /// <param name="directoryName">A System.String containing the name of the subdirectory; null will get the root directory.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The directory URI for the primary location.</returns>
        public Uri GetDirectoryUriAsync(string directoryName = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file Uri with SAS.</returns>
        public Uri GetFileUriWithSasAsync(string fileName, string directoryName = null, SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file Uri with read only SAS.</returns>
        public Uri GetFileUriWithSasReadOnlyAsync(string fileName, TimeSpan expiryTimeSpan, string directoryName = null, CancellationToken? cancellationToken = null)
        {
            return this.GetFileUriWithSas(fileName, directoryName, SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Returns a shared access signature for the file share.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileShareSasAsync(SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The query string returned includes the leading question mark.</returns>
        public string GetFileShareSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetFileShareSas(SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Gets the file share URI.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file share URI.</returns>
        public Uri GetFileShareUriAsync()
        {
            return this._cloudFileShare.Uri;
        }

        /// <summary>
        /// Gets the file share Uri with SAS.
        /// </summary>
        /// <param name="permissions">The permissions for a shared access signature associated with this shared access policy.</param>
        /// <param name="startTime">The start time for a shared access signature associated with this shared access policy.</param>
        /// <param name="endTime">The expiry time for a shared access signature associated with this shared access policy.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file share Uri with SAS.</returns>
        public Uri GetFileShareUriWithSasAsync(SharedAccessFilePermissions permissions = SharedAccessFilePermissions.Read, DateTimeOffset? startTime = null, DateTimeOffset? endTime = null, CancellationToken? cancellationToken = null)
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
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The file share Uri with read only SAS.</returns>
        public Uri GetFileShareUriWithSasReadOnlyAsync(TimeSpan expiryTimeSpan, CancellationToken? cancellationToken = null)
        {
            return this.GetFileShareUriWithSas(SharedAccessFilePermissions.Read, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.Add(expiryTimeSpan));
        }

        /// <summary>
        /// Begins an operation to start copying another file's contents, properties, and metadata to this file.
        /// </summary>
        /// <param name="sourceFileName">Name of the source file.</param>
        /// <param name="destFileName">Name of the destination file.</param>
        /// <param name="sourceDirectoryName">Name of the source directory; null will get the root directory.</param>
        /// <param name="destDirectoryName">Name of the destination directory; null will get the root directory.</param>
        /// <param name="destFileShare">The destination file share.</param>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> to observe while waiting for a task to complete.</param>
        /// <returns>The copy ID associated with the copy operation; empty if source file does not exist.</returns>
        public string StartCopyFileAsync(string sourceFileName, string destFileName, string sourceDirectoryName = null, string destDirectoryName = null, FileStorage destFileShare = null, CancellationToken? cancellationToken = null)
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

    }
}
