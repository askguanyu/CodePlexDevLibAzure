//-----------------------------------------------------------------------
// <copyright file="ValidatorExtensions.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Provides helpers to validate resource names across the Microsoft Azure Storage Services.
    /// </summary>
    public static class ValidatorExtensions
    {
        /// <summary>
        /// Checks if a blob name is valid.
        /// </summary>
        /// <param name="blobName">A string representing the blob name to validate.</param>
        public static void ValidateBlobName(this string blobName)
        {
            NameValidator.ValidateBlobName(blobName);
        }

        /// <summary>
        /// Checks if a container name is valid.
        /// </summary>
        /// <param name="containerName">A string representing the container name to validate.</param>
        public static void ValidateContainerName(this string containerName)
        {
            NameValidator.ValidateContainerName(containerName);
        }

        /// <summary>
        /// Checks if a directory name is valid.
        /// </summary>
        /// <param name="directoryName">A string representing the directory name to validate.</param>
        public static void ValidateDirectoryName(this string directoryName)
        {
            NameValidator.ValidateDirectoryName(directoryName);
        }

        /// <summary>
        /// Checks if a file name is valid.
        /// </summary>
        /// <param name="fileName">A string representing the file name to validate.</param>
        public static void ValidateFileName(this string fileName)
        {
            NameValidator.ValidateFileName(fileName);
        }

        /// <summary>
        /// Checks if a queue name is valid.
        /// </summary>
        /// <param name="queueName">A string representing the queue name to validate.</param>
        public static void ValidateQueueName(this string queueName)
        {
            NameValidator.ValidateQueueName(queueName);
        }

        /// <summary>
        /// Checks if a share name is valid.
        /// </summary>
        /// <param name="shareName">A string representing the share name to validate.</param>
        public static void ValidateShareName(this string shareName)
        {
            NameValidator.ValidateShareName(shareName);
        }

        /// <summary>
        /// Checks if a table name is valid.
        /// </summary>
        /// <param name="tableName">A string representing the table name to validate.</param>
        public static void ValidateTableName(this string tableName)
        {
            NameValidator.ValidateTableName(tableName);
        }

        /// <summary>
        /// Checks if parameter value is null.
        /// </summary>
        /// <param name="parameterValue">Parameter value.</param>
        public static void ValidateNull(this object parameterValue)
        {
            if (parameterValue == null)
            {
                throw new ArgumentException("Parameter must not be null.");
            }
        }

        /// <summary>
        /// Checks if parameter string is null Or white space.
        /// </summary>
        /// <param name="parameterValue">Parameter string.</param>
        public static void ValidateNullOrWhiteSpace(this string parameterValue)
        {
            if (string.IsNullOrWhiteSpace(parameterValue))
            {
                throw new ArgumentException("Parameter string length must be greater than zero.");
            }
        }
    }
}
