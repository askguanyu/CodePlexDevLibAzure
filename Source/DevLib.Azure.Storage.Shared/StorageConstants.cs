//-----------------------------------------------------------------------
// <copyright file="StorageConstants.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

    /// <summary>
    /// Storage Constants.
    /// </summary>
    public static class StorageConstants
    {
        /// <summary>
        /// The development storage connection string.
        /// </summary>
        public const string DevelopmentStorageConnectionString = "UseDevelopmentStorage=true";

        /// <summary>
        /// Represents a retry policy that performs three (3) times of retries, using one (1) second exponential back off scheme to determine the interval between retries.
        /// </summary>
        public static IRetryPolicy DefaultExponentialRetry = new ExponentialRetry(TimeSpan.FromSeconds(1), 3);
    }
}
