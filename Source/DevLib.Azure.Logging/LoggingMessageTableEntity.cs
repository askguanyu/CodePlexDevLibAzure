//-----------------------------------------------------------------------
// <copyright file="LoggingMessageTableEntity.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents the logging message object type for a table entity in the Table service.
    /// </summary>
    public class LoggingMessageTableEntity : TableEntity
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LoggingMessageTableEntity"/> class.
        /// </summary>
        public LoggingMessageTableEntity()
            : base()
        {
            this.EventTickCount = Stopwatch.GetTimestamp();
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LoggingMessageTableEntity"/> class.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="rowKey">The row key.</param>
        public LoggingMessageTableEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
            this.EventTickCount = Stopwatch.GetTimestamp();
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
        }

        /// <summary>
        /// Gets or sets the date and time that the event occurred, in Tick format (greater precision)
        /// </summary>
        public long EventTickCount
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the user name of the person who is currently logged on to the Windows operating system.
        /// </summary>
        public string User
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the network domain name associated with the current user.
        /// </summary>
        public string Domain
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets application name.
        /// </summary>
        public string ApplicationName
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets event level (e.g. error, warning, information)
        /// </summary>
        public string Level
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the event ID of this event. Defaults to 0 if none specified.
        /// </summary>
        public string EventId
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets instance of the web app that the even occurred on.
        /// </summary>
        public string InstanceId
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the process ID.
        /// </summary>
        public string Pid
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the thread ID of the thread that produced the event.
        /// </summary>
        public string Tid
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets event detail message.
        /// </summary>
        public string Message
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets information of the stack trace.
        /// </summary>
        public string StackInfo
        {
            get;
            set;
        }
    }
}
