//-----------------------------------------------------------------------
// <copyright file="LoggingMessageTableEntity.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Represents the logging message object type for a table entity in the Table service.
    /// </summary>
    public class LoggingMessageTableEntity : TableEntity
    {
        /// <summary>
        /// The date format.
        /// </summary>
        private const string DateFormat = "yyyy-MM-ddTHHUz";

        /// <summary>
        /// The time format.
        /// </summary>
        private const string TimeFormat = "HH:mm:ss.fffUz_";

        /// <summary>
        /// The date time format.
        /// </summary>
        private const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

        /// <summary>
        /// Initializes a new instance of the <see cref="LoggingMessageTableEntity"/> class.
        /// </summary>
        public LoggingMessageTableEntity()
            : base()
        {
            this.EventTickCount = Stopwatch.GetTimestamp();
            this.Timestamp = DateTimeOffset.Now;
            this.PartitionKey = this.Timestamp.ToString(DateFormat, CultureInfo.InvariantCulture);
            this.RowKey = this.Timestamp.ToString(TimeFormat, CultureInfo.InvariantCulture) + this.EventTickCount;
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
            this.Machine = Environment.MachineName;
            this.WorkingSet = Environment.WorkingSet;
            this.Tid = Environment.CurrentManagedThreadId;
            this.CommandLine = Environment.CommandLine;
            this.Is64BitProcess = Environment.Is64BitProcess;
            this.StackTrace = Environment.StackTrace;
            Process currentProcess = Process.GetCurrentProcess();
            this.Pid = currentProcess.Id;
            this.ApplicationName = currentProcess.ProcessName;
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
            this.Timestamp = DateTimeOffset.Now;
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
            this.Machine = Environment.MachineName;
            this.WorkingSet = Environment.WorkingSet;
            this.Tid = Environment.CurrentManagedThreadId;
            this.CommandLine = Environment.CommandLine;
            this.Is64BitProcess = Environment.Is64BitProcess;
            this.StackTrace = Environment.StackTrace;
            Process currentProcess = Process.GetCurrentProcess();
            this.Pid = currentProcess.Id;
            this.ApplicationName = currentProcess.ProcessName;
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
        /// Gets or sets event detail message.
        /// </summary>
        public string Message
        {
            get;
            set;
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
        /// Gets or sets the NetBIOS name of the computer.
        /// </summary>
        public string Machine
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the NetBIOS name of the computer.
        /// </summary>
        public long WorkingSet
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
        /// Gets or sets the event ID of this event. Defaults to null if none specified.
        /// </summary>
        public string EventId
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets instance of the web app that the even occurred on. Defaults to null if none specified.
        /// </summary>
        public string InstanceId
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the process ID.
        /// </summary>
        public int Pid
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the thread ID of the thread that produced the event.
        /// </summary>
        public int Tid
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets information of the stack trace.
        /// </summary>
        public string StackTrace
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the command line for this process.
        /// </summary>
        /// <value>The command line.</value>
        public string CommandLine
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the current process is a 64-bit process.
        /// </summary>
        public bool Is64BitProcess
        {
            get;
            set;
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"[PK: {this.PartitionKey}] [RK: {this.RowKey}] [Timestamp: {this.Timestamp.UtcDateTime.ToString(DateTimeFormat)}] [Level: {this.Level}] [Message: {this.Message}] [EventTickCount: {this.EventTickCount}] [User: {this.User}] [Domain: {this.Domain}] [Machine: {this.Machine}] [WorkingSet: {this.WorkingSet}] [ApplicationName: {this.ApplicationName}] [EventId: {this.EventId}] [InstanceId: {this.InstanceId}] [Pid: {this.Pid}] [Tid: {this.Tid}] [StackTrace: {this.StackTrace}] [CmdLine: {this.CommandLine}] [Is64Bit: {this.Is64BitProcess}]".Replace(Environment.NewLine, " ");
        }
    }
}
