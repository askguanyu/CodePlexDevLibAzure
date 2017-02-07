//-----------------------------------------------------------------------
// <copyright file="LogMessageTableEntity.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using DevLib.Azure.Storage;

    /// <summary>
    /// Represents the logging message object type for a table entity in the Table service.
    /// </summary>
    public class LogMessageTableEntity : DictionaryTableEntity
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
        /// Initializes a new instance of the <see cref="LogMessageTableEntity"/> class.
        /// </summary>
        public LogMessageTableEntity()
            : base()
        {
            var tickCount = Stopwatch.GetTimestamp();
            var timestamp = DateTimeOffset.Now;
            var currentProcess = Process.GetCurrentProcess();

            this.PartitionKey = timestamp.ToString(DateFormat, CultureInfo.InvariantCulture);
            this.RowKey = timestamp.ToString(TimeFormat, CultureInfo.InvariantCulture) + tickCount;
            this.Timestamp = timestamp;
            this.Level = LogLevel.ALL.ToString();
            this.Message = string.Empty;
            this.EventTickCount = tickCount;
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
            this.Machine = Environment.MachineName;
            this.WorkingSet = Environment.WorkingSet;
            this.ApplicationName = currentProcess.ProcessName;
            this.EventId = string.Empty;
            this.InstanceId = string.Empty;
            this.Pid = currentProcess.Id;
            this.Tid = Environment.CurrentManagedThreadId;
            this.CommandLine = Environment.CommandLine;
            this.StackTrace = Environment.StackTrace;
            this.Is64BitProcess = Environment.Is64BitProcess;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LogMessageTableEntity"/> class.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="rowKey">The row key.</param>
        public LogMessageTableEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
            var tickCount = Stopwatch.GetTimestamp();
            var timestamp = DateTimeOffset.Now;
            var currentProcess = Process.GetCurrentProcess();

            this.Timestamp = timestamp;
            this.Level = LogLevel.ALL.ToString();
            this.Message = string.Empty;
            this.EventTickCount = tickCount;
            this.User = Environment.UserName;
            this.Domain = Environment.UserDomainName;
            this.Machine = Environment.MachineName;
            this.WorkingSet = Environment.WorkingSet;
            this.ApplicationName = currentProcess.ProcessName;
            this.EventId = string.Empty;
            this.InstanceId = string.Empty;
            this.Pid = currentProcess.Id;
            this.Tid = Environment.CurrentManagedThreadId;
            this.CommandLine = Environment.CommandLine;
            this.StackTrace = Environment.StackTrace;
            this.Is64BitProcess = Environment.Is64BitProcess;
        }

        /// <summary>
        /// Gets or sets event level (e.g. error, warning, information)
        /// </summary>
        public string Level
        {
            get
            {
                return this.GetValue<string>(nameof(this.Level), false);
            }

            set
            {
                this[nameof(this.Level)] = value;
            }
        }

        /// <summary>
        /// Gets or sets event detail message.
        /// </summary>
        public string Message
        {
            get
            {
                return this.GetValue<string>(nameof(this.Message), false);
            }

            set
            {
                this[nameof(this.Message)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the date and time that the event occurred, in Tick format (greater precision)
        /// </summary>
        public long EventTickCount
        {
            get
            {
                return this.GetValue<long>(nameof(this.EventTickCount), false);
            }

            set
            {
                this[nameof(this.EventTickCount)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the user name of the person who is currently logged on to the Windows operating system.
        /// </summary>
        public string User
        {
            get
            {
                return this.GetValue<string>(nameof(this.User), false);
            }

            set
            {
                this[nameof(this.User)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the network domain name associated with the current user.
        /// </summary>
        public string Domain
        {
            get
            {
                return this.GetValue<string>(nameof(this.Domain), false);
            }

            set
            {
                this[nameof(this.Domain)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the NetBIOS name of the computer.
        /// </summary>
        public string Machine
        {
            get
            {
                return this.GetValue<string>(nameof(this.Machine), false);
            }

            set
            {
                this[nameof(this.Machine)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the NetBIOS name of the computer.
        /// </summary>
        public long WorkingSet
        {
            get
            {
                return this.GetValue<long>(nameof(this.WorkingSet), false);
            }

            set
            {
                this[nameof(this.WorkingSet)] = value;
            }
        }

        /// <summary>
        /// Gets or sets application name.
        /// </summary>
        public string ApplicationName
        {
            get
            {
                return this.GetValue<string>(nameof(this.ApplicationName), false);
            }

            set
            {
                this[nameof(this.ApplicationName)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the event ID of this event. Defaults to null if none specified.
        /// </summary>
        public string EventId
        {
            get
            {
                return this.GetValue<string>(nameof(this.EventId), false);
            }

            set
            {
                this[nameof(this.EventId)] = value;
            }
        }

        /// <summary>
        /// Gets or sets instance of the web app that the even occurred on. Defaults to null if none specified.
        /// </summary>
        public string InstanceId
        {
            get
            {
                return this.GetValue<string>(nameof(this.InstanceId), false);
            }

            set
            {
                this[nameof(this.InstanceId)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the process ID.
        /// </summary>
        public int Pid
        {
            get
            {
                return this.GetValue<int>(nameof(this.Pid), false);
            }

            set
            {
                this[nameof(this.Pid)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the thread ID of the thread that produced the event.
        /// </summary>
        public int Tid
        {
            get
            {
                return this.GetValue<int>(nameof(this.Tid), false);
            }

            set
            {
                this[nameof(this.Tid)] = value;
            }
        }

        /// <summary>
        /// Gets or sets the command line for this process.
        /// </summary>
        /// <value>The command line.</value>
        public string CommandLine
        {
            get
            {
                return this.GetValue<string>(nameof(this.CommandLine), false);
            }

            set
            {
                this[nameof(this.CommandLine)] = value;
            }
        }

        /// <summary>
        /// Gets or sets information of the stack trace.
        /// </summary>
        public string StackTrace
        {
            get
            {
                return this.GetValue<string>(nameof(this.StackTrace), false);
            }

            set
            {
                this[nameof(this.StackTrace)] = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the current process is a 64-bit process.
        /// </summary>
        public bool Is64BitProcess
        {
            get
            {
                return this.GetValue<bool>(nameof(this.Is64BitProcess), false);
            }

            set
            {
                this[nameof(this.Is64BitProcess)] = value;
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"[PK: {this.PartitionKey}] [RK: {this.RowKey}] [Level: {this.Level}] [Message: {this.Message}] [Pid: {this.Pid}] [Tid: {this.Tid}] [Timestamp: {this.Timestamp.UtcDateTime.ToString(DateTimeFormat)}] [EventTickCount: {this.EventTickCount}] [User: {this.User}] [Domain: {this.Domain}] [Machine: {this.Machine}] [WorkingSet: {this.WorkingSet}] [ApplicationName: {this.ApplicationName}] [EventId: {this.EventId}] [InstanceId: {this.InstanceId}] [StackTrace: {this.StackTrace}] [CmdLine: {this.CommandLine}] [Is64Bit: {this.Is64BitProcess}]".Replace(Environment.NewLine, " ");
        }
    }
}
