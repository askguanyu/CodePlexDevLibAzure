//-----------------------------------------------------------------------
// <copyright file="Logger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using DevLib.Azure.Storage;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Class Logger.
    /// </summary>
    public class Logger
    {
        /// <summary>
        /// The table storage.
        /// </summary>
        private readonly TableStorage _tableStorage;

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableStorage">The table storage.</param>
        public Logger(TableStorage tableStorage)
        {
            this._tableStorage = tableStorage;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="cloudTable">The cloud table.</param>
        public Logger(CloudTable cloudTable)
        {
            try
            {
                this._tableStorage = new TableStorage(cloudTable);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="tableClient">The table client.</param>
        public Logger(string tableName, TableClient tableClient)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, tableClient);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudTableClient">The cloud table client.</param>
        public Logger(string tableName, CloudTableClient cloudTableClient)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, cloudTableClient);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="connectionString">The connection string.</param>
        public Logger(string tableName, string connectionString)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, connectionString);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="accountName">A string that represents the name of the storage account.</param>
        /// <param name="keyValue">A string that represents the Base64-encoded account access key.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public Logger(string tableName, string accountName, string keyValue, bool useHttps = true)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, accountName, keyValue);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="storageCredentials">A Microsoft.WindowsAzure.Storage.Auth.StorageCredentials object.</param>
        /// <param name="useHttps">true to use HTTPS to connect to storage service endpoints; otherwise, false.</param>
        public Logger(string tableName, StorageCredentials storageCredentials, bool useHttps = true)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, storageCredentials);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Logger"/> class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="cloudStorageAccount">The cloud storage account.</param>
        public Logger(string tableName, CloudStorageAccount cloudStorageAccount)
        {
            try
            {
                this._tableStorage = new TableStorage(tableName, cloudStorageAccount);
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }

        /// <summary>
        /// Builds a readable representation of the stack trace.
        /// </summary>
        /// <param name="skipFrames">The number of frames up the stack to skip.</param>
        /// <returns>A readable representation of the stack trace.</returns>
        public static string GetStackFrameInfo(int skipFrames = 0)
        {
            StackFrame stackFrame = new StackFrame(skipFrames + 1, true);

            MethodBase method = stackFrame.GetMethod();

            if (method != null)
            {
                StringBuilder result = new StringBuilder();

                result.Append(method.Name);

                if (method is MethodInfo && ((MethodInfo)method).IsGenericMethod)
                {
                    Type[] genericArguments = ((MethodInfo)method).GetGenericArguments();

                    result.Append("<");

                    int i = 0;

                    bool flag = true;

                    while (i < genericArguments.Length)
                    {
                        if (!flag)
                        {
                            result.Append(",");
                        }
                        else
                        {
                            flag = false;
                        }

                        result.Append(genericArguments[i].Name);

                        i++;
                    }

                    result.Append(">");
                }

                result.Append(" in ");
                result.Append(Path.GetFileName(stackFrame.GetFileName()) ?? "<unknown>");
                result.Append(":");
                result.Append(stackFrame.GetFileLineNumber());

                return result.ToString();
            }
            else
            {
                return "<null>";
            }
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger Log(LoggingLevel level, object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LoggingLevel.OFF)
            {
                return this;
            }

            return this.InternalLog(level, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the messages with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger Log(LoggingLevel level, object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LoggingLevel.OFF)
            {
                return this;
            }

            return this.InternalLog(level, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger Log(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.ALL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the messages.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger Log(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.ALL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogDebug(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.DBG, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogDebug(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.DBG, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogInfo(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.INF, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogInfo(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.INF, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogWarning(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.WRN, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogWarning(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.WRN, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogException(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.EXP, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogException(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.EXP, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogError(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.ERR, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogError(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.ERR, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogFatal(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.FAL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        public Logger LogFatal(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LoggingLevel.FAL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        private Logger InternalLog(LoggingLevel level, object message, string applicationName, string eventId, string instanceId)
        {
            var messageEntity = new LoggingMessageTableEntity
            {
                Level = level.ToString(),
                Message = message?.ToString() ?? string.Empty,
                StackTrace = GetStackFrameInfo(2),
                EventId = eventId,
                InstanceId = instanceId
            };

            if (!string.IsNullOrWhiteSpace(applicationName))
            {
                messageEntity.ApplicationName = applicationName;
            }

            this.InternalLog(messageEntity);

            return this;
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current Logger.</returns>
        private Logger InternalLog(LoggingLevel level, object[] messages, string applicationName, string eventId, string instanceId)
        {
            var messageEntity = new LoggingMessageTableEntity
            {
                Level = level.ToString(),
                Message = messages != null ? string.Join(" ", messages.Select(i => i?.ToString() ?? string.Empty)) : string.Empty,
                StackTrace = GetStackFrameInfo(2),
                EventId = eventId,
                InstanceId = instanceId
            };

            if (!string.IsNullOrWhiteSpace(applicationName))
            {
                messageEntity.ApplicationName = applicationName;
            }

            this.InternalLog(messageEntity);

            return this;
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        private void InternalLog(LoggingMessageTableEntity messageEntity)
        {
            if (this._tableStorage != null)
            {
                try
                {
                    this._tableStorage.Insert(messageEntity);
                }
                catch (Exception e)
                {
                    InternalLogger.Log(e);

                    try
                    {
                        messageEntity.RowKey = Guid.NewGuid().ToString();
                        this._tableStorage.Insert(messageEntity);
                    }
                    catch (Exception ex)
                    {
                        InternalLogger.Log(ex);
                    }
                }
            }
            else
            {
                InternalLogger.Log("DevLib.Azure.Logging.Logger._tableStorage is null");
            }
        }
    }
}
