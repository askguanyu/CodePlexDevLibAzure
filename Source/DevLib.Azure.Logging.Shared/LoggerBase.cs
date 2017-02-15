//-----------------------------------------------------------------------
// <copyright file="LoggerBase.cs" company="YuGuan Corporation">
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

    /// <summary>
    /// Class LoggerBase.
    /// </summary>
    public abstract class LoggerBase : ILogger
    {
        /// <summary>
        /// Gets or sets the level criteria.
        /// </summary>
        public LogLevel LogLevelCriteria { get; set; } = LogLevel.ALL;

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
        /// Logs the specified message entity.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(LogMessageTableEntity messageEntity)
        {
            if (messageEntity != null)
            {
                this.InternalLog(LogLevel.ALL, messageEntity);
            }

            return this;
        }

        /// <summary>
        /// Logs the specified message entity.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(LogLevel level, LogMessageTableEntity messageEntity)
        {
            if (messageEntity != null && level != LogLevel.OFF)
            {
                this.InternalLog(level, messageEntity);
            }

            return this;
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(LogLevel level, object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LogLevel.OFF)
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
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(LogLevel level, object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LogLevel.OFF)
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
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.ALL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the messages.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger Log(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.ALL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogDebug(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.DBG, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogDebug(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.DBG, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogInfo(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.INF, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogInfo(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.INF, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogWarning(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.WRN, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogWarning(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.WRN, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogException(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.EXP, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogException(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.EXP, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogError(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.ERR, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogError(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.ERR, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogFatal(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.FAL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        public ILogger LogFatal(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return this.InternalLog(LogLevel.FAL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        protected virtual void InternalLog(LogLevel level, LogMessageTableEntity messageEntity)
        {
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        private ILogger InternalLog(LogLevel level, object message, string applicationName, string eventId, string instanceId)
        {
            if (level < this.LogLevelCriteria)
            {
                return this;
            }

            var messageEntity = new LogMessageTableEntity
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

            this.InternalLog(level, messageEntity);

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
        /// <returns>The current ILogger instance.</returns>
        private ILogger InternalLog(LogLevel level, object[] messages, string applicationName, string eventId, string instanceId)
        {
            if (level < this.LogLevelCriteria)
            {
                return this;
            }

            var messageEntity = new LogMessageTableEntity
            {
                Level = level.ToString(),
                Message = messages != null ? string.Join(", ", messages.Select(i => i?.ToString() ?? string.Empty)) : string.Empty,
                StackTrace = GetStackFrameInfo(2),
                EventId = eventId,
                InstanceId = instanceId
            };

            if (!string.IsNullOrWhiteSpace(applicationName))
            {
                messageEntity.ApplicationName = applicationName;
            }

            this.InternalLog(level, messageEntity);

            return this;
        }
    }
}
