//-----------------------------------------------------------------------
// <copyright file="LogFactory.cs" company="YuGuan Corporation">
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
    /// Class LogFactory.
    /// </summary>
    public static class LogFactory
    {
        /// <summary>
        /// The global logger
        /// </summary>
        private static readonly Logger GlobalLogger = new Logger();

        /// <summary>
        /// Gets the logger.
        /// </summary>
        /// <value>The logger.</value>
        public static Logger Logger
        {
            get
            {
                return GlobalLogger;
            }
        }

        /// <summary>
        /// Gets or sets the level criteria.
        /// </summary>
        public static LogLevel LogLevelCriteria
        {
            get
            {
                return GlobalLogger.LogLevelCriteria;
            }

            set
            {
                GlobalLogger.LogLevelCriteria = value;
            }
        }

        /// <summary>
        /// Adds the logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>The global logger instance.</returns>
        public static Logger Add(ILogger logger)
        {
            return GlobalLogger.Add(logger);
        }

        /// <summary>
        /// Adds the console logger.
        /// </summary>
        /// <returns>The global logger instance.</returns>
        public static Logger AddConsole()
        {
            return GlobalLogger.AddConsole();
        }

        /// <summary>
        /// Adds the debug logger.
        /// </summary>
        /// <returns>The global logger instance.</returns>
        public static Logger AddDebug()
        {
            return GlobalLogger.AddDebug();
        }

        /// <summary>
        /// Adds the trace logger.
        /// </summary>
        /// <returns>The global logger instance.</returns>
        public static Logger AddTrace()
        {
            return GlobalLogger.AddTrace();
        }

        /// <summary>
        /// Adds the trace logger with listeners.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>The global logger instance.</returns>
        public static Logger AddTrace(params TraceListener[] listeners)
        {
            return GlobalLogger.AddTrace(listeners);
        }

        /// <summary>
        /// Adds the table logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>The global logger instance.</returns>
        public static Logger AddTableLogger(TableLogger logger)
        {
            return GlobalLogger.AddTableLogger(logger);
        }

        /// <summary>
        /// Adds the blob logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>The global logger instance.</returns>
        public static Logger AddBlobLogger(BlobLogger logger)
        {
            return GlobalLogger.AddBlobLogger(logger);
        }

        /// <summary>
        /// Removes the logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>Current Logger instance.</returns>
        public static Logger Remove(ILogger logger)
        {
            return GlobalLogger.Remove(logger);
        }

        /// <summary>
        /// Removes the console logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public static Logger RemoveConsole()
        {
            return GlobalLogger.RemoveConsole();
        }

        /// <summary>
        /// Removes the debug logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public static Logger RemoveDebug()
        {
            return GlobalLogger.RemoveDebug();
        }

        /// <summary>
        /// Removes the trace logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public static Logger RemoveTrace()
        {
            return GlobalLogger.RemoveTrace();
        }

        /// <summary>
        /// Removes the trace logger listeners.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>Current Logger instance.</returns>
        public static Logger RemoveTrace(params TraceListener[] listeners)
        {
            return GlobalLogger.RemoveTrace(listeners);
        }

        /// <summary>
        /// Removes all loggers.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public static Logger Clear()
        {
            return GlobalLogger.Clear();
        }

        /// <summary>
        /// Logs the specified message entity.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(LogMessageTableEntity messageEntity)
        {
            if (messageEntity != null)
            {
                InternalLog(LogLevel.ALL, messageEntity);
            }

            return GlobalLogger;
        }

        /// <summary>
        /// Logs the specified message entity.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(LogLevel level, LogMessageTableEntity messageEntity)
        {
            if (messageEntity != null && level != LogLevel.OFF)
            {
                InternalLog(level, messageEntity);
            }

            return GlobalLogger;
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(LogLevel level, object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LogLevel.OFF)
            {
                return GlobalLogger;
            }

            return InternalLog(level, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the messages with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(LogLevel level, object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            if (level == LogLevel.OFF)
            {
                return GlobalLogger;
            }

            return InternalLog(level, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.ALL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the messages.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger Log(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.ALL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogDebug(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.DBG, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogDebug(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.DBG, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogInfo(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.INF, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogInfo(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.INF, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogWarning(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.WRN, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogWarning(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.WRN, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogException(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.EXP, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogException(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.EXP, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogError(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.ERR, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogError(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.ERR, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogFatal(object message, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.FAL, message, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        public static ILogger LogFatal(object[] messages, string applicationName = null, string eventId = null, string instanceId = null)
        {
            return InternalLog(LogLevel.FAL, messages, applicationName, eventId, instanceId);
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        private static ILogger InternalLog(LogLevel level, object message, string applicationName, string eventId, string instanceId)
        {
            if (level < LogLevelCriteria)
            {
                return GlobalLogger;
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

            InternalLog(level, messageEntity);

            return GlobalLogger;
        }

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The global logger instance.</returns>
        private static ILogger InternalLog(LogLevel level, object[] messages, string applicationName, string eventId, string instanceId)
        {
            if (level < LogLevelCriteria)
            {
                return GlobalLogger;
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

            InternalLog(level, messageEntity);

            return GlobalLogger;
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        private static void InternalLog(LogLevel level, LogMessageTableEntity messageEntity)
        {
            GlobalLogger.Log(level, messageEntity);
        }

        /// <summary>
        /// Builds a readable representation of the stack trace.
        /// </summary>
        /// <param name="skipFrames">The number of frames up the stack to skip.</param>
        /// <returns>A readable representation of the stack trace.</returns>
        private static string GetStackFrameInfo(int skipFrames = 0)
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
    }
}
