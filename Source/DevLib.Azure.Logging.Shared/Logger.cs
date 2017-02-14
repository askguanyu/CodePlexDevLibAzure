//-----------------------------------------------------------------------
// <copyright file="Logger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Class Logger.
    /// </summary>
    public class Logger : LoggerBase, ILogger
    {
        /// <summary>
        /// The trace logger.
        /// </summary>
        private static readonly TraceLogger GlobalTraceLogger = new TraceLogger();

        /// <summary>
        /// The loggers.
        /// </summary>
        private readonly List<ILogger> _loggers = new List<ILogger>();

        /// <summary>
        /// The console logger.
        /// </summary>
        private readonly ConsoleLogger _consoleLogger = new ConsoleLogger();

        /// <summary>
        /// The debug logger.
        /// </summary>
        private readonly DebugLogger _debugLogger = new DebugLogger();

        /// <summary>
        /// Adds the logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>Current Logger instance.</returns>
        public Logger Add(ILogger logger)
        {
            if (logger != null && !this._loggers.Contains(logger))
            {
                this._loggers.Add(logger);
            }

            return this;
        }

        /// <summary>
        /// Adds the console logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public Logger AddConsole()
        {
            if (!this._loggers.Contains(this._consoleLogger))
            {
                this._loggers.Add(this._consoleLogger);
            }

            return this;
        }

        /// <summary>
        /// Adds the debug logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public Logger AddDebug()
        {
            if (!this._loggers.Contains(this._debugLogger))
            {
                this._loggers.Add(this._debugLogger);
            }

            return this;
        }

        /// <summary>
        /// Adds the trace logger.
        /// </summary>
        /// <returns>Current Logger instance.</returns>
        public Logger AddTrace()
        {
            if (!this._loggers.Contains(GlobalTraceLogger))
            {
                this._loggers.Add(GlobalTraceLogger);
            }

            return this;
        }

        /// <summary>
        /// Adds the trace logger.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>Current Logger instance.</returns>
        public Logger AddTrace(params TraceListener[] listeners)
        {
            if (!this._loggers.Contains(GlobalTraceLogger))
            {
                this._loggers.Add(GlobalTraceLogger);
            }

            GlobalTraceLogger.AddListeners(listeners);

            return this;
        }

        /// <summary>
        /// Adds the table logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>Current Logger instance.</returns>
        public Logger AddTableLogger(TableLogger logger)
        {
            if (logger != null && !this._loggers.Contains(logger))
            {
                this._loggers.Add(logger);
            }

            return this;
        }

        /// <summary>
        /// Adds the blob logger.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>Current Logger instance.</returns>
        public Logger AddBlobLogger(BlobLogger logger)
        {
            if (logger != null && !this._loggers.Contains(logger))
            {
                this._loggers.Add(logger);
            }

            return this;
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LogLevel level, LogMessageTableEntity messageEntity)
        {
            try
            {
                foreach (var logger in this._loggers)
                {
                    try
                    {
                        logger.Log(messageEntity);
                    }
                    catch (Exception e)
                    {
                        InternalLogger.Log(e);
                    }
                }
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }
    }
}
