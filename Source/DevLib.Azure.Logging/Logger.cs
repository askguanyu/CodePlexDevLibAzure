//-----------------------------------------------------------------------
// <copyright file="Logger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Class Logger.
    /// </summary>
    public class Logger : LoggerBase, ILogger
    {
        /// <summary>
        /// The loggers.
        /// </summary>
        private readonly List<ILogger> _loggers = new List<ILogger>();

        /// <summary>
        /// The console logger.
        /// </summary>
        private readonly ILogger _consoleLogger = new ConsoleLogger();

        /// <summary>
        /// The debug logger.
        /// </summary>
        private readonly ILogger _debugLogger = new DebugLogger();

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
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LoggingMessageTableEntity messageEntity)
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
