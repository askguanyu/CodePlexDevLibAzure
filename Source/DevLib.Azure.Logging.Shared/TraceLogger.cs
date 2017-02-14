//-----------------------------------------------------------------------
// <copyright file="TraceLogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Class TraceLogger.
    /// </summary>
    public class TraceLogger : LoggerBase, ILogger
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TraceLogger"/> class.
        /// </summary>
        public TraceLogger()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TraceLogger"/> class.
        /// </summary>
        /// <param name="listeners">The listeners.</param>
        public TraceLogger(params TraceListener[] listeners)
        {
            Trace.Listeners.AddRange(listeners);
        }

        /// <summary>
        /// Adds the trace listeners.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>The current TraceLogger instance.</returns>
        public TraceLogger AddListeners(params TraceListener[] listeners)
        {
            Trace.Listeners.AddRange(listeners);

            return this;
        }

        /// <summary>
        /// Removes the trace listeners.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>The current TraceLogger instance.</returns>
        public TraceLogger RemoveListeners(params TraceListener[] listeners)
        {
            if (listeners != null)
            {
                foreach (var listener in listeners)
                {
                    Trace.Listeners.Remove(listener);
                }
            }

            return this;
        }

        /// <summary>
        /// Removes the trace listeners.
        /// </summary>
        /// <param name="listeners">The trace listeners.</param>
        /// <returns>The current TraceLogger instance.</returns>
        public TraceLogger RemoveListeners(params string[] listeners)
        {
            if (listeners != null)
            {
                foreach (var listener in listeners)
                {
                    Trace.Listeners.Remove(listener);
                }
            }

            return this;
        }

        /// <summary>
        /// Removes all trace listeners.
        /// </summary>
        /// <returns>The current TraceLogger instance.</returns>
        public TraceLogger ClearListeners()
        {
            Trace.Listeners.Clear();

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
                if (level == LogLevel.ALL || level == LogLevel.DBG || level == LogLevel.INF)
                {
                    Trace.TraceInformation(messageEntity.ToString());
                }
                else if (level == LogLevel.WRN)
                {
                    Trace.TraceWarning(messageEntity.ToString());
                }
                else if (level == LogLevel.EXP || level == LogLevel.ERR)
                {
                    Trace.TraceError(messageEntity.ToString());
                }
                else if (level == LogLevel.FAL)
                {
                    Trace.TraceError(messageEntity.ToString());
                    Trace.Fail(messageEntity.ToString());
                }
                else
                {
                    Trace.WriteLine(messageEntity.ToString());
                }
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }
    }
}
