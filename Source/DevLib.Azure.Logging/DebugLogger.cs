//-----------------------------------------------------------------------
// <copyright file="DebugLogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Class DebugLogger.
    /// </summary>
    public class DebugLogger : LoggerBase, ILogger
    {
        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LogLevel level, LogMessageTableEntity messageEntity)
        {
            try
            {
                if (level == LogLevel.EXP || level == LogLevel.ERR || level == LogLevel.FAL)
                {
                    Debug.Fail(messageEntity.ToString());
                }

                Debug.WriteLine(messageEntity.ToString());
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }
    }
}
