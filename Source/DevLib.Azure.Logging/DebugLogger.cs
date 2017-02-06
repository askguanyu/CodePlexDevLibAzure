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
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LogMessageTableEntity messageEntity)
        {
            try
            {
                Debug.WriteLine(messageEntity.ToString());
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }
    }
}
