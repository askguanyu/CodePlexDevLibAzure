//-----------------------------------------------------------------------
// <copyright file="ConsoleLogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Class ConsoleLogger.
    /// </summary>
    public class ConsoleLogger : LoggerBase, ILogger
    {
        /// <summary>
        /// Field ConsoleSyncRoot.
        /// </summary>
        private static readonly object ConsoleSyncRoot = new object();

        /// <summary>
        /// Field ConsoleColorDictionary.
        /// </summary>
        private static readonly Dictionary<LogLevel, ConsoleColor> ConsoleColorDictionary;

        /// <summary>
        /// Initializes static members of the <see cref="ConsoleLogger"/> class.
        /// </summary>
        static ConsoleLogger()
        {
            ConsoleColorDictionary = new Dictionary<LogLevel, ConsoleColor>(8);

            ConsoleColorDictionary.Add(LogLevel.OFF, ConsoleColor.Gray);
            ConsoleColorDictionary.Add(LogLevel.ALL, ConsoleColor.White);
            ConsoleColorDictionary.Add(LogLevel.DBG, ConsoleColor.DarkCyan);
            ConsoleColorDictionary.Add(LogLevel.INF, ConsoleColor.Cyan);
            ConsoleColorDictionary.Add(LogLevel.EXP, ConsoleColor.DarkYellow);
            ConsoleColorDictionary.Add(LogLevel.WRN, ConsoleColor.Yellow);
            ConsoleColorDictionary.Add(LogLevel.ERR, ConsoleColor.Red);
            ConsoleColorDictionary.Add(LogLevel.FAL, ConsoleColor.Magenta);
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
                lock (ConsoleSyncRoot)
                {
                    ConsoleColor originalForeColor = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColorDictionary[level];
                    Console.WriteLine(messageEntity.ToString());
                    Console.WriteLine();
                    Console.ForegroundColor = originalForeColor;
                }
            }
            catch (Exception e)
            {
                InternalLogger.Log(e);
            }
        }
    }
}
