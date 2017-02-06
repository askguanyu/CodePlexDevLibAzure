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
        private static readonly Dictionary<string, ConsoleColor> ConsoleColorDictionary;

        /// <summary>
        /// Initializes static members of the <see cref="ConsoleLogger"/> class.
        /// </summary>
        static ConsoleLogger()
        {
            ConsoleColorDictionary = new Dictionary<string, ConsoleColor>(7, StringComparer.OrdinalIgnoreCase);

            ConsoleColorDictionary.Add("ALL", ConsoleColor.White);
            ConsoleColorDictionary.Add("DBG", ConsoleColor.DarkCyan);
            ConsoleColorDictionary.Add("INF", ConsoleColor.Cyan);
            ConsoleColorDictionary.Add("EXP", ConsoleColor.DarkYellow);
            ConsoleColorDictionary.Add("WRN", ConsoleColor.Yellow);
            ConsoleColorDictionary.Add("ERR", ConsoleColor.Red);
            ConsoleColorDictionary.Add("FAL", ConsoleColor.Magenta);
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        protected override void InternalLog(LogMessageTableEntity messageEntity)
        {
            try
            {
                lock (ConsoleSyncRoot)
                {
                    ConsoleColor originalForeColor = Console.ForegroundColor;
                    ConsoleColor color = Console.ForegroundColor;
                    ConsoleColorDictionary.TryGetValue(messageEntity.Level, out color);
                    Console.ForegroundColor = color;
                    Console.WriteLine(messageEntity.ToString());
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
