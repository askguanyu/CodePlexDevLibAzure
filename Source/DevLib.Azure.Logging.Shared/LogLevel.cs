//-----------------------------------------------------------------------
// <copyright file="LogLevel.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    /// <summary>
    /// Message log level.
    /// </summary>
    public enum LogLevel
    {
        /// <summary>
        /// Represents turn off message.
        /// </summary>
        OFF = 0,

        /// <summary>
        /// Represents turn on message and log all levels messages.
        /// </summary>
        ALL = 1,

        /// <summary>
        /// Represents debug message level. Very detailed logs, which may include high-volume information such as protocol payloads. This log level is typically only enabled during development.
        /// </summary>
        DBG = 2,

        /// <summary>
        /// Represents information message level. Which are normally enabled in production environment.
        /// </summary>
        INF = 3,

        /// <summary>
        /// Represents warning message level. Typically for non-critical issues, which can be recovered or which are temporary failures.
        /// </summary>
        WRN = 4,

        /// <summary>
        /// Represents exception message level.
        /// </summary>
        EXP = 5,

        /// <summary>
        /// Represents error message level.
        /// </summary>
        ERR = 6,

        /// <summary>
        /// Represents fatal or critical message level.
        /// </summary>
        FAL = 7,
    }
}
