//-----------------------------------------------------------------------
// <copyright file="LoggingLevel.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    /// <summary>
    /// Message logging level.
    /// </summary>
    public enum LoggingLevel
    {
        /// <summary>
        /// Represents turn off message.
        /// </summary>
        OFF = 0,

        /// <summary>
        /// Represents debug message level. Very detailed logs, which may include high-volume information such as protocol payloads. This log level is typically only enabled during development.
        /// </summary>
        DBG = 1,

        /// <summary>
        /// Represents information message level. Which are normally enabled in production environment.
        /// </summary>
        INF = 2,

        /// <summary>
        /// Represents warning message level. Typically for non-critical issues, which can be recovered or which are temporary failures.
        /// </summary>
        WRN = 3,

        /// <summary>
        /// Represents exception message level.
        /// </summary>
        EXP = 4,

        /// <summary>
        /// Represents error message level.
        /// </summary>
        ERR = 5,

        /// <summary>
        /// Represents fatal or critical message level.
        /// </summary>
        FAL = 6,
    }
}
