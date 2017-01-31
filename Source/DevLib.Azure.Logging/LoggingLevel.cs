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
        /// Represents debug message level. Very detailed logs, which may include high-volume information such as protocol payloads. This log level is typically only enabled during development.
        /// </summary>
        DBUG = 0,

        /// <summary>
        /// Represents information message level. Which are normally enabled in production environment.
        /// </summary>
        INFO = 1,

        /// <summary>
        /// Represents warning message level. Typically for non-critical issues, which can be recovered or which are temporary failures.
        /// </summary>
        WARN = 2,

        /// <summary>
        /// Represents exception message level.
        /// </summary>
        EXCP = 3,

        /// <summary>
        /// Represents error message level.
        /// </summary>
        ERRO = 4,

        /// <summary>
        /// Represents fatal or critical message level.
        /// </summary>
        FAIL = 5,
    }
}
