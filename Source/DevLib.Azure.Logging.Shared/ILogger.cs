//-----------------------------------------------------------------------
// <copyright file="ILogger.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Logging
{
    /// <summary>
    /// Interface ILogger.
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        /// Gets or sets the level criteria.
        /// </summary>
        LogLevel LogLevelCriteria { get; set; }

        /// <summary>
        /// Logs the specified message entity.
        /// </summary>
        /// <param name="messageEntity">The message entity.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger Log(LogMessageTableEntity messageEntity);

        /// <summary>
        /// Logs the message with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger Log(LogLevel level, object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the messages with specified level.
        /// </summary>
        /// <param name="level">The logging level.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger Log(LogLevel level, object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger Log(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the messages.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger Log(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogDebug(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the debug.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogDebug(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogInfo(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the information.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogInfo(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogWarning(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the warning.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogWarning(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogException(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogException(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogError(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the error.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogError(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogFatal(object message, string applicationName = null, string eventId = null, string instanceId = null);

        /// <summary>
        /// Logs the fatal.
        /// </summary>
        /// <param name="messages">The messages.</param>
        /// <param name="applicationName">Name of the application.</param>
        /// <param name="eventId">The event identifier.</param>
        /// <param name="instanceId">The instance identifier.</param>
        /// <returns>The current ILogger instance.</returns>
        ILogger LogFatal(object[] messages, string applicationName = null, string eventId = null, string instanceId = null);
    }
}
