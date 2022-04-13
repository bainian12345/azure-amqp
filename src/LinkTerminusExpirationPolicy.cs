﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Amqp
{
    /// <summary>
    /// Determines when the link terminus object should be deleted and forgotten.
    /// </summary>
    public enum LinkTerminusExpirationPolicy
    {
        /// <summary>
        /// Do not save any link terminus information.
        /// </summary>
        NONE,

        /// <summary>
        /// Delete and forget the link terminus when the <see cref="AmqpLink"/> is detached.
        /// </summary>
        LINK_DETACH,

        /// <summary>
        /// Delete and forget the link terminus when the <see cref="AmqpSession"/> is ended.
        /// </summary>
        SESSION_END,

        /// <summary>
        /// Delete and forget the link terminus when the <see cref="AmqpConnection"/> is closed.
        /// </summary>
        CONNECTION_CLOSE,

        /// <summary>
        /// Keep the link terminus object for as long as possible.
        /// </summary>
        NEVER
    }
}
