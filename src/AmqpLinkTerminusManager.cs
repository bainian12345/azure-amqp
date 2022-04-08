// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Amqp.Encoding;
using Microsoft.Azure.Amqp.Framing;
using System.Collections.Concurrent;

namespace Microsoft.Azure.Amqp
{
    /// <summary>
    /// This class is used to manage the link terminus objects within a container.
    /// It should be responsible for things like ensuring uniqueness of link endpoints and managing link expiration policies.
    /// </summary>
    public class AmqpLinkTerminusManager
    {
        /// <summary>
        /// Create a new instance of <see cref="AmqpLinkTerminusManager"/>.
        /// </summary>
        public AmqpLinkTerminusManager()
        {
            this.RecoverableLinkEndpoints = new ConcurrentDictionary<AmqpLinkSettings, AmqpLink>();
        }

        /// <summary>
        /// The expiration policy which will be applied to all the created links tracked by this class.
        /// </summary>
        public LinkTerminusExpirationPolicy ExpirationPolicy { get; set; }

        internal AmqpSymbol ExpirationPolicySymbol
        {
            get
            {
                switch (this.ExpirationPolicy)
                {
                    case LinkTerminusExpirationPolicy.LINK_DETACH:
                        return AmqpConstants.TerminusExpirationPolicy.LinkDetach;
                    case LinkTerminusExpirationPolicy.SESSION_END:
                        return AmqpConstants.TerminusExpirationPolicy.SessionEnd;
                    case LinkTerminusExpirationPolicy.CONNECTION_CLOSE:
                        return AmqpConstants.TerminusExpirationPolicy.ConnectionClose;
                    case LinkTerminusExpirationPolicy.NEVER:
                        return AmqpConstants.TerminusExpirationPolicy.Never;
                    default:
                        return new AmqpSymbol(null);
                }
            }
        }

        internal ConcurrentDictionary<AmqpLinkSettings, AmqpLink> RecoverableLinkEndpoints { get; }

        /// <summary>
        /// Check if the given <see cref="AmqpLinkSettings"/> is for a recoverable link, or if a new recoverable link instance should be created out of the given settings.
        /// </summary>
        /// <param name="linkSettings">The link settings to be checked.</param>
        /// <param name="isRemoteSettings">True if the link setting is sent by a remote peer as part of an <see cref="Attach"/> frame to create a new link.</param>
        public static bool IsRecoverableLink(AmqpLinkSettings linkSettings, bool isRemoteSettings)
        {
            if (linkSettings != null)
            {
                if ((linkSettings.IsReceiver() && !isRemoteSettings) || (!linkSettings.IsReceiver() && isRemoteSettings))
                {
                    // this is may be a recoverable receiver, or may be created as a recoverable reciever, depending on the expiration policy.
                    Target target = linkSettings.Target as Target;
                    return IsValidTerminusExpirationPolicy(target.ExpiryPolicy);
                }
                else
                {
                    // this is may be a recoverable receiver, or may be created as a recoverable reciever, depending on the expiration policy.
                    Source source = linkSettings.Source as Source;
                    return IsValidTerminusExpirationPolicy(source.ExpiryPolicy);
                }
            }

            return false;
        }

        /// <summary>
        /// Checks if the given <see cref="AmqpSymbol"/> is a valid link terminus expiration policy or not.
        /// </summary>
        public static bool IsValidTerminusExpirationPolicy(AmqpSymbol symbol)
        {
            return symbol.Equals(AmqpConstants.TerminusExpirationPolicy.LinkDetach) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.SessionEnd) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.ConnectionClose) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.Never);
        }
    }
}
