// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Amqp.Encoding;
using Microsoft.Azure.Amqp.Framing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Timers;

namespace Microsoft.Azure.Amqp
{
    /// <summary>
    /// This class is used to manage the link terminus objects within a container.
    /// It should be responsible for things like ensuring uniqueness of link endpoints and managing link expiration policies.
    /// </summary>
    public class AmqpLinkTerminusManager
    {
        static readonly TimeSpan DefaultExpiryTimeout = TimeSpan.Zero;
        object linkTerminiLock;
        IDictionary<AmqpLinkIdentifier, AmqpLinkTerminus> linkTermini;

        /// <summary>
        /// Create a new instance of <see cref="AmqpLinkTerminusManager"/>.
        /// </summary>
        public AmqpLinkTerminusManager()
        {
            this.linkTerminiLock = new object();
            this.linkTermini = new Dictionary<AmqpLinkIdentifier, AmqpLinkTerminus>();
        }

        /// <summary>
        /// The default expiration policy which will be applied to all the created links tracked by this class.
        /// </summary>
        public LinkTerminusExpirationPolicy ExpirationPolicy { get; set; } = LinkTerminusExpirationPolicy.NONE;

        /// <summary>
        /// The default duration that the link endpoint should be kept for after the expiration countdown begins.
        /// </summary>
        public TimeSpan ExpiryTimeout { get; set; } = DefaultExpiryTimeout;

        /// <summary>
        /// Returns the corresponding <see cref="AmqpSymbol"/> for the given link terminus expiration policy.
        /// </summary>
        public static AmqpSymbol GetExpirationPolicySymbol(LinkTerminusExpirationPolicy linkTerminusExpirationPolicy)
        {
            switch (linkTerminusExpirationPolicy)
            {
                case LinkTerminusExpirationPolicy.LINK_DETACH:
                    return TerminusExpiryPolicy.LinkDetach;
                case LinkTerminusExpirationPolicy.SESSION_END:
                    return TerminusExpiryPolicy.SessionEnd;
                case LinkTerminusExpirationPolicy.CONNECTION_CLOSE:
                    return TerminusExpiryPolicy.ConnectionClose;
                case LinkTerminusExpirationPolicy.NEVER:
                    return TerminusExpiryPolicy.Never;
                default:
                    return new AmqpSymbol(null);
            }
        }

        /// <summary>
        /// Check if the given <see cref="AmqpLinkSettings"/> is for a recoverable link, or if a new recoverable link instance should be created out of the given settings.
        /// </summary>
        /// <param name="linkSettings">The link settings to be checked.</param>
        public static bool IsRecoverableLink(AmqpLinkSettings linkSettings)
        {
            AmqpSymbol expiryPolicy = linkSettings.ExpiryPolicy();
            return IsValidTerminusExpirationPolicy(expiryPolicy);
        }

        /// <summary>
        /// Checks if the given <see cref="AmqpSymbol"/> is a valid link terminus expiration policy or not.
        /// </summary>
        public static bool IsValidTerminusExpirationPolicy(AmqpSymbol symbol)
        {
            return symbol.Equals(TerminusExpiryPolicy.LinkDetach) ||
                symbol.Equals(TerminusExpiryPolicy.SessionEnd) ||
                symbol.Equals(TerminusExpiryPolicy.ConnectionClose) ||
                symbol.Equals(TerminusExpiryPolicy.Never);
        }

        /// <summary>
        /// Try to get the link terminus with the given link identifier. Return true if the link terminus has been found.
        /// </summary>
        /// <param name="linkIdentifier">The unique identifier of a link endpoint which will be used as a key to identify the link terminus.</param>
        /// <param name="linkTerminus">The link terminus object associated with the link identifier.</param>
        public bool TryGetLinkTerminus(AmqpLinkIdentifier linkIdentifier, out AmqpLinkTerminus linkTerminus)
        {
            lock (this.linkTerminiLock)
            {
                return this.linkTermini.TryGetValue(linkIdentifier, out linkTerminus);
            }
        }

        /// <summary>
        /// Try to add a link terminus object which will be associated with the link identifier.
        /// Returns true if it was successfully added, or false if there is already an existing entry and the value is not added.
        /// </summary>
        /// <param name="linkIdentifier">The unique identifier of a link endpoint which will be used as a key to identify the link terminus.</param>
        /// <param name="linkTerminus">The link terminus object to be added.</param>
        public bool TryAddLinkTerminus(AmqpLinkIdentifier linkIdentifier, AmqpLinkTerminus linkTerminus)
        {
            lock (this.linkTerminiLock)
            {
                if (this.linkTermini.ContainsKey(linkIdentifier))
                {
                    return false;
                }

                this.linkTermini.Add(linkIdentifier, linkTerminus);
                return true;
            }
        }

        /// <summary>
        /// Add the link terminus to be tracked by this manager. Also associate the given link with the given link terminus.
        /// </summary>
        internal void RegisterLink(AmqpLink link)
        {
            Fx.Assert(link != null, "Should not be registering a null link.");
            AmqpLinkTerminus linkTerminus = null;
            AmqpLink stolenLink = null;
            lock (this.linkTerminiLock)
            {
                if (this.linkTermini.TryGetValue(link.Settings.LinkIdentifier, out linkTerminus))
                {
                    this.linkTermini.Remove(link.Settings.LinkIdentifier);
                    stolenLink = linkTerminus.Link;
                }
                else
                {
                    linkTerminus = new AmqpLinkTerminus(link.Settings.LinkIdentifier, link.UnsettledMap);
                }

                linkTerminus.AssociateLink(link);
                this.linkTermini.Add(link.Settings.LinkIdentifier, linkTerminus);
            }

            if (stolenLink != null && stolenLink != link)
            {
                stolenLink.OnLinkStolen();
            }

            linkTerminus.Suspended += this.OnSuspendLinkTerminus;
            linkTerminus.Expired += this.OnExpireLinkTerminus;

            // suspend the link terminus when the corresponding AmqpObject is closed.
            EventHandler onSuspendTerminus = null;
            onSuspendTerminus = (sender, e) =>
            {
                linkTerminus.OnSuspend();
                ((AmqpObject)sender).Closed -= onSuspendTerminus;
            };

            AmqpSymbol expiryPolicy = link.Settings.ExpiryPolicy();
            if (expiryPolicy.Equals(TerminusExpiryPolicy.LinkDetach) || expiryPolicy.Value == null)
            {
                link.Closed += onSuspendTerminus;
            }
            else if (expiryPolicy.Equals(TerminusExpiryPolicy.SessionEnd))
            {
                link.Session.Closed += onSuspendTerminus;
            }
            else if (expiryPolicy.Equals(TerminusExpiryPolicy.ConnectionClose))
            {
                link.Session.Connection.Closed += onSuspendTerminus;
            }
        }

        /// <summary>
        /// Expire the link terminus for the given <see cref="AmqpLink"/>. Should be called when the ExpiryPolicy of the given link is triggered.
        /// </summary>
        internal void OnSuspendLinkTerminus(object sender, EventArgs e)
        {
            AmqpLinkTerminus linkTerminus = (AmqpLinkTerminus)sender;
            linkTerminus.Suspended -= this.OnSuspendLinkTerminus;
            if (linkTerminus.Link.Settings.Timeout() > TimeSpan.Zero)
            {
                Timer expireLinkEndpointsTimer = new Timer();
                expireLinkEndpointsTimer.Elapsed += (s, e) => this.OnExpireLinkTerminusTimer(s, e, linkTerminus);
                expireLinkEndpointsTimer.Interval = this.ExpiryTimeout.TotalMilliseconds;
                expireLinkEndpointsTimer.AutoReset = false;
                expireLinkEndpointsTimer.Enabled = true;
            }
            else
            {
                linkTerminus.OnExpire();
            }
        }

        void OnExpireLinkTerminus(object sender, EventArgs e)
        {
            AmqpLinkTerminus linkTerminus = (AmqpLinkTerminus)sender;
            linkTerminus.Expired -= this.OnExpireLinkTerminus;
            lock (this.linkTerminiLock)
            {
                this.linkTermini.Remove(linkTerminus.Identifier);
            }
        }

        void OnExpireLinkTerminusTimer(object sender, ElapsedEventArgs e, AmqpLinkTerminus linkTerminusToExpire)
        {
            linkTerminusToExpire.OnExpire();
            Timer timer = (Timer)sender;
            timer.Dispose();
        }
    }
}
