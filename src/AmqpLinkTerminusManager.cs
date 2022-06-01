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
        Dictionary<AmqpLinkTerminus, AmqpLink> linkTermini;
        Dictionary<AmqpLinkTerminus, AmqpLink> suspendedLinkTermini;

        /// <summary>
        /// Create a new instance of <see cref="AmqpLinkTerminusManager"/>.
        /// </summary>
        public AmqpLinkTerminusManager()
        {
            this.linkTerminiLock = new object();
            this.linkTermini = new Dictionary<AmqpLinkTerminus, AmqpLink>();
            this.suspendedLinkTermini = new Dictionary<AmqpLinkTerminus, AmqpLink>();
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
            return symbol.Equals(AmqpConstants.TerminusExpirationPolicy.LinkDetach) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.SessionEnd) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.ConnectionClose) ||
                symbol.Equals(AmqpConstants.TerminusExpirationPolicy.Never);
        }

        /// <summary>
        /// Try to get the value under the given key. Return true and the value if it was found.
        /// </summary>
        internal bool TryGetValue(AmqpLinkTerminus key, out AmqpLink value)
        {
            lock (this.linkTerminiLock)
            {
                return this.linkTermini.TryGetValue(key, out value);
            }
        }

        /// <summary>
        /// Add the link terminus to be tracked. If there is already an existing link associated with this link terminus, close the existing link due to link stealing.
        /// Also try to remove it from the expiring link termini map, since adding it means makes it active again, it should no long expire.
        /// </summary>
        internal void RegisterLinkTerminus(AmqpLinkTerminus key, AmqpLink value)
        {
            Fx.Assert(key != null && value != null, "Should not be adding a null link terminus.");
            lock (this.linkTerminiLock)
            {
                this.TryStealLink(key, value);
                if (!this.linkTermini.ContainsKey(key))
                {
                    this.linkTermini.Add(key, value);
                }

                this.suspendedLinkTermini.Remove(key);
            }
        }

        /// <summary>
        /// Remove the entry with the given key and output the corresponding value if available. Return true if the value was removed, or false if the key was not found and nothing was removed.
        /// </summary>
        internal bool TryRemove(AmqpLinkTerminus key, out AmqpLink value)
        {
            Fx.Assert(key != null, "Should not be removing a null link terminus.");
            lock (this.linkTerminiLock)
            {
                if (this.linkTermini.TryGetValue(key, out value))
                {
                    this.linkTermini.Remove(key);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// If this link terminus manager already has a link associated with the given link terminus, and the associated 
        /// link is different than the given link, then close the associated link due to link stealing.
        /// Return true if there is an existing link closed due to link stealing, return false otherwise.
        /// </summary>
        internal bool TryStealLink(AmqpLinkTerminus linkTerminus, AmqpLink link)
        {
            AmqpLink existingLink = null;
            bool shouldStealLink = true;
            lock (this.linkTerminiLock)
            {
                shouldStealLink = this.linkTermini.TryGetValue(linkTerminus, out existingLink) && existingLink != link; 
                if (shouldStealLink)
                {
                    this.linkTermini.Remove(linkTerminus);
                }
            }

            if (shouldStealLink)
            {
                existingLink.OnLinkStolen();
            }

            return shouldStealLink;
        }

        /// <summary>
        /// Expire the link terminus for the given <see cref="AmqpLink"/>. Should be called when the ExpiryPolicy of the given link is triggered.
        /// </summary>
        internal void SuspendLink(AmqpLink link)
        {
            Fx.Assert(link?.Terminus != null, "Cannot expire a null link terminus");

            bool alreadySuspended = true;
            lock (this.linkTerminiLock)
            {
                if (!this.suspendedLinkTermini.ContainsKey(link.Terminus))
                {
                    alreadySuspended = false;
                    this.suspendedLinkTermini.Add(link.Terminus, link);
                }
            }

            if (!alreadySuspended)
            {
                if (this.ExpiryTimeout > TimeSpan.Zero)
                {
                    Timer expireLinkEndpointsTimer = new Timer();
                    expireLinkEndpointsTimer.Elapsed += (s, e) => this.OnExpireLinkTerminusTimer(s, e, link.Terminus);
                    expireLinkEndpointsTimer.Interval = this.ExpiryTimeout.TotalMilliseconds;
                    expireLinkEndpointsTimer.AutoReset = false;
                    expireLinkEndpointsTimer.Enabled = true;
                }
                else
                {
                    // if expiry timeout is zero, do the removals immediately to avoid creating timers.
                    this.ExpireLinkTerminus(link.Terminus);
                }
            }
        }

        void ExpireLinkTerminus(AmqpLinkTerminus linkTerminusToExpire)
        {
            Fx.Assert(linkTerminusToExpire != null, "The link terminus to be expired should not be null.");
            try
            {
                lock (this.linkTerminiLock)
                {
                    if (this.suspendedLinkTermini.ContainsKey(linkTerminusToExpire))
                    {
                        // this link terminus is still marked for expiration, so we can remove it safely here.
                        this.linkTermini.Remove(linkTerminusToExpire);
                        this.suspendedLinkTermini.Remove(linkTerminusToExpire);
                    }
                }

                linkTerminusToExpire.OnExpired();
            }
            catch (Exception ex) when (!Fx.IsFatal(ex))
            {
                AmqpTrace.Provider.AmqpLogError(nameof(AmqpLinkTerminusManager), nameof(ExpireLinkTerminus), ex);
                throw;
            }
        }

        void OnExpireLinkTerminusTimer(object sender, ElapsedEventArgs e, AmqpLinkTerminus linkTerminusToExpire)
        {
            using ((Timer)sender)
            {
                this.ExpireLinkTerminus(linkTerminusToExpire);
            }
        }
    }
}
