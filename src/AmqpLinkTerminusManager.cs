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
        /// <param name="isRemoteSettings">True if the link setting is sent by a remote peer as part of an <see cref="Attach"/> frame to create a new link.</param>
        public static bool IsRecoverableLink(AmqpLinkSettings linkSettings, bool isRemoteSettings)
        {
            if (linkSettings != null)
            {
                if ((linkSettings.IsReceiver() && !isRemoteSettings) || (!linkSettings.IsReceiver() && isRemoteSettings))
                {
                    Target target = linkSettings.Target as Target;
                    return target != null && IsValidTerminusExpirationPolicy(target.ExpiryPolicy);
                }
                else
                {
                    Source source = linkSettings.Source as Source;
                    return source != null && IsValidTerminusExpirationPolicy(source.ExpiryPolicy);
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
        /// Try to add the link terminus if it not exist yet.
        /// Also try to remove it from the expiring link termini map, since adding it means it is active again, so it should no long expire.
        /// Return true if the key/value was added successfully, or false otherwise.
        /// </summary>
        internal bool TryRegisterLinkTerminus(AmqpLinkTerminus key, AmqpLink value)
        {
            Fx.Assert(key != null && value != null, "Should not be adding a null link terminus.");
            lock (this.linkTerminiLock)
            {
                if (this.linkTermini.TryGetValue(key, out AmqpLink existingLink))
                {
                    if (existingLink.State == AmqpObjectState.End || existingLink.State == AmqpObjectState.Faulted)
                    {
                        // The record of the closed link should be replaced with this new record instead.
                        this.linkTermini.Remove(key);
                    }
                    else
                    {
                        // The existing link is still active, we should not override it.
                        return false;
                    }
                }

                this.linkTermini.Add(key, value);
                this.suspendedLinkTermini.Remove(key);
                return true;
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
        /// Remove all link terminus under the given <see cref="AmqpConnection"/>. Should be called upon connection close.
        /// </summary>
        internal void ExpireConnection(AmqpConnection connection)
        {
            IList<AmqpLinkTerminus> linkTerminiToExpire = new List<AmqpLinkTerminus>();

            lock (this.linkTerminiLock)
            {
                foreach (KeyValuePair<AmqpLinkTerminus, AmqpLink> entry in this.linkTermini)
                {
                    if (entry.Value.Session.Connection == connection && entry.Value.TerminusExpiryPolicy.Equals(AmqpConstants.TerminusExpirationPolicy.ConnectionClose))
                    {
                        linkTerminiToExpire.Add(entry.Key);
                        if (!this.suspendedLinkTermini.ContainsKey(entry.Key))
                        {
                            this.suspendedLinkTermini.Add(entry.Key, entry.Value);
                        }
                    }
                }
            }

            this.ScheduleExpiringLinkEndpoints(linkTerminiToExpire);
        }

        /// <summary>
        /// Remove all link terminus under the given <see cref="AmqpSession"/>. Should be called upon session end.
        /// </summary>
        internal void ExpireSession(AmqpSession session)
        {
            IList<AmqpLinkTerminus> linkTerminiToExpire = new List<AmqpLinkTerminus>();

            lock (this.linkTerminiLock)
            {
                foreach (KeyValuePair<AmqpLinkTerminus, AmqpLink> entry in this.linkTermini)
                {
                    if (entry.Value.Session == session && entry.Value.TerminusExpiryPolicy.Equals(AmqpConstants.TerminusExpirationPolicy.SessionEnd))
                    {
                        linkTerminiToExpire.Add(entry.Key);
                        if (!this.suspendedLinkTermini.ContainsKey(entry.Key))
                        {
                            this.suspendedLinkTermini.Add(entry.Key, entry.Value);
                        }
                    }
                }
            }

            this.ScheduleExpiringLinkEndpoints(linkTerminiToExpire);
        }

        /// <summary>
        /// Expire the link terminus for the given <see cref="AmqpLink"/>. Should be called upon link detach.
        /// </summary>
        internal void ExpireLink(AmqpLink link)
        {
            //Fx.Assert(link.Terminus != null, "Cannot expire a null link terminus");
            lock (this.linkTerminiLock)
            {
                if (!this.suspendedLinkTermini.ContainsKey(link.Terminus))
                {
                    this.suspendedLinkTermini.Add(link.Terminus, link);
                }
            }

            IList<AmqpLinkTerminus> linkTerminiToExpire = new List<AmqpLinkTerminus>(1) { link.Terminus };
            this.ScheduleExpiringLinkEndpoints(linkTerminiToExpire);
        }

        void ScheduleExpiringLinkEndpoints(IList<AmqpLinkTerminus> linkTerminiToExpire)
        {
            if (linkTerminiToExpire.Count > 0)
            {
                if (this.ExpiryTimeout > TimeSpan.Zero)
                {
                    Timer expireLinkEndpointsTimer = new Timer();
                    expireLinkEndpointsTimer.Elapsed += (s, e) => this.OnExpireLinkEnpoints(s, e, linkTerminiToExpire);
                    expireLinkEndpointsTimer.Interval = this.ExpiryTimeout.TotalMilliseconds;
                    expireLinkEndpointsTimer.AutoReset = false;
                    expireLinkEndpointsTimer.Enabled = true;
                }
                else
                {
                    this.ExpireLinkTermini(linkTerminiToExpire);
                }
            }
        }

        void ExpireLinkTermini(IList<AmqpLinkTerminus> linkTerminiToExpire)
        {
            try
            {
                lock (this.linkTerminiLock)
                {
                    foreach (AmqpLinkTerminus linkTerminusToExpire in linkTerminiToExpire)
                    {
                        if (this.suspendedLinkTermini.ContainsKey(linkTerminusToExpire))
                        {
                            // this link terminus is still marked for expiration, so we can remove it safely here.
                            this.linkTermini.Remove(linkTerminusToExpire);
                            this.suspendedLinkTermini.Remove(linkTerminusToExpire);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                AmqpTrace.Provider.AmqpLogError(nameof(AmqpLinkTerminusManager), nameof(OnExpireLinkEnpoints), ex);
            }
        }

        void OnExpireLinkEnpoints(object sender, ElapsedEventArgs e, IList<AmqpLinkTerminus> linkTerminiToExpire)
        {
            using ((Timer)sender)
            {
                this.ExpireLinkTermini(linkTerminiToExpire);
            }
        }
    }
}
