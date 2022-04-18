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
        Dictionary<AmqpLinkSettings, AmqpLink> recoverableLinkTermini;
        Dictionary<AmqpLinkSettings, AmqpLink> expiringLinkTermini;

        /// <summary>
        /// Create a new instance of <see cref="AmqpLinkTerminusManager"/>.
        /// </summary>
        public AmqpLinkTerminusManager()
        {
            this.linkTerminiLock = new object();
            this.recoverableLinkTermini = new Dictionary<AmqpLinkSettings, AmqpLink>();
            this.expiringLinkTermini = new Dictionary<AmqpLinkSettings, AmqpLink>();
        }

        /// <summary>
        /// The expiration policy which will be applied to all the created links tracked by this class.
        /// </summary>
        public LinkTerminusExpirationPolicy ExpirationPolicy { get; set; }

        /// <summary>
        /// The duration that the link endpoint should be kept for after the expiration countdown begins.
        /// </summary>
        public TimeSpan ExpiryTimeout { get; set; } = DefaultExpiryTimeout;

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
        internal bool TryGetValue(AmqpLinkSettings key, out AmqpLink value)
        {
            lock (this.linkTerminiLock)
            {
                return this.recoverableLinkTermini.TryGetValue(key, out value);
            }
        }

        /// <summary>
        /// Try to add the link terminus if it not exist yet.
        /// Also try to remove it from the expiring link termini map, since adding it means it is active again, so it should no long expire.
        /// Return true if the key/value was added successfully, or false otherwise.
        /// </summary>
        internal bool TryAdd(AmqpLinkSettings key, AmqpLink value)
        {
            Fx.Assert(key != null && value != null, "Should not be adding a null link terminus.");
            lock (this.linkTerminiLock)
            {
                if (!this.recoverableLinkTermini.ContainsKey(key))
                {
                    this.recoverableLinkTermini.Add(key, value);
                    this.expiringLinkTermini.Remove(key);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Remove the entry with the given key and output the corresponding value if available. Return true if the value was removed, or false if the key was not found and nothing was removed.
        /// </summary>
        internal bool TryRemove(AmqpLinkSettings key, out AmqpLink value)
        {
            Fx.Assert(key != null, "Should not be removing a null link terminus.");
            lock (this.linkTerminiLock)
            {
                if (this.recoverableLinkTermini.TryGetValue(key, out value))
                {
                    this.recoverableLinkTermini.Remove(key);
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
            IList<AmqpLinkSettings> linkTerminiToExpire = new List<AmqpLinkSettings>();

            lock (this.linkTerminiLock)
            {
                foreach (KeyValuePair<AmqpLinkSettings, AmqpLink> entry in this.recoverableLinkTermini)
                {
                    if (entry.Value.Session.Connection == connection && entry.Value.TerminusExpiryPolicy.Equals(AmqpConstants.TerminusExpirationPolicy.ConnectionClose))
                    {
                        linkTerminiToExpire.Add(entry.Key);
                        if (!this.expiringLinkTermini.ContainsKey(entry.Key))
                        {
                            this.expiringLinkTermini.Add(entry.Key, entry.Value);
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
            IList<AmqpLinkSettings> linkTerminiToExpire = new List<AmqpLinkSettings>();

            lock (this.linkTerminiLock)
            {
                foreach (KeyValuePair<AmqpLinkSettings, AmqpLink> entry in this.recoverableLinkTermini)
                {
                    if (entry.Value.Session == session && entry.Value.TerminusExpiryPolicy.Equals(AmqpConstants.TerminusExpirationPolicy.SessionEnd))
                    {
                        linkTerminiToExpire.Add(entry.Key);
                        if (!this.expiringLinkTermini.ContainsKey(entry.Key))
                        {
                            this.expiringLinkTermini.Add(entry.Key, entry.Value);
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
                if (!this.expiringLinkTermini.ContainsKey(link.Settings))
                {
                    this.expiringLinkTermini.Add(link.Settings, link);
                }
            }

            IList<AmqpLinkSettings> linkTerminiToExpire = new List<AmqpLinkSettings>(1) { link.Settings };
            this.ScheduleExpiringLinkEndpoints(linkTerminiToExpire);
        }

        void ScheduleExpiringLinkEndpoints(IList<AmqpLinkSettings> linkTerminiToExpire)
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

        void ExpireLinkTermini(IList<AmqpLinkSettings> linkTerminiToExpire)
        {
            try
            {
                lock (this.linkTerminiLock)
                {
                    foreach (AmqpLinkSettings linkTerminusToExpire in linkTerminiToExpire)
                    {
                        if (this.expiringLinkTermini.ContainsKey(linkTerminusToExpire))
                        {
                            // this link terminus is still marked for expiration, so we can remove it safely here.
                            this.recoverableLinkTermini.Remove(linkTerminusToExpire);
                            this.expiringLinkTermini.Remove(linkTerminusToExpire);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                AmqpTrace.Provider.AmqpLogError(nameof(AmqpLinkTerminusManager), nameof(OnExpireLinkEnpoints), ex);
            }
        }

        void OnExpireLinkEnpoints(object sender, ElapsedEventArgs e, IList<AmqpLinkSettings> linkTerminiToExpire)
        {
            using ((Timer)sender)
            {
                this.ExpireLinkTermini(linkTerminiToExpire);
            }
        }
    }
}
