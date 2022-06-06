namespace Microsoft.Azure.Amqp
{
    using Microsoft.Azure.Amqp.Encoding;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// A class which represents a link endpoint, which contains information about the link's settings and unsettled map.
    /// This class should be treated as a "snapshot" of the link's current state and may be used for link recovery.
    /// </summary>
    public class AmqpLinkTerminus
    {
        object thisLock = new object();
        AmqpLink link;
        bool suspended;

        /// <summary>
        /// Create a new instance of a link terminus object.
        /// </summary>
        /// <param name="identifier">The identifier that is used to uniquely identify the link endpoint.</param>
        /// <param name="unsettledMap">The unsettled deliveries for this link terminus object.</param>
        public AmqpLinkTerminus(AmqpLinkIdentifier identifier, IDictionary<ArraySegment<byte>, Delivery> unsettledMap)
        {
            if (identifier == null)
            {
                throw new ArgumentNullException(nameof(identifier));
            }

            this.Identifier = identifier;
            this.UnsettledMap = unsettledMap;
        }

        /// <summary>
        /// This event will be fired when the terminus is suspended from the corresponding ExpiryPolicy.
        /// Please note that this event will not fire if the terminus is already in a suspended state.
        /// </summary>
        public event EventHandler Suspended;

        /// <summary>
        /// This event will be fired when the terminus is expired from the corresponding ExpiryPolicy after the corresponding ExiryTimeout.
        /// Please note that this event will not fire if the terminus is no longer suspended.
        /// </summary>
        public event EventHandler Expired;

        /// <summary>
        /// Returns the link that's currently associated with the link terminus. 
        /// Please keep in mind that this link may not be the most recent link associated with the terminus, as it could have been stolen.
        /// </summary>
        public AmqpLink Link
        {
            get
            {
                lock (this.thisLock)
                {
                    return this.link;
                }
            }
        }

        /// <summary>
        /// Returns the identifier used to uniquely identify this terminus object.
        /// </summary>
        public AmqpLinkIdentifier Identifier { get; }

        /// <summary>
        /// Returns the map of unsettled deliveries for this link terminus, where the key is the deliveryID and the value is the Delivery.
        /// </summary>
        public IDictionary<ArraySegment<byte>, Delivery> UnsettledMap { get; private set; }

        /// <summary>
        /// Check if this link terminus object is equal to the other object provided.
        /// </summary>
        /// <param name="other">The other object to check equality against.</param>
        /// <returns>True if the other object is a link terminus object that should be treated as equal to this link terminus object. Please note that they do not have to be the same object reference to be equal.</returns>
        public override bool Equals(object other)
        {
            AmqpLinkTerminus otherLinkTerminus = other as AmqpLinkTerminus;
            return otherLinkTerminus != null && this.Identifier.Equals(otherLinkTerminus.Identifier);
        }

        /// <summary>
        /// Get hash code for this link terminus object.
        /// </summary>
        public override int GetHashCode()
        {
            return this.Identifier.GetHashCode();
        }

        /// <summary>
        /// Associate a given link with this link terminus. 
        /// This whould close the existing link for link stealing before registering this new link if the existing link is a different link.
        /// </summary>
        internal void AssociateLink(AmqpLink link)
        {
            lock (this.thisLock)
            {
                this.link = link;
                this.suspended = false;
            }

            link.Terminus = this;
            if (this.UnsettledMap != null)
            {
                link.Settings.Unsettled = new AmqpMap(
                    this.UnsettledMap.ToDictionary(
                        kvPair => kvPair.Key,
                        kvPair => kvPair.Value.State),
                    MapKeyByteArrayComparer.Instance);
            }
        }

        internal void OnSuspend()
        {
            bool alreadySuspended = false;
            lock (this.thisLock)
            {
                alreadySuspended = this.suspended;
                this.suspended = true;
                this.UnsettledMap = this.link.UnsettledMap;
            }

            if (!alreadySuspended)
            {
                this.Suspended?.Invoke(this, EventArgs.Empty);
            }
        }

        /// <summary>
        /// Returns true if the 
        /// </summary>
        /// <returns></returns>
        internal bool OnExpire()
        {
            bool stillSuspended;
            lock (this.thisLock)
            {
                stillSuspended = this.suspended;
            }

            if (stillSuspended)
            {
                this.Expired?.Invoke(this, EventArgs.Empty);
                return true;
            }

            return false;
        }
    }
}
