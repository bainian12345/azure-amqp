namespace Microsoft.Azure.Amqp
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A class which represents a link endpoint, which contains information about the link's settings and unsettled map.
    /// This class should be treated as a "snapshot" of the link's current state and may be used for link recovery.
    /// </summary>
    public class AmqpLinkTerminus
    {
        /// <summary>
        /// Create a new instance of a link terminus object.
        /// </summary>
        /// <param name="settings">The <see cref="AmqpLinkSettings"/> that is associated with this link terminus object, which is used to uniquely identify it.</param>
        /// <param name="unsettledMap">The unsettled deliveries for this link terminus object.</param>
        public AmqpLinkTerminus(AmqpLinkSettings settings, IDictionary<ArraySegment<byte>, Delivery> unsettledMap)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            this.Settings = settings;
            this.UnsettledMap = unsettledMap;
        }

        /// <summary>
        /// This event will be fired when the terminus is expired from the ExpiryPolicy.
        /// </summary>
        public event EventHandler Expired;

        /// <summary>
        /// Returns the <see cref="AmqpLinkSettings"/> that is associated with this link terminus object.
        /// Please note that this link settings object is used to uniquely identify this link terminus.
        /// </summary>
        public AmqpLinkSettings Settings { get; }

        /// <summary>
        /// Returns the map of unsettled deliveries for this link terminus, where the key is the deliveryID and the value is the Delivery.
        /// </summary>
        public IDictionary<ArraySegment<byte>, Delivery> UnsettledMap { get; }

        /// <summary>
        /// Check if this link terminus object is equal to the other object provided.
        /// </summary>
        /// <param name="other">The other object to check equality against.</param>
        /// <returns>True if the other object is a link terminus object that should be treated as equal to this link terminus object. Please note that they do not have to be the same object reference to be equal.</returns>
        public override bool Equals(object other)
        {
            AmqpLinkTerminus otherLinkTerminus = other as AmqpLinkTerminus;
            return otherLinkTerminus != null && this.Settings.Equals(otherLinkTerminus.Settings);
        }

        /// <summary>
        /// Get hash code for this link terminus object.
        /// </summary>
        public override int GetHashCode()
        {
            return this.Settings.GetHashCode();
        }

        internal void OnExpired()
        {
            this.Expired?.Invoke(this, EventArgs.Empty);
        }
    }
}
