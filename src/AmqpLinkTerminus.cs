namespace Microsoft.Azure.Amqp
{
    using Microsoft.Azure.Amqp.Framing;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A class which represents a link endpoint, which may be used for link recovery.
    /// </summary>
    public class AmqpLinkTerminus
    {
        internal static IEqualityComparer<AmqpLinkTerminus> Comparer = new AmqpLinkTerminusComparer();

        internal AmqpLinkTerminus(AmqpLinkSettings settings)
        {
            this.Settings = settings;
            this.UnsettledMap = new Dictionary<ArraySegment<byte>, Delivery>(ByteArrayComparer.Instance);
        }

        internal string ContainerId { get; set; }

        internal AmqpLinkSettings Settings { get; set; }

        internal string RemoteContainerId { get; set; }

        internal Dictionary<ArraySegment<byte>, Delivery> UnsettledMap { get; set; }

        /// <summary>
        /// Used to comparer the equality of <see cref="AmqpLinkTerminus"/>.
        /// </summary>
        internal class AmqpLinkTerminusComparer : IEqualityComparer<AmqpLinkTerminus>
        {
            public bool Equals(AmqpLinkTerminus x, AmqpLinkTerminus y)
            {
                if (x == null || y == null)
                {
                    return x == y;
                }

                return StringComparer.OrdinalIgnoreCase.Equals(x.ContainerId, y.ContainerId)
                        && StringComparer.OrdinalIgnoreCase.Equals(x.RemoteContainerId, y.RemoteContainerId)
                        && StringComparer.OrdinalIgnoreCase.Equals(x.Settings.LinkName, y.Settings.LinkName)
                        && x.Settings.Role == y.Settings.Role;
            }

            public int GetHashCode(AmqpLinkTerminus obj)
            {
                return (obj.Settings.LinkName.ToLower() + obj.Settings.Role).GetHashCode();
            }
        }
    }
}
