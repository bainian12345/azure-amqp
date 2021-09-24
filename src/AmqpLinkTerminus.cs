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

        internal AmqpLinkTerminus(AmqpLinkSettings linkSettings)
        {
            this.LinkSettings = linkSettings;
        }

        internal string ContainerId { get; set; }

        internal AmqpLinkSettings LinkSettings { get; set; }

        internal string RemoteContainerId { get; set; }

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
                        && StringComparer.OrdinalIgnoreCase.Equals(x.LinkSettings.LinkName, y.LinkSettings.LinkName)
                        && x.LinkSettings.Role == y.LinkSettings.Role;
            }

            public int GetHashCode(AmqpLinkTerminus obj)
            {
                return (obj.LinkSettings.LinkName.ToLower() + obj.LinkSettings.Role).GetHashCode();
            }
        }
    }
}
