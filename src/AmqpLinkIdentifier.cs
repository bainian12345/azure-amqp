// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Amqp
{
    using Microsoft.Azure.Amqp.Framing;
    using System;

    class AmqpLinkIdentifier
    {
        string linkName;
        bool? role;

        public AmqpLinkIdentifier(string linkName, bool? role)
        {
            this.linkName = linkName;
            this.role = role;
        }

        /// <summary>
        /// Determines whether two link identifiers are equal based on <see cref="Attach.LinkName"/>
        /// and <see cref="Attach.Role"/>. Name comparison is case insensitive.
        /// </summary>
        /// <param name="obj">The object to compare with the current object.</param>
        /// <returns>True if the specified object is equal to the current object; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            AmqpLinkIdentifier other = obj as AmqpLinkIdentifier;
            if (other == null || other.linkName == null)
            {
                return false;
            }

            return this.linkName.Equals(other.linkName, StringComparison.CurrentCultureIgnoreCase) && this.role == other.role;
        }

        /// <summary>
        /// Gets a hash code of the object.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return (this.linkName.GetHashCode() * 397) + this.role.GetHashCode();
        }
    }
}
