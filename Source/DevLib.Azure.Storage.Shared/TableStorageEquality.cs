//-----------------------------------------------------------------------
// <copyright file="TableStorageEquality.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Represents a Microsoft Azure table.
    /// </summary>
    public partial class TableStorage : IEquatable<TableStorage>, IEqualityComparer<TableStorage>, IEqualityComparer
    {
        /// <summary>
        /// Implements the == operator.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator ==(TableStorage x, TableStorage y)
        {
            if (((object)x) == null || ((object)y) == null)
            {
                return object.Equals(x, y);
            }

            return x.Equals(y);
        }

        /// <summary>
        /// Implements the != operator.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator !=(TableStorage x, TableStorage y)
        {
            if (((object)x) == null || ((object)y) == null)
            {
                return !object.Equals(x, y);
            }

            return !x.Equals(y);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns>true if the current object is equal to the other parameter; otherwise, false.</returns>
        public bool Equals(TableStorage other)
        {
            if (other == null)
            {
                return false;
            }

            if (this.InnerCloudTable.StorageUri == other.InnerCloudTable.StorageUri)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="obj">An object to compare with this object.</param>
        /// <returns>true if the current object is equal to the other parameter; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            TableStorage other = obj as TableStorage;

            if (other == null)
            {
                return false;
            }
            else
            {
                return this.Equals(other);
            }
        }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>true if the specified objects are equal; otherwise, false.</returns>
        public bool Equals(TableStorage x, TableStorage y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            else if (x == null || y == null)
            {
                return false;
            }

            return x.Equals(y);
        }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>true if the specified objects are equal; otherwise, false.</returns>
        public new bool Equals(object x, object y)
        {
            if (x == null && y == null)
            {
                return true;
            }
            else if (x == null || y == null)
            {
                return false;
            }

            return x.Equals(y);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.</returns>
        public override int GetHashCode()
        {
            return this.InnerCloudTable.StorageUri.GetHashCode();
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.</returns>
        public int GetHashCode(TableStorage obj)
        {
            return obj?.GetHashCode() ?? 0;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.</returns>
        public int GetHashCode(object obj)
        {
            return obj?.GetHashCode() ?? 0;
        }
    }
}
