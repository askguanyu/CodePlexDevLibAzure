//-----------------------------------------------------------------------
// <copyright file="DictionaryTableEntity.cs" company="YuGuan Corporation">
//     Copyright (c) YuGuan Corporation. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace DevLib.Azure.Storage
{
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// A TableEntity which allows callers direct access to the property map of the entity.
    /// </summary>
    public class DictionaryTableEntity : TableEntity, IDictionary<string, object>
    {
        /// <summary>
        /// The PropertyInfo array.
        /// </summary>
        private readonly PropertyInfo[] _propertyInfos;

        /// <summary>
        /// The properties.
        /// </summary>
        private IDictionary<string, EntityProperty> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryTableEntity"/> class.
        /// </summary>
        public DictionaryTableEntity()
        {
            this._properties = new Dictionary<string, EntityProperty>();
            this._propertyInfos = this.GetPublicGetSetProperties();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryTableEntity"/> class.
        /// </summary>
        /// <param name="partitionKey">The partition key.</param>
        /// <param name="rowKey">The row key.</param>
        public DictionaryTableEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
            this._properties = new Dictionary<string, EntityProperty>();
            this._propertyInfos = this.GetPublicGetSetProperties();
        }

        /// <summary>
        /// Gets the keys.
        /// </summary>
        public ICollection<string> Keys
        {
            get
            {
                return this._properties.Keys.ToList();
            }
        }

        /// <summary>
        /// Gets the values.
        /// </summary>
        public ICollection<object> Values
        {
            get
            {
                return this._properties.Values.Select(i => i.ToObject()).ToList();
            }
        }

        /// <summary>
        /// Gets all KeyValuePair in the dictionary.
        /// </summary>
        /// <value>A list of KeyValuePair.</value>
        public List<KeyValuePair<string, object>> KeyValuePairs
        {
            get
            {
                return this._properties.Select(i => this.ToObjectKeyValuePair(i)).ToList();
            }
        }

        /// <summary>
        /// Gets the number of elements contained in the DictionaryTableEntity.
        /// </summary>
        public int Count
        {
            get
            {
                return this._properties.Count;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the DictionaryTableEntity is read-only.
        /// </summary>
        public bool IsReadOnly
        {
            get
            {
                return this._properties.IsReadOnly;
            }
        }

        /// <summary>
        /// Gets or sets the element with the specified key.
        /// </summary>
        /// <param name="key">The key of the element to get or set.</param>
        /// <returns>The element with the specified key.</returns>
        public object this[string key]
        {
            get
            {
                return this._properties[key].ToObject();
            }

            set
            {
                this._properties[key] = value.ToEntityProperty();
            }
        }

        /// <summary>
        /// Reads the entity.
        /// </summary>
        /// <param name="properties">The properties.</param>
        /// <param name="operationContext">The operation context.</param>
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            this._properties = properties;

            foreach (var item in this._propertyInfos)
            {
                this._properties.Remove(item.Name);
            }
        }

        /// <summary>
        /// Writes the entity.
        /// </summary>
        /// <param name="operationContext">The operation context.</param>
        /// <returns>An IDictionary object that maps string property names to Microsoft.WindowsAzure.Storage.Table.EntityProperty typed values created by serializing this table entity instance.</returns>
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var result = this.RetrieveProperties();

            foreach (var item in this._properties)
            {
                if (!result.ContainsKey(item.Key))
                {
                    result.Add(item.Key, item.Value);
                }
            }

            return result;
        }

        /// <summary>
        /// Adds an element with the provided key and value to the DictionaryTableEntity.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Add(string key, object value)
        {
            this._properties.Add(key, value.ToEntityProperty());
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="key">The key to add or update.</param>
        /// <param name="value">The value to add or update.</param>
        /// <returns>Current DictionaryTableEntity instance.</returns>
        public DictionaryTableEntity AddOrUpdate(string key, object value)
        {
            this._properties[key] = value.ToEntityProperty();
            return this;
        }

        /// <summary>
        /// Update value, if not contain key then add value.
        /// </summary>
        /// <param name="item">The item to add or update.</param>
        /// <returns>Current DictionaryTableEntity instance.</returns>
        public DictionaryTableEntity AddOrUpdate(KeyValuePair<string, object> item)
        {
            return this.AddOrUpdate(item.Key, item.Value);
        }

        /// <summary>
        /// Determines whether the DictionaryTableEntity contains an element with the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>true if the DictionaryTableEntity contains an element with the key; otherwise, false. </returns>
        public bool ContainsKey(string key)
        {
            return this._properties.ContainsKey(key);
        }

        /// <summary>
        /// Removes the element with the specified key from the DictionaryTableEntity.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>true if the element is successfully removed; otherwise, false. This method also returns false if key was not found.</returns>
        public bool Remove(string key)
        {
            return this._properties.Remove(key);
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>true if the DictionaryTableEntity contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue(string key, out object value)
        {
            var entity = EntityProperty.CreateEntityPropertyFromObject(null);
            var result = this._properties.TryGetValue(key, out entity);
            value = entity.ToObject();
            return result;
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="T">The type of return value.</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>true if the DictionaryTableEntity contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue<T>(string key, out T value)
        {
            var entity = EntityProperty.CreateEntityPropertyFromObject(null);
            var result = this._properties.TryGetValue(key, out entity);
            value = entity.ToObject<T>();
            return result;
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">true to throw any exception that occurs.-or- false to ignore any exception that occurs.</param>
        /// <param name="defaultValueOnError">The default value if error occurred.</param>
        /// <returns>The value.</returns>
        public object GetValue(string key, bool throwOnError = true, object defaultValueOnError = null)
        {
            try
            {
                return this._properties[key].ToObject();
            }
            catch
            {
                if (throwOnError)
                {
                    throw;
                }

                return defaultValueOnError;
            }
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <typeparam name="T">The type of return value.</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="throwOnError">true to throw any exception that occurs.-or- false to ignore any exception that occurs.</param>
        /// <param name="defaultValueOnError">The default value if error occurred.</param>
        /// <returns>The value.</returns>
        public T GetValue<T>(string key, bool throwOnError = true, T defaultValueOnError = default(T))
        {
            try
            {
                return this._properties[key].ToObject<T>();
            }
            catch
            {
                if (throwOnError)
                {
                    throw;
                }

                return defaultValueOnError;
            }
        }

        /// <summary>
        /// Adds the specified item.
        /// </summary>
        /// <param name="item">The item.</param>
        public void Add(KeyValuePair<string, object> item)
        {
            this._properties.Add(this.ToEntityPropertyKeyValuePair(item));
        }

        /// <summary>
        /// Removes all items from the DictionaryTableEntity.
        /// </summary>
        public void Clear()
        {
            this._properties.Clear();
        }

        /// <summary>
        /// Determines whether the DictionaryTableEntity contains a specific value.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>true if item is found in the DictionaryTableEntity; otherwise, false.</returns>
        public bool Contains(KeyValuePair<string, object> item)
        {
            return this._properties.Contains(this.ToEntityPropertyKeyValuePair(item));
        }

        /// <summary>
        /// Copies the times to a compatible one-dimensional array, starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">The target array.</param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            this._properties.Select(i => this.ToObjectKeyValuePair(i)).ToList().CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the DictionaryTableEntity.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>true if item was successfully removed from the DictionaryTableEntity; otherwise, false. This method also returns false if item is not found.</returns>
        public bool Remove(KeyValuePair<string, object> item)
        {
            return this._properties.Remove(this.ToEntityPropertyKeyValuePair(item));
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>An enumerator that can be used to iterate through the collection.</returns>
        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            return this._properties.Select(i => this.ToObjectKeyValuePair(i)).GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>An enumerator that can be used to iterate through the collection.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this._properties.GetEnumerator();
        }

        /// <summary>
        /// Retrieves object's all properties value.
        /// </summary>
        /// <returns>Instance of Dictionary{string, EntityProperty}.</returns>
        private Dictionary<string, EntityProperty> RetrieveProperties()
        {
            Dictionary<string, EntityProperty> result = new Dictionary<string, EntityProperty>();

            foreach (PropertyInfo property in this._propertyInfos)
            {
                result.Add(property.Name, property.GetValue(this, null).ToEntityProperty());
            }

            return result;
        }

        /// <summary>
        /// Gets the public get set properties.
        /// </summary>
        /// <returns>Array of PropertyInfo.</returns>
        private PropertyInfo[] GetPublicGetSetProperties()
        {
            return this
                .GetType()
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(i => i.CanRead && i.CanWrite && i.GetGetMethod(true).IsPublic && i.GetSetMethod(true).IsPublic)
                .ToArray();
        }

        /// <summary>
        /// Converts from KeyValuePair{string, object} to KeyValuePair{string, EntityProperty}.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>KeyValuePair{string, EntityProperty} instance.</returns>
        private KeyValuePair<string, EntityProperty> ToEntityPropertyKeyValuePair(KeyValuePair<string, object> item)
        {
            return new KeyValuePair<string, EntityProperty>(item.Key, item.Value.ToEntityProperty());
        }

        /// <summary>
        /// Converts from KeyValuePair{string, EntityProperty} to KeyValuePair{string, object}.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <returns>KeyValuePair{string, object} instance.</returns>
        private KeyValuePair<string, object> ToObjectKeyValuePair(KeyValuePair<string, EntityProperty> item)
        {
            return new KeyValuePair<string, object>(item.Key, item.Value.ToObject());
        }
    }
}
