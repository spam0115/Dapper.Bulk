using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Dapper.Bulk.Tests")]

namespace Dapper.Bulk
{
    /// <summary>
    /// Bulk inserts for Dapper
    /// </summary>
    public static class DapperBulk
    {
        private class PropertiesContainer
        {
            public List<PropertyInfo> AllProperties;
            public List<PropertyInfo> KeyProperties;
            public List<PropertyInfo> ComputedProperties;
            public IReadOnlyDictionary<string, string> ColumnNameMap;
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default).
        /// </summary>
        public static void BulkInsert<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30, bool identityInsert = false)
        {
            BulkInsert(connection, typeof(T), data.Cast<object>(), transaction, batchSize, bulkCopyTimeout, identityInsert);
        }

        /// <summary>
        /// Inserts entities into table.
        /// </summary>
        public static void BulkInsert(this SqlConnection connection, Type type, IEnumerable<object> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30, bool identityInsert = false)
        {
            var properties = GetProperties(type);
            var (tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr, insertProperties, sqlBulkCopyOptions) = PrepareInfo(type, identityInsert, properties);

            connection.Execute($@"SELECT TOP 0 {insertPropertiesStr} INTO {tempTableName} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.WriteToServer(ToDataTable(data, insertProperties).CreateDataReader());
            }

            var sqlString = GetInsertSql(tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr);
            connection.Execute(sqlString, null, transaction);
        }

        
        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) returns inserted entities.
        /// </summary>
        public static IEnumerable<T> BulkInsertAndSelect<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30, bool identityInsert = false)
        {
            var type = typeof(T);

            var properties = GetProperties(type);
            var (tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr, insertProperties, sqlBulkCopyOptions) = PrepareInfo(type, identityInsert, properties);

            if (!properties.KeyProperties.Any())
            {
                var dataList = data.ToList();
                connection.BulkInsert(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var keyPropertiesStr = GetColumnsStringSqlServer(properties.KeyProperties, properties.ColumnNameMap);
            var keyPropertiesInsertedStr = GetColumnsStringSqlServer(properties.KeyProperties, properties.ColumnNameMap, "inserted.");
            var allPropertiesStr = GetColumnsStringSqlServer(PropertiesCache.TypePropertiesCache(type), properties.ColumnNameMap, "target.");

            var tempInsertedWithIdentity = $"@TempInserted_{tableName}".Replace(".", string.Empty);

            connection.Execute($@"SELECT TOP 0 {insertPropertiesStr} INTO {tempTableName} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempTableName;
                bulkCopy.WriteToServer(ToDataTable(data, insertProperties).CreateDataReader());
            }

            var tableStr = GetKeysString(properties.KeyProperties, properties.ColumnNameMap);
            var joinOnStr = GetJoinString(properties);

            var sqlStr = GetInsertAndSelectSql(tableName, identityInsertOnStr, identityInsertOffStr, tempTableName, insertPropertiesStr, keyPropertiesStr, keyPropertiesInsertedStr, allPropertiesStr, tempInsertedWithIdentity, tableStr, joinOnStr);
            return connection.Query<T>(sqlStr, null, transaction);
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) asynchronously.
        /// </summary>
        public static async Task BulkInsertAsync<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30, bool identityInsert = false)
        {
            var type = typeof(T);
            var properties = GetProperties(type);
            var (tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr, insertProperties, sqlBulkCopyOptions) = PrepareInfo(type, identityInsert, properties);

            await connection.ExecuteAsync($@"SELECT TOP 0 {insertPropertiesStr} INTO {tempTableName} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempTableName;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, insertProperties).CreateDataReader());
            }

            var sqlString = GetInsertSql(tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr);
            await connection.ExecuteAsync(sqlString, null, transaction);
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) asynchronously and returns inserted entities.
        /// </summary>
        public static async Task<IEnumerable<T>> BulkInsertAndSelectAsync<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30, bool identityInsert = false)
        {
            var type = typeof(T);
            var properties = GetProperties(type);
            var (tableName, tempTableName, identityInsertOnStr, identityInsertOffStr, insertPropertiesStr, insertProperties, sqlBulkCopyOptions) = PrepareInfo(type, identityInsert, properties);

            if (!properties.KeyProperties.Any())
            {
                var dataList = data.ToList();
                await connection.BulkInsertAsync(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var keyPropertiesStr = GetColumnsStringSqlServer(properties.KeyProperties, properties.ColumnNameMap);
            var keyPropertiesInsertedStr = GetColumnsStringSqlServer(properties.KeyProperties, properties.ColumnNameMap, "inserted.");
            var allPropertiesStr = GetColumnsStringSqlServer(PropertiesCache.TypePropertiesCache(type), properties.ColumnNameMap, "target.");

            var tempInsertedWithIdentity = $"@TempInserted_{tableName}".Replace(".", string.Empty);

            await connection.ExecuteAsync($@"SELECT TOP 0 {insertPropertiesStr} INTO {tempTableName} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, sqlBulkCopyOptions, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempTableName;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, insertProperties).CreateDataReader());
            }

            var tableStr = GetKeysString(properties.KeyProperties, properties.ColumnNameMap);
            string joinOnStr = GetJoinString(properties);

            var sqlStr = GetInsertAndSelectSql(tableName, identityInsertOnStr, identityInsertOffStr, tempTableName, insertPropertiesStr, keyPropertiesStr, keyPropertiesInsertedStr, allPropertiesStr, tempInsertedWithIdentity, tableStr, joinOnStr);
            return await connection.QueryAsync<T>(sqlStr, null, transaction);
        }

        private static string GetJoinString(PropertiesContainer properties)
        {
            var str = properties.KeyProperties.Select(k => 
                $"target.[{(properties.ColumnNameMap.ContainsKey(k.Name) ? properties.ColumnNameMap[k.Name] : k.Name)}] = ins.[{(properties.ColumnNameMap.ContainsKey(k.Name) ? properties.ColumnNameMap[k.Name] : k.Name)}]");
            return string.Join(" AND ", str);
        }
        
        private static PropertiesContainer GetProperties(Type type)
        {
            var properties = new PropertiesContainer();
            properties.AllProperties = PropertiesCache.TypePropertiesCache(type);
            properties.KeyProperties = PropertiesCache.KeyPropertiesCache(type);
            properties.ComputedProperties = PropertiesCache.ComputedPropertiesCache(type);
            properties.ColumnNameMap = PropertiesCache.GetColumnNamesCache(type);

            return properties;
        }

        private static (string tableName, string tempTableName, string identityInsertOn, string identityInsertOff, string insertPropertiesStr, List<PropertyInfo> insertProperties,  SqlBulkCopyOptions sqlBulkCopyOptions) PrepareInfo(Type type, bool identityInsert, PropertiesContainer properties)
        {
            var insertProperties = properties.AllProperties.Except(properties.ComputedProperties).ToList();
            if (!identityInsert)
                insertProperties = insertProperties.Except(properties.KeyProperties).ToList();
            var insertPropertiesStr = GetColumnsStringSqlServer(insertProperties, properties.ColumnNameMap);

            var tableName = TableMapper.GetTableName(type);
            var tempTableName = GetTempTableName(tableName);

            var keyIsGuid = AnyKeyIsGuid(properties.KeyProperties);
            var (identityInsertOn, identityInsertOff, sqlBulkCopyOptions) = GetIdentityInsertOptions(identityInsert, keyIsGuid, tableName);

            return (tableName, tempTableName, identityInsertOn, identityInsertOff, insertPropertiesStr, insertProperties, sqlBulkCopyOptions);
        }

        private static string GetTempTableName(string tableName) => $"#TempInsert_{tableName}".Replace(".", string.Empty);

        /// <summary>
        /// this provides support for the feature allowing object property names to be different than table columns names.
        /// </summary>
        /// <param name="properties"></param>
        /// <param name="columnNames"></param>
        /// <param name="tablePrefix"></param>
        /// <returns></returns>
        private static string GetColumnsStringSqlServer(IEnumerable<PropertyInfo> properties, IReadOnlyDictionary<string, string> columnNames, string tablePrefix = null)
        {
            if (tablePrefix == "target.")
            {
                return string.Join(", ", properties.Select(property => $"{tablePrefix}[{columnNames[property.Name]}] as [{property.Name}] "));
            }

            return string.Join(", ", properties.Select(property => $"{tablePrefix}[{columnNames[property.Name]}] "));
        }

        private static DataTable ToDataTable<T>(IEnumerable<T> data, IList<PropertyInfo> properties)
        {
            var dataTable = new DataTable();
            var typeCasts = properties.Select(p => p.PropertyType.IsEnum ? Enum.GetUnderlyingType(p.PropertyType) : null).ToArray();

            foreach (var property in properties)
            {
                var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                dataTable.Columns.Add(property.Name, typeCasts[properties.IndexOf(property)] ?? propertyType);
            }

            foreach (var item in data)
            {
                var values = properties.Select((p, i) => typeCasts[i] == null ? p.GetValue(item) : Convert.ChangeType(p.GetValue(item), typeCasts[i])).ToArray();
                dataTable.Rows.Add(values);
            }

            return dataTable;
        }

        internal static string FormatTableName(string table)
        {
            if (string.IsNullOrEmpty(table)) return table;
            return string.Join(".", table.Split('.').Select(part => $"[{part}]"));
        }

        private static (string identityInsertOn, string identityInsertOff, SqlBulkCopyOptions bulkCopyOptions) GetIdentityInsertOptions(bool identityInsert, bool keyIsGuid, string tableName)
        {
            var identityInsertOn = string.Empty;
            var identityInsertOff = string.Empty;
            var bulkCopyOptions = identityInsert ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;

            if (identityInsert && !keyIsGuid)
            {
                identityInsertOn = $"SET IDENTITY_INSERT {FormatTableName(tableName)} ON";
                identityInsertOff = $"SET IDENTITY_INSERT {FormatTableName(tableName)} OFF";
            }

            return (identityInsertOn, identityInsertOff, bulkCopyOptions);
        }

        private static bool AnyKeyIsGuid(IEnumerable<PropertyInfo> propertyInfos) => propertyInfos.Any(p => p.PropertyType == typeof(Guid));

        private static string GetKeysString(IEnumerable<PropertyInfo> keyProperties, IReadOnlyDictionary<string, string> columns)
        {
            var keys = keyProperties.Select(k =>
            {
                if (columns.ContainsKey(k.Name))
                {
                    var typeString = k.PropertyType.Name switch
                    {
                        "Guid" => "uniqueidentifier",
                        "Int32" or "UInt32" => "int",
                        "Int64" or "UInt64" => "bigint",
                        _ => throw new ArgumentException($"Invalid data type used in primary key. Type='{k.PropertyType.Name}'.")
                    };
                    return $"[{columns[k.Name]}] {typeString}";
                }
                else
                {
                    return $"[{k.Name}]" + " bigint";
                }
            });

            return String.Join(",", keys);
        }

        private static string GetInsertSql(string tableName, string tempTableNameStr, string identityInsertOnStr, string identityInsertOffStr, string insertPropertiesStr)
        {
            return 
                $@"{identityInsertOnStr}
                INSERT INTO {FormatTableName(tableName)} ({insertPropertiesStr}) 
                SELECT {insertPropertiesStr} FROM {tempTableNameStr}
                {identityInsertOffStr}
                DROP TABLE {tempTableNameStr};";
        }

        private static string GetInsertAndSelectSql(string tableName, string identityInsertOnStr, string identityInsertOffStr, string tempTableName, string insertPropertiesStr, string keyPropertiesStr, string keyPropertiesInsertedStr, string allPropertiesStr, string tempInsertedWithIdentity, string tableStr, string joinOnStr)
        {
            return 
                $@"{identityInsertOnStr}
                DECLARE {tempInsertedWithIdentity} TABLE ({tableStr})
                INSERT INTO {FormatTableName(tableName)} ({insertPropertiesStr}) 
                    OUTPUT {keyPropertiesInsertedStr} INTO {tempInsertedWithIdentity} ({keyPropertiesStr})
                SELECT {insertPropertiesStr} FROM {tempTableName}
                {identityInsertOffStr}
                SELECT {allPropertiesStr}
                FROM {FormatTableName(tableName)} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOnStr}
                DROP TABLE {tempTableName};";
        }
    }
}