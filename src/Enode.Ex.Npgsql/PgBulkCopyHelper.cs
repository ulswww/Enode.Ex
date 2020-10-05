﻿using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;

namespace Enode.Ex.Npgsql
{
    /// <summary>
    /// 用以快速将大批量数据插入到Npgsqlql中
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class PgBulkCopyHelper<TEntity>
    {
        /// <summary>
        /// TEntity的属性信息
        /// Dictionary(string "property_name", Type property_type)
        /// </summary>
        public Dictionary<string, Type> PropInfo { get; set; }
        /// <summary>
        /// TEntity的属性名称列表
        /// </summary>
        public List<string> PropNames { get; set; }
        /// <summary>
        /// 数据表全名：schema.tableName or tableName
        /// </summary>
        public string FullTableName { get; set; }

        

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="schema">数据表的schema，一般为public</param>
        /// <param name="tableName">数据表的名称</param>
        public PgBulkCopyHelper(string schema, string tableName)
        {
            PropNames = new List<string>();
            PropInfo = new Dictionary<string, Type>();
            PropertyInfo[] typeArgs = GetPropertyFromTEntity();
            foreach (PropertyInfo tParam in typeArgs)
            {
                PropNames.Add(tParam.Name);
                PropInfo[tParam.Name] = tParam.PropertyType;
            }

            if (!string.IsNullOrWhiteSpace(tableName))
            {
                if (string.IsNullOrWhiteSpace(schema))
                {
                    FullTableName = tableName;
                }
                else
                    FullTableName = string.Format("{0}.{1}", schema, tableName);
            }
            FullTableName = Delimited(FullTableName);
        }

        string Delimited(string objName)
        {
            var dotindex = objName.IndexOf(".");
            if(dotindex < 0)
              return $"\"{objName}\"";

            var scheme = objName.Substring(0,dotindex);
            var table = objName.Substring(dotindex+1);

            return $"{scheme}.\"{table}\"";
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="tableName">数据表的名称</param>
        public PgBulkCopyHelper(string tableName)
            : this(null, tableName)
        { }

        /// <summary>
        /// 获取TEntity的属性信息
        /// </summary>
        /// <returns>TEntity的属性信息的列表</returns>
        private PropertyInfo[] GetPropertyFromTEntity()
        {
            Type t = typeof(TEntity);
            PropertyInfo[] typeArgs = t.GetProperties();
            return typeArgs;
        }

        /// <summary>
        /// 根据TEntity的属性信息构造对应数据表
        /// </summary>
        /// <returns>只有字段信息的数据表</returns>
        public DataTable InitDataTable()
        {
            DataTable dataTable = new DataTable();

            foreach (PropertyInfo tParam in GetPropertyFromTEntity())
            {
                Type propType = tParam.PropertyType;
                //由于 DataSet 不支持 System.Nullable<> 类型，因此要先做判断
                if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    propType = propType.GetGenericArguments()[0];
                dataTable.Columns.Add(tParam.Name, propType);
            }

            return dataTable;
        }

        /// <summary>
        /// 根据TEntity可枚举列表填充给定的数据表
        /// </summary>
        /// <param name="entities">TEntity类型的可枚举列表</param>
        /// <param name="dataTable">数据表</param>
        public void FillDataTable(IEnumerable<TEntity> entities, DataTable dataTable)
        {
            if (entities != null && entities.Count() > 0)
            {
                foreach (TEntity entity in entities)
                {
                    FillDataTable(entity, dataTable);
                }
            }
        }

        /// <summary>
        /// 在DataTable中插入单条数据
        /// </summary>
        /// <param name="entity">具体数据</param>
        /// <param name="dataTable">数据表</param>
        public void FillDataTable(TEntity entity, DataTable dataTable)
        {
            var dataRow = dataTable.NewRow();
            int colNum = dataTable.Columns.Count;
            PropertyInfo[] typeArgs = GetPropertyFromTEntity();
            for (int i = 0; i < colNum; i++)
            {
                dataRow[i] = typeArgs[i].GetValue(entity);
            }
            dataTable.Rows.Add(dataRow);
        }

        /// <summary>
        /// 通过NpgsqlQL连接把dataTable中的数据整块填充到数据库对应的数据表中
        /// 注意，该函数不负责NpgsqlConnection的创建、打开以及关闭
        /// </summary>
        /// <param name="conn">NpgsqlQL连接</param>
        /// <param name="dataTable">数据表</param>
        public void BulkInsert(NpgsqlConnection conn, DataTable dataTable)
        {
            var columns = "";
            foreach (var item in PropNames)
            {
                if(item == "Id")
                  continue;

                columns+=$"\"{item}\",";
            }
            columns = columns.TrimEnd(',');
            var commandFormat = string.Format(CultureInfo.InvariantCulture, "COPY {0} ({1}) FROM STDIN BINARY", FullTableName,columns);
            using (var writer = conn.BeginBinaryImport(commandFormat))
            {
                foreach (DataRow item in dataTable.Rows)
                    writer.WriteRow(item.ItemArray);

                writer.Complete();
            }
        }
    }
}
