using System;
using System.Data;
using Xunit;
using Npgsql;
using System.Collections.Generic;
using ENode.Ex.Postgres;
using System.Globalization;
using System.Reflection;
using System.Linq;

namespace NpgsqlTests
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            var rs = GetRecords(9990);
            var connectstr = "Host=192.168.3.5;Port=15432;Username=postgres;Password=123;Database=postgres";
            var helper = new PgBulkCopyHelper<StreamRecord>(null, "EventStream");

            DataTable dataTable = helper.InitDataTable();
            using (var connection = new NpgsqlConnection(connectstr))
            {
                connection.Open();

                DataTable dataTable1 = new DataTable();
                dataTable1.Columns.Add("AggregateRootId");
                dataTable1.Columns.Add("AggregateRootName");

                helper.FillDataTable(rs,dataTable);

                BulkInsert(connection,"ssdsf",rs.ToArray());
            }
        }

        int _bulkCopyBatchSize = 100;

          private void BulkInsert(NpgsqlConnection connection, string aggregateRootId, StreamRecord[] eventStreamList)
        {
            int index = 0;
            do
            {
                var count = eventStreamList.Length - index;

                StreamRecord[] copyArray = null;

                if (count > _bulkCopyBatchSize)
                {
                    copyArray = eventStreamList.Skip(index).Take(_bulkCopyBatchSize).ToArray();

                    index += _bulkCopyBatchSize;
                }
                else
                {
                    copyArray = eventStreamList.Skip(index).ToArray();

                    index = 0;
                }

                if (copyArray.Length > 0)
                {
                    var commandFormat = string.Format(CultureInfo.InvariantCulture, "COPY \"EventStream1\" (\"AggregateRootTypeName\",\"AggregateRootId\",\"Version\",\"CommandId\",\"CreatedOn\",\"Events\") FROM STDIN BINARY");
                    using (var writer = connection.BeginBinaryImport(commandFormat))
                    {

                        foreach (var item in copyArray)
                        {
                            writer.StartRow();

                            writer.Write(item.AggregateRootTypeName, NpgsqlTypes.NpgsqlDbType.Varchar);
                            writer.Write(item.AggregateRootId, NpgsqlTypes.NpgsqlDbType.Varchar);
                            writer.Write(item.Version, NpgsqlTypes.NpgsqlDbType.Integer);
                            writer.Write(item.CommandId, NpgsqlTypes.NpgsqlDbType.Varchar);
                            writer.Write(item.CreatedOn, NpgsqlTypes.NpgsqlDbType.Timestamp);
                            writer.Write(item.Events, NpgsqlTypes.NpgsqlDbType.Varchar);

                        }

                        writer.Complete();
                    }
                }
            }
            while (index > 0);
        }


        List<StreamRecord> GetRecords(int count)
        {
            var list = new List<StreamRecord>();

            for (int i = 0; i < count; i++)
            {
                list.Add(new StreamRecord
                {
                    Sequence = i,
                    CommandId = i.ToString(),
                    AggregateRootId = i.ToString(),
                    AggregateRootTypeName = i.ToString(),
                    Version = i + 1,
                    CreatedOn = DateTime.Now,
                    Events = "asdfsadfasdfasdfsaoldfjsaldfjsldjflsdjflsjflsajflsajflsajdfls",
                });
            }
            return list;
        }

        class StreamRecord
        {
            public long Sequence { get; set; }
            public string AggregateRootTypeName { get; set; }
            public string AggregateRootId { get; set; }
            public int Version { get; set; }
            public string CommandId { get; set; }
            public DateTime CreatedOn { get; set; }
            public string Events { get; set; }
        }
    }
}
