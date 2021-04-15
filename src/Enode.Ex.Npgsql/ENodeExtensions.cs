using System;
using AggregateSnapshotStore;
using ECommon.Components;
using ENode.AggregateSnapshotStore;
using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;

namespace Enode.Ex.Npgsql
{
    public static class ENodeExtensions
    {
        

        /// <summary>Use the NpgsqlEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseNpgsqlEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, NpgsqlEventStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the NpgsqlPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseNpgsqlPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, NpgsqlPublishedVersionStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the NpgsqlLockService as the ILockService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseNpgsqlLockService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ILockService, NpgsqlLockService>();
            return eNodeConfiguration;
        }
        /// <summary>Initialize the NpgsqlEventStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="versionIndexName"></param>
        /// <param name="commandIndexName"></param>
        /// <param name="batchInsertTimeoutSeconds"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeNpgsqlEventStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "EventStream",
            int tableCount = 1,
            string versionIndexName = "IX_EventStream_AggId_Version",
            string commandIndexName = "IX_EventStream_AggId_CommandId",
            int bulkCopyBatchSize = 1000,
            int batchInsertTimeoutSeconds = 60)
        {
            ((NpgsqlEventStore)ObjectContainer.Resolve<IEventStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                versionIndexName,
                commandIndexName,
                bulkCopyBatchSize,
                batchInsertTimeoutSeconds);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the NpgsqlPublishedVersionStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="uniqueIndexName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeNpgsqlPublishedVersionStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "PublishedVersion",
            int tableCount = 1,
            string uniqueIndexName = "IX_PublishedVersion_AggId_Version")
        {
            ((NpgsqlPublishedVersionStore)ObjectContainer.Resolve<IPublishedVersionStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                uniqueIndexName);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the NpgsqlLockService with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeNpgsqlLockService(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "LockKey")
        {
            ((NpgsqlLockService)ObjectContainer.Resolve<ILockService>()).Initialize(connectionString, tableName);
            return eNodeConfiguration;
        }
        public static ENodeConfiguration UseNpgsqlAggregateSnapshotStore(this ENodeConfiguration enodeConfiguration)
        {
            var configuration = enodeConfiguration.GetCommonConfiguration();
            enodeConfiguration.UseAggregateSnapshotStore<NpgsqlAggregateSnapshotStore>();
            configuration.SetDefault<IAggregateSnapshotRequestFilter, DefaultAggregateSnapshotRequestFilter>((string)null);
            configuration.SetDefault<IAggregateSnapshotRequestQueue, DefaultAggregateSnapshotRequestProcessor>((string)null);
            return enodeConfiguration;
        }

        public static ENodeConfiguration InitializeNpgsqlAggregateSnapshotStore(this ENodeConfiguration enodeConfiguration, string connectionString, string[]  aggregateTypeNames)
        {
            enodeConfiguration.InitializeAggregateSnapshotStore<NpgsqlAggregateSnapshotStore>(s =>
            s.Initialize(
                connectionString,
                aggregateTypeNames:aggregateTypeNames
            ));

            ((DefaultAggregateSnapshotSaver)ObjectContainer.Resolve<IAggregateSnapshotSaver>()).Initialize(1);
            ((DefaultAggregateSnapshotRequestProcessor)ObjectContainer.Resolve<IAggregateSnapshotRequestQueue>()).Initialize(
                TimeSpan.FromSeconds(3),
                ObjectContainer.Resolve<IAggregateSnapshotRequestFilter>(),
                ObjectContainer.Resolve<IAggregateSnapshotSaver>()
            );
            ((DefaultAggregateSnapshotRequestFilter)ObjectContainer.Resolve<IAggregateSnapshotRequestFilter>()).Initialize(
                20,
                ObjectContainer.Resolve<IAggregateSnapshotStore>()
            );
            return enodeConfiguration;
        }

        public static ENodeConfiguration StartAggregateSnapshotRequestProcessor(this ENodeConfiguration enodeConfiguration)
        {
            ((DefaultAggregateSnapshotRequestProcessor)ObjectContainer.Resolve<IAggregateSnapshotRequestQueue>()).Start();
            return enodeConfiguration;
        }

        public static ENodeConfiguration ShutdownAggregateSnapshotRequestProcessor(this ENodeConfiguration enodeConfiguration)
        {
            ((DefaultAggregateSnapshotRequestProcessor)ObjectContainer.Resolve<IAggregateSnapshotRequestQueue>()).Stop();

            return enodeConfiguration;
        }
    }
}