using ECommon.Components;
using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.Ex.Postgres
{
    public static class ENodeExtensions
    {
        

        /// <summary>Use the PostgresEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UsePostgresEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, PostgresEventStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the PostgresPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UsePostgresPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, PostgresPublishedVersionStore>();
            return eNodeConfiguration;
        }
        /// <summary>Use the PostgresLockService as the ILockService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UsePostgresLockService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ILockService, PostgresLockService>();
            return eNodeConfiguration;
        }
        /// <summary>Initialize the PostgresEventStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="versionIndexName"></param>
        /// <param name="commandIndexName"></param>
        /// <param name="batchInsertTimeoutSeconds"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializePostgresEventStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "EventStream",
            int tableCount = 1,
            string versionIndexName = "IX_EventStream_AggId_Version",
            string commandIndexName = "IX_EventStream_AggId_CommandId",
            int batchInsertTimeoutSeconds = 60)
        {
            ((PostgresEventStore)ObjectContainer.Resolve<IEventStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                versionIndexName,
                commandIndexName,
                batchInsertTimeoutSeconds);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the PostgresPublishedVersionStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <param name="tableCount"></param>
        /// <param name="uniqueIndexName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializePostgresPublishedVersionStore(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "PublishedVersion",
            int tableCount = 1,
            string uniqueIndexName = "IX_PublishedVersion_AggId_Version")
        {
            ((PostgresPublishedVersionStore)ObjectContainer.Resolve<IPublishedVersionStore>()).Initialize(
                connectionString,
                tableName,
                tableCount,
                uniqueIndexName);
            return eNodeConfiguration;
        }
        /// <summary>Initialize the PostgresLockService with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializePostgresLockService(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "LockKey")
        {
            ((PostgresLockService)ObjectContainer.Resolve<ILockService>()).Initialize(connectionString, tableName);
            return eNodeConfiguration;
        }
    }
}