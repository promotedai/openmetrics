package ai.promoted.metrics.logprocessor.job.contentmetrics;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

/**
 * Utility for interacting with Flink SQL tables.
 */
interface Tables {

    static void createCatalogAndDatabase(TableEnvironment tableEnv) {
        GenericInMemoryCatalog catalog = new GenericInMemoryCatalog("default");
        tableEnv.registerCatalog("default", catalog);
        tableEnv.executeSql("USE CATALOG `default`");
        // TODO - rename
        tableEnv.executeSql("CREATE DATABASE metrics");
        tableEnv.executeSql("USE metrics");
    }
}
