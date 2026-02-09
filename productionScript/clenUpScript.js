import { createClient } from '@clickhouse/client';

const CLICKHOUSE_DB = 'clickHouseInvoice';
const TABLE_PREFIX = 'customers_';

const client = createClient({
  host: 'http://localhost:8123',
  database: CLICKHOUSE_DB,
});

async function cleanupInvoiceItemTables() {
  try {
    console.log('🔍 Fetching tables to delete...');

    const result = await client.query({
      query: `
        SELECT name
        FROM system.tables
        WHERE database = {db:String}
          AND name LIKE {prefix:String}
      `,
      query_params: {
        db: CLICKHOUSE_DB,
        prefix: `${TABLE_PREFIX}%`,
      },
      format: 'JSONEachRow',
    });

    const tables = await result.json();

    if (!tables.length) {
      console.log('✅ No tables found.');
      return;
    }

    for (const { name } of tables) {
      console.log(`➡️ Dropping table: ${name}`);

      // 1️⃣ Drop invoice item table
      await client.exec({
        query: `DROP TABLE IF EXISTS ${CLICKHOUSE_DB}.${name}`,
      });

      console.log(`   ✔ Dropped ${name}`);

      // 2️⃣ Delete migration_progress entry (FIXED COLUMN)
      await client.exec({
        query: `
          ALTER TABLE migration_progress
          DELETE WHERE table_name = {table:String}
        `,
        query_params: {
          table: name,
        },
      });

      console.log(`   🧹 Removed migration_progress entry for ${name}`);
    }

    console.log('🎉 Cleanup completed successfully');
  } catch (err) {
    console.error('❌ Cleanup failed:', err.message);
  } finally {
    await client.close();
  }
}

cleanupInvoiceItemTables();
