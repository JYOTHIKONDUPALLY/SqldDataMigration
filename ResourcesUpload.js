import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

/**
 * Formats a value into ClickHouse DateTime string
 */
function formatDate(dateValue) {
    if (!dateValue) return '1970-01-01 00:00:00';

    let date;
    try {
        if (dateValue instanceof Date) {
            date = dateValue;
        } else {
            const dateStr = String(dateValue).trim();
            if (dateStr.includes(' ') && !dateStr.includes('T')) {
                date = new Date(dateStr.replace(' ', 'T') + 'Z');
            } else {
                date = new Date(dateStr);
            }
        }
        if (isNaN(date.getTime())) return '1970-01-01 00:00:00';

        const y = date.getFullYear();
        const m = String(date.getMonth() + 1).padStart(2, '0');
        const d = String(date.getDate()).padStart(2, '0');
        const h = String(date.getHours()).padStart(2, '0');
        const mi = String(date.getMinutes()).padStart(2, '0');
        const s = String(date.getSeconds()).padStart(2, '0');
        return `${y}-${m}-${d} ${h}:${mi}:${s}`;
    } catch {
        return '1970-01-01 00:00:00';
    }
}

/**
 * Formats a value into ClickHouse Date (YYYY-MM-DD only)
 */
function formatDateOnly(dateValue) {
    if (!dateValue) return '1970-01-01';

    let date;
    try {
        if (dateValue instanceof Date) {
            date = dateValue;
        } else {
            date = new Date(String(dateValue).trim());
        }
        if (isNaN(date.getTime())) return '1970-01-01';

        const y = date.getFullYear();
        const m = String(date.getMonth() + 1).padStart(2, '0');
        const d = String(date.getDate()).padStart(2, '0');
        return `${y}-${m}-${d}`;
    } catch {
        return '1970-01-01';
    }
}
async function migrateResources(mysqlConn, clickhouse, batchSize = 1000) {
  try {
    const [countResult] = await mysqlConn.execute(`SELECT COUNT(*) as total FROM resource`);
    const totalRecords = countResult[0].total;
    console.log(`Total resources to migrate: ${totalRecords}`);

    let offset = 0;
    while (offset < totalRecords) {
      const [rows] = await mysqlConn.execute(
        `SELECT id FROM resource ORDER BY id LIMIT ? OFFSET ?`,
        [batchSize, offset]
      );

      const data = rows.map(r => ({
        resource_id: r.id
      }));
// Insert rows into ClickHouse `resources` table
      if (data.length > 0) {
        try {
          await clickhouse.insert({
            table: 'resources',
            values: data,
            format: 'JSONEachRow',
          });
          console.log(` Migrated resources batch ${offset + 1} → ${offset + data.length}`);
        } catch (insertErr) {
          console.error('❌ Batch insert failed, falling back to single inserts:', insertErr.message);
          for (const rec of data) {
            try {
              await clickhouse.insert({
                table: 'resources',
                values: [rec],
                format: 'JSONEachRow',
              });
            } catch (rowErr) {
              console.error(`⚠️ Failed to insert resource ID ${rec.resource_id}:`, rowErr.message);
            }
          }
        }
      }

      offset += batchSize;
    }

    console.log('🎉 Resources migration completed!');
  } catch (err) {
    console.error('❌ Resources migration error:', err);
  }
}


async function migrateData() {
    // MySQL connection
    const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 
    const clickhouse = createClient({ url: 'http://localhost:8123', username: 'default', password: '', database: 'clickHouseInvoice', });
    try {
        await migrateResources(mysqlConn, clickhouse);
    }
    finally {
        await mysqlConn.end(); await clickhouse.close();
    }
}

migrateData();