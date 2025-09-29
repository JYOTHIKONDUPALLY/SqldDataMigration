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

// Async function to migrate brand data from MySQL to ClickHouse
async function migrateBrands(mysqlConn, clickhouse) {
  try {
    // Fetch brand rows from MySQL
    const [rows] = await mysqlConn.execute(`
      SELECT id, franchiseId, name, productCategoryId, serviceProviderId, status
      FROM productbrand
    `);

    console.log(`Fetched ${rows.length} rows from MySQL (brands)`);

    if (rows.length > 0) {
      // Insert rows into ClickHouse `brands` table
      await clickhouse.insert({
        table: 'brands',
        values: rows.map(row => ({
          brand_id: row.id,                // Map MySQL id → ClickHouse brand_id
          franchiseId: row.franchiseId,    // Franchise reference
          name: row.name,                  // Brand name
          productCategoryId: row.productCategoryId, // Category association
          serviceProviderId: row.serviceProviderId, // Service provider reference
          status: row.status               // Current status
        })),
        format: 'JSONEachRow', // ClickHouse expects JSONEachRow format
      });

      console.log('Brands migrated to ClickHouse successfully!');
    }
  } catch (err) {
    // Log any errors that occur during migration
    console.error('Brands migration error:', err);
  }
}



async function migrateData() {
    // MySQL connection
    const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 
    const clickhouse = createClient({ url: 'http://localhost:8123', username: 'default', password: '', database: 'clickHouseInvoice', });
    try {
        await migrateBrands(mysqlConn, clickhouse);
    }
    finally {
        await mysqlConn.end(); await clickhouse.close();
    }
}

migrateData();