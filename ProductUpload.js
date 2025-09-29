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
async function migrateProducts(mysqlConn, clickhouse) {
  try {
    const [rows] = await mysqlConn.execute(`SELECT id, name,regularPrice,salePrice,serviceProviderId,status FROM product`);

    console.log(`Fetched ${rows.length} rows from MySQL (products)`);

    if (rows.length > 0) {
      // Helper: format MySQL dates for ClickHouse
      function formatDate(dateValue) {
        // Handle null, undefined, or empty values
        if (!dateValue) {
          return null; // or return a default date
        }

        // Handle MySQL zero dates
        if (dateValue === '0000-00-00' || dateValue === '0000-00-00 00:00:00') {
          return null; // or return a default date
        }

        const date = new Date(dateValue);

        // Check if the date is valid
        if (isNaN(date.getTime())) {
          console.log(`Invalid date value: ${dateValue}`);
          return null; // or return a default date
        }

        return date.toISOString();
      }
// Insert rows into ClickHouse `products` table
      await clickhouse.insert({
        table: 'products', // target ClickHouse table
        values: rows.map(row => ({
          created_at: formatDate(new Date()), // mapping createdDate -> created_at
          id: row.id,
          name: row.name,
          regularPrice: row.regularPrice,
          salePrice: row.salePrice,
          serviceProviderId: row.serviceProviderId,
          status: row.status
        })),
        format: 'JSONEachRow'
      });

      console.log(' Products migrated to ClickHouse successfully!');
    }
  } catch (err) {
    console.error('Products migration error:', err);
  }
}


async function migrateData() {
    // MySQL connection
    const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 
    const clickhouse = createClient({ url: 'http://localhost:8123', username: 'default', password: '', database: 'clickHouseInvoice', });
    try {
        await migrateProducts(mysqlConn, clickhouse);
    }
    finally {
        await mysqlConn.end(); await clickhouse.close();
    }
}

migrateData();