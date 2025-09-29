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
async function migrateToAddress(mysqlConn, clickhouse) {
  try {
    const [rows] = await mysqlConn.execute(`SELECT id,email,firstName,middleName,lastName,mobile,phone,internalChargeout FROM customer`); console.log(`Fetched ${rows.length} rows from MySQL (customers → address)`);
// Insert rows into ClickHouse `customer` table
    if (rows.length > 0) {
      await clickhouse.insert({
        table: 'customer', // target ClickHouse table
        values: rows.map(row => ({
          id: row.id,
          email: row.email,
          name: [row.firstName, row.middleName, row.lastName].filter(Boolean).join(' '),
          phone: row.phone || row.mobile,
          address: row.internalChargeout,
          created_at: formatDate(new Date()),  //  always string
          updated_at: formatDate(new Date()),  //  always string
          franchise_id: 1, //change later
          location_id: 0,
          provider_id: 0
        }))
        ,
        format: 'JSONEachRow',
      });

      console.log(' Data migrated into customer table successfully!');
    }
  } catch (err) {
    console.error('Address migration error:', err);
  }
}


async function migrateData() {
    // MySQL connection
    const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 
    const clickhouse = createClient({ url: 'http://localhost:8123', username: 'default', password: '', database: 'clickHouseInvoice', });
    try {
        await migrateToAddress(mysqlConn, clickhouse);
    }
    finally {
        await mysqlConn.end(); await clickhouse.close();
    }
}

migrateData();