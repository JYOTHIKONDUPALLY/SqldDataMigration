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
async function migrateProviderAffiliates(mysqlConn, clickhouse, batchSize = 1000) {
  const safeStr = (v, def = '') => (v === null || v === undefined ? def : String(v));
  const safeNum = (v, def = 0) => (v === null || v === undefined || isNaN(Number(v)) ? def : Number(v));

  // Format dates for ClickHouse
  function formatDate(dateValue) {
    if (!dateValue) return '1970-01-01 00:00:00';
    try {
      const d = new Date(dateValue);
      if (isNaN(d.getTime())) return '1970-01-01 00:00:00';
      return d.toISOString().slice(0, 19).replace('T', ' '); // YYYY-MM-DD HH:mm:ss
    } catch {
      return '1970-01-01 00:00:00';
    }
  }

  try {
    const [countResult] = await mysqlConn.execute(`SELECT COUNT(*) as total FROM providerAffiliate`);
    const totalRecords = countResult[0].total;
    console.log(`Total provider affiliates to migrate: ${totalRecords}`);

    let offset = 0;
    while (offset < totalRecords) {
      const [rows] = await mysqlConn.execute(
        `SELECT id, affiliateName, serviceProviderId, affiliateCode, date, deletedOn 
         FROM providerAffiliate 
         ORDER BY id LIMIT ? OFFSET ?`,
        [batchSize, offset]
      );

      const data = rows.map(r => ({
        id: r.id,
        name: safeStr(r.affiliateName),
        code: safeStr(r.affiliateCode),
        franchise_id: 1,//chnage later,
        created_at: formatDate(r.date),
        updated_at: formatDate(r.deletedOn),
        contact_email: '',   // no source field in MySQL
        contact_phone: ''    // no source field in MySQL
      }));
// Insert rows into ClickHouse `providers` table
      if (data.length > 0) {
        try {
          await clickhouse.insert({
            table: 'providers',
            values: data,
            format: 'JSONEachRow',
          });
          console.log(` Migrated affiliates batch ${offset + 1} → ${offset + data.length}`);
        } catch (batchErr) {
          console.error('❌ Batch insert failed, retrying individually:', batchErr.message);
          for (const rec of data) {
            try {
              await clickhouse.insert({
                table: 'providers',
                values: [rec],
                format: 'JSONEachRow',
              });
            } catch (rowErr) {
              console.error(`⚠️ Failed to insert providerAffiliate ID ${rec.id}:`, rowErr.message);
            }
          }
        }
      }

      offset += batchSize;
    }

    console.log('🎉 Provider affiliates migration completed!');
  } catch (err) {
    console.error('❌ Provider affiliates migration error:', err);
  }
}


async function migrateData() {
    // MySQL connection
    const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 
    const clickhouse = createClient({ url: 'http://localhost:8123', username: 'default', password: '', database: 'clickHouseInvoice', });
    try {
        await migrateProviderAffiliates(mysqlConn, clickhouse);
    }
    finally {
        await mysqlConn.end(); await clickhouse.close();
    }
}

migrateData();