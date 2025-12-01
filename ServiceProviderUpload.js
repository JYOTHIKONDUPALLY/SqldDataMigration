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

/**
 * Migrate service providers in batches
 */
async function migrateServiceProvider(mysqlConn, clickhouse, batchSize = 2000) {
  try {
    console.log('Starting service provider migration...');

    // Fetch all providers with category info
    const [providers] = await mysqlConn.execute(`
      SELECT 
        p.id,
        p.serviceCategoryId,
        sc.name AS serviceCategoryName,
        p.legalName,
        p.creationDate,
        p.expiryDate
      FROM serviceprovider p
      LEFT JOIN servicecategory sc ON sc.id = p.serviceCategoryId
    `);

    console.log(`Fetched ${providers.length} service providers`);

    if (providers.length === 0) {
      console.log('No service providers to migrate');
      return { success: true, count: 0 };
    }

    // Get providers that have memberships
    const [membershipProviders] = await mysqlConn.execute(`
      SELECT DISTINCT serviceProviderId AS id FROM membership
    `);

    const membershipSet = new Set(membershipProviders.map(m => m.id));
    console.log(`Found ${membershipSet.size} providers with memberships`);

    // Transform data
    const rows = providers.map(p => ({
      id: p.id,
      serviceCategoryId: p.serviceCategoryId || 0,
      serviceCategoryName: p.serviceCategoryName || 'N/A',
      legalName: p.legalName || 'N/A',
      creationDate: formatDate(p.creationDate),
      expiryDate: formatDate(p.expiryDate),
      hasMembership: membershipSet.has(p.id) ? 'Yes' : 'No'
    }));

    // Insert in batches
    let insertedCount = 0;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      
      await clickhouse.insert({
        table: 'serviceprovider',
        values: batch,
        format: 'JSONEachRow'
      });

      insertedCount += batch.length;
      console.log(`✓ Inserted batch: ${insertedCount}/${rows.length} service providers`);
    }

    console.log(`✓ Service provider migration completed: ${insertedCount} records`);
    return { success: true, count: insertedCount };

  } catch (err) {
    console.error('✗ Service provider migration error:', err);
    return { success: false, error: err };
  }
}

/**
 * Main migration function
 */
async function migrateData() {
  let mysqlConn;
  let clickhouse;

  try {
    console.log('=== Starting Service Provider Migration ===\n');

    mysqlConn = await mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: '',
      database: 'bizzflo',
    });

    clickhouse = createClient({
      url: 'http://localhost:8123',
      username: 'default',
      password: '',
      database: 'clickHouseInvoice',
    });

    console.log('✓ Database connections established\n');

    const result = await migrateServiceProvider(mysqlConn, clickhouse);

    if (result.success) {
      console.log('\n=== Migration Completed Successfully ===');
    } else {
      console.error('\n=== Migration Failed ===');
      process.exit(1);
    }

  } catch (err) {
    console.error('\n✗ Migration failed:', err);
    process.exit(1);
  } finally {
    if (mysqlConn) await mysqlConn.end();
    if (clickhouse) await clickhouse.close();
    console.log('\n✓ Connections closed');
  }
}

migrateData();