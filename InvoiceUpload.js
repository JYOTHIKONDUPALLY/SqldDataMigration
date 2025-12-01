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

async function migrateInvoices(mysqlConn, clickhouse, batchSize = 2000) {
  try {
    console.log('🚀 Starting invoice migration from MySQL to ClickHouse...');

    const [countResult] = await mysqlConn.execute('SELECT COUNT(*) as total FROM invoiceNew ');
    const totalRecords = countResult[0].total;
    console.log(`📊 Total records to migrate: ${totalRecords}`);

    let offset = 0;
    let totalMigrated = 0;
    let totalErrors = 0;
    const errors = [];
    const franchise = 'dummy';
    const franchise_id = 0;

    while (offset < totalRecords) {
      console.log(`\n➡️ Fetching batch: OFFSET=${offset}, LIMIT=${batchSize}`);

      const [rows] = await mysqlConn.execute(`
        SELECT 
          id, serviceProviderId, invoiceNumber, customerId, customerMemberId,
          resourceId, loggedinUserId, locationId, invoiceDate, status, price, discount, 
          grandTotal, dueDate, posTerminalId, notes, parentInvoiceId, tax, bookingType
        FROM invoiceNew
      
        ORDER BY id
        LIMIT ? OFFSET ?
      `, [batchSize, offset]);

      console.log(`   🔹 Retrieved ${rows.length} rows from MySQL`);

      if (rows.length === 0) {
        console.log('✅ No more rows to process. Exiting loop.');
        break;
      }

      // 🔹 Collect unique IDs for batch lookups
      const providerIds = [...new Set(rows.map(r => r.serviceProviderId).filter(Boolean))];
      const resourceIds = [...new Set(rows.map(r => r.resourceId).filter(Boolean))];
      const locationIds = [...new Set(rows.map(r => r.locationId).filter(Boolean))];
      const customerIds = [...new Set(rows.map(r => r.customerId).filter(Boolean))];
      const posIds = [...new Set(rows.map(r => r.posTerminalId).filter(Boolean))];

      // 🔹 Query in bulk
      const [providers] = providerIds.length
        ? await mysqlConn.query(`SELECT id, legalName as name FROM serviceprovider WHERE id IN (?)`, [providerIds]) : [[]];
      const [resources] = resourceIds.length
        ? await mysqlConn.query(`SELECT id, CONCAT(firstName, ' ', middleName, ' ', lastName) AS name FROM resource WHERE id IN (?)`, [resourceIds]) : [[]];
      const [locations] = locationIds.length
        ? await mysqlConn.query(`SELECT id, name FROM location WHERE id IN (?)`, [locationIds]) : [[]];
      const [customers] = customerIds.length
        ? await mysqlConn.query(`SELECT id, CONCAT(firstName, ' ', middleName, ' ', lastName) AS name, email FROM customer WHERE id IN (?)`, [customerIds]) : [[]];
      const [companies] = providerIds.length && customerIds.length
  ? await mysqlConn.query(
      `SELECT serviceProviderId as id, company 
       FROM serviceprovidercustomerdetails 
       WHERE serviceProviderId IN (?) 
         AND customerId IN (?)`,
      [providerIds, customerIds]
    )
  : [[]];
      const [posTerminals] = posIds.length
        ? await mysqlConn.query(`SELECT id, name FROM posterminal WHERE id IN (?)`, [posIds]) : [[]];

      // 🔹 Index results by ID
      const providerMap = Object.fromEntries(providers.map(p => [p.id, p]));
      const resourceMap = Object.fromEntries(resources.map(r => [r.id, r]));
      const locationMap = Object.fromEntries(locations.map(l => [l.id, l]));
      const customerMap = Object.fromEntries(customers.map(c => [c.id, c]));
      const companyMap = Object.fromEntries(companies.map(c => [c.id, c]));
      const posMap = Object.fromEntries(posTerminals.map(p => [p.id, p]));
      const bookingMap = {
    1: 'online',
    0: 'online',
    2: 'phone_in',
    3: 'walk_in',
    4: 'mobile_app'
};

console.log("booking type", bookingMap[0]);

      // 🔹 Build processed data
      const processedData = rows.map(row => (
     {
        id: row.id,
        franchise_id,
        franchise,
        provider_id: row.serviceProviderId || 0,
        provider: providerMap[row.serviceProviderId]?.name || 'N/A',
        resource: resourceMap[row.resourceId]?.name || 'N/A',
        location_id: row.locationId || 0,
        location: locationMap[row.locationId]?.name || 'N/A',
        customer_id: row.customerId || 0,
        customer_name: customerMap[row.customerId]?.name || 'N/A',
        customer_email: customerMap[row.customerId]?.email || '',
        parent_invoice_id: row.parentInvoiceId || 0,
        invoice_number: String(row.invoiceNumber || ''),
        invoice_date: formatDateOnly(row.invoiceDate),
        due_date: formatDateOnly(row.dueDate),
        status: Number(row.status) || 0,
        member_id: row.customerMemberId || 0,
        company: companyMap[row.serviceProviderId, row.customerId]?.company || 'N/A',
        commission_clerk: resourceMap[row.resourceId]?.name || 'N/A',
        sales_clerk: resourceMap[row.loggedinUserId ]?.name || 0,
        pos_terminal: posMap[row.posTerminalId]?.name || 'N/A',
        pos_terminal_id: row.posTerminalId || 0,
        total_amount: Number(row.grandTotal) || 0.0,
        is_member: row.customerMemberId ? 'yes' : 'no',
        retail_discount: Number(row.discount) || 0.0,
        notes: String(row.notes || ''),
        tax: Number(row.tax) || 0.0,
        booking_type:  bookingMap[row.bookingType] || '',
        created_at: formatDate(row.invoiceDate),
        updated_at: formatDate(row.invoiceDate)
      }));

      console.log(`   📥 Prepared ${processedData.length} rows for ClickHouse insert`);

      // 🔹 Insert in sub-batches
      const CHUNK_SIZE = 500;
      for (let i = 0; i < processedData.length; i += CHUNK_SIZE) {
        const chunk = processedData.slice(i, i + CHUNK_SIZE);
        try {
          await clickhouse.insert({
            table: 'invoice_details',
            values: chunk,
            format: 'JSONEachRow'
          });
          console.log(`   ✅ Inserted sub-batch ${i + 1} → ${i + chunk.length}`);
          totalMigrated += chunk.length;
        } catch (insertError) {
          console.error(`   ❌ Sub-batch insert failed (rows ${i + 1} → ${i + chunk.length}):`, insertError.message);
          totalErrors += chunk.length;
          errors.push(insertError.message);
        }
      }

      offset += batchSize;
      console.log(`➡️ Progress: Migrated=${totalMigrated}, Errors=${totalErrors}`);
    }

    console.log(`\n🏁 Migration finished: ${totalMigrated} migrated, ${totalErrors} errors`);
    return { success: totalErrors === 0, totalRecords, migrated: totalMigrated, errors: totalErrors };
  } catch (err) {
    console.error('💥 Critical error in migrateInvoices:', err.message);
    return { success: false, error: err.message };
  }
}

async function migrateData() {
  const mysqlConn = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'bizzflo',
  });

  const clickhouse = createClient({
    url: 'http://localhost:8123',
    username: 'default',
    password: '',
    database: 'clickHouseInvoice',
  });

  try {
    await migrateInvoices(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();
