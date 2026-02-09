import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

async function getLastMigratedId(clickhouse, tableName) {
   const result = await clickhouse.query({
    query: `SELECT last_migrated_id 
            FROM migration_progress 
            WHERE table_name = {table_name:String}
            order by updated_at desc 
            LIMIT 1`,
    format: 'JSONEachRow',
    query_params: { table_name: tableName }
  });

  const rows = await result.json();

  return rows.length ? rows[0].last_migrated_id : 0;
}

async function updateLastMigratedId(clickhouse, tableName, lastId, totalRecords) {
  
 if(totalRecords >0){await clickhouse.insert({
    table: 'migration_progress',
    values: [{
      table_name: tableName,
      last_migrated_id: lastId,
      updated_at: new Date().toISOString().slice(0, 19).replace("T"," ")
    }],
    format: 'JSONEachRow'
  });
}
  console.log("updated the last migrated id");
}

// async function getDistinctServiceProviders(mysqlConn) {
//   const [rows] = await mysqlConn.execute(`
//     SELECT DISTINCT serviceProviderId
//     FROM invoiceNew
//     WHERE status = 1
//   `);
//   return rows.map(r => r.serviceProviderId);
// }

async function createInvoiceTable(clickhouse, tableName) {
  const createQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName}
    (
        id UInt64,
    franchise String,
    franchise_id UInt64,
    provider String,
    provider_id UInt64,
    location String,
    location_id UInt64,
    invoice_id UInt64 NOT NULL,
    pos_terminal String,
    pos_terminal_id UInt64,
    payment_date Date,
    amount_paid Decimal(12,2),
    payment_method_id UInt64,
    payment_method String,
    refund_amount Decimal(12,2),
    notes String,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, invoice_id, id);
  `;

  await clickhouse.exec({ query: createQuery });
  console.log(`📦 Table ready: ${tableName}`);
}

async function migratePayments(mysqlConn, clickhouse, serviceProviderId, batchSize = 1000) {
  // --- Helpers ---
  function formatDate(dateValue) {
    const now = new Date();
    if (!dateValue) return now.toISOString().slice(0, 19).replace("T", " ");
    let date;
    try {
      if (dateValue instanceof Date) {
        date = dateValue;
      } else {
        const dateStr = String(dateValue).trim();
        if (dateStr.includes(" ") && !dateStr.includes("T")) {
          date = new Date(dateStr.replace(" ", "T") + "Z");
        } else {
          date = new Date(dateStr);
        }
      }
      if (isNaN(date.getTime())) return now.toISOString().slice(0, 19).replace("T", " ");
      const y = date.getFullYear();
      const m = String(date.getMonth() + 1).padStart(2, "0");
      const d = String(date.getDate()).padStart(2, "0");
      const h = String(date.getHours()).padStart(2, "0");
      const mi = String(date.getMinutes()).padStart(2, "0");
      const s = String(date.getSeconds()).padStart(2, "0");
      return `${y}-${m}-${d} ${h}:${mi}:${s}`;
    } catch {
      return now.toISOString().slice(0, 19).replace("T", " ");
    }
  }

  function formatDateOnly(dateValue) {
    const now = new Date();
    if (!dateValue) return now.toISOString().slice(0, 10);
    let date;
    try {
      if (dateValue instanceof Date) {
        date = dateValue;
      } else {
        date = new Date(String(dateValue).trim());
      }
      if (isNaN(date.getTime())) return now.toISOString().slice(0, 10);
      const y = date.getFullYear();
      const m = String(date.getMonth() + 1).padStart(2, "0");
      const d = String(date.getDate()).padStart(2, "0");
      return `${y}-${m}-${d}`;
    } catch {
      return now.toISOString().slice(0, 10);
    }
  }

  const safeNum = (v, def = 0) =>
    v === null || v === undefined || isNaN(Number(v)) ? def : Number(v);
  const safeStr = (v, def = "") =>
    v === null || v === undefined ? def : String(v);

  try {
        const TABLE_KEY = `paymentDetails_${serviceProviderId}`;
         let lastId = await getLastMigratedId(clickhouse, TABLE_KEY);
        console.log(`▶ Resuming migration from paymentItemNew.id > ${lastId}`);
    // Count total records
    const [countResult] = await mysqlConn.execute(
      `SELECT COUNT(*) as total FROM paymentItemNew  where paymentItemNew.serviceProviderId = ${serviceProviderId} and status=1 and paymentItemNew.id > ${lastId}`
    );
    const totalRecords = countResult[0].total;
    console.log(`Total payments to migrate: ${totalRecords}`);

    if (totalRecords === 0) {
      console.log("No payments found to migrate.");
      return;
    }
let offset = 0;
    let totalMigrated = 0;

    while (totalMigrated < totalRecords) {
      // ⚠️ Alias IDs to avoid collisions
      const [rows] = await mysqlConn.execute(
        `SELECT 
            paymentItemNew.id AS paymentId,
            paymentItemNew.*
         FROM paymentItemNew         
          where serviceProviderId =${serviceProviderId} && status=1 && paymentItemNew.id > ${lastId}
         ORDER BY id
         LIMIT ${batchSize}
        `
      );

      const data = [];
      for (const r of rows) {
        lastId = r.id;
        // Provider name
        let providerName = "Other";
        let locationId =0;
        if (r.invoiceId) {
          const [[prov]] = await mysqlConn.execute(
            "SELECT locationId as locationId FROM invoiceNew WHERE id = ?",
            [r.invoiceId]
          );
          locationId = prov?.locationId || "0";
        }

        if (r.serviceProviderId) {
          const [[prov]] = await mysqlConn.execute(
            "SELECT legalName as name FROM serviceProvider WHERE id = ?",
            [r.serviceProviderId]
          );
          providerName = prov?.name || "Other";
        }

        // Location name
        let locationName = "Other";
        if (locationId) {
          const [[loc]] = await mysqlConn.execute(
            "SELECT name FROM location WHERE id = ?",
            [locationId]
          );
          locationName = loc?.name || "Other";
        }

        // POS Terminal
        let posTerminalName = "Other";
        if (r.posTerminalId) {
          const [[posTerminal]] = await mysqlConn.execute(
            "SELECT name FROM posTerminal WHERE id = ?",
            [r.posTerminalId]
          );
          posTerminalName = posTerminal?.name || "Other";
        } else {
          // console.log(`posterminal id missing for payment id: ${r.paymentId}, ${(r.posTerminalId)}`);
          posTerminalName = "Other";
        }

        let paymentMethodName = "";
        if (r.paymentMethodId) {
          const [[paymentMethod]] = await mysqlConn.execute(
            "SELECT name FROM paymentMethod WHERE id = ?",
            [r.paymentMethodId]
          );
          paymentMethodName = paymentMethod?.name || "Other";
        }


        data.push({
          id: r.paymentId,
          franchise: "88 Tactical",
          provider: providerName,
          provider_id: r.serviceProviderId || 0,
          location: locationName,
          location_id: locationId || 0,
          invoice_id: r.invoiceId || 0,
          pos_terminal: posTerminalName,
          pos_terminal_id: r.posTerminalId || 0,
          franchise_id: 0, // default
          payment_method_id: safeStr(r.paymentMethodId),
          payment_method: safeStr(paymentMethodName),
          amount_paid: safeNum(r.amount),
          refund_amount: safeNum(r.refundAmount),
          reference_number: safeStr(r.code),
          notes: safeStr(r.notes),
          payment_date: formatDateOnly(r.date || new Date()),
          created_at: formatDate(r.createdAt || new Date()),
          updated_at: formatDate(r.updatedAt || new Date()),
        });
      }

      if (data.length > 0) {
        await clickhouse.insert({
          table: "paymentDetails",
          values: data,
          format: "JSONEachRow",
        });

        totalMigrated += data.length;
        console.log(
          ` Migrated batch: ${offset + 1} → ${offset + data.length} (total so far: ${totalMigrated})`
        );
      }

      offset += batchSize;
    }
    if(totalMigrated >0){
await updateLastMigratedId(clickhouse, TABLE_KEY, lastId, totalRecords);
console.log(`✔ Migrated up to ID: ${lastId}`);
    }
    console.log(`🎉 Payments migration completed. Total migrated: ${totalMigrated}`);
  } catch (err) {
    console.error("❌ Payments migration error:", err.message);
    console.error(err);
  }
}



async function migrateData() {

 const mysqlConn = await mysql.createConnection({ host: 'localhost', user: 'root', password: '', database: 'bizzflo', }); // ClickHouse connection 

    const [resultRows] = await mysqlConn.execute('SELECT NOW() AS now');
    console.log("DB Time:", resultRows[0].now);

  const clickhouse = createClient({
    url: 'http://localhost:8123',
    username: 'default',
    password: '',
    database: 'clickHouseInvoice',
  });

  try {
    //  const providerIds = await getDistinctServiceProviders(mysqlConn);
    //   console.log(`🔑 Found ${providerIds.length} service providers`);
  //  for (const providerId of providerIds) {
  const providerId = 22;
      const tableName = `paymentDetails_${providerId}`;
      // console.log(`\n🚀 Migrating provider ${providerId}`);

      await createInvoiceTable(clickhouse, tableName);
      await migratePayments(mysqlConn, clickhouse, providerId);
    // }
   
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();