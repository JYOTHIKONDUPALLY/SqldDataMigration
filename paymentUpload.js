import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

async function migratePayments(mysqlConn, clickhouse, batchSize = 1000) {
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
    // Count total records
    const [countResult] = await mysqlConn.execute(
      `SELECT COUNT(*) as total FROM paymentItemNew  where paymentItemNew.serviceProviderId =2087`
    );
    const totalRecords = countResult[0].total;
    console.log(`Total payments to migrate: ${totalRecords}`);

    if (totalRecords === 0) {
      console.log("No payments found to migrate.");
      return;
    }

    let offset = 0;
    let totalMigrated = 0;

    while (offset < totalRecords) {
      // ⚠️ Alias IDs to avoid collisions
      const [rows] = await mysqlConn.execute(
        `SELECT 
            paymentItemNew.id AS paymentId,
            paymentItemNew.*,
            invoiceNew.serviceProviderId,
            invoiceNew.locationId,
            invoiceNew.posTerminalId
         FROM paymentItemNew
         LEFT JOIN invoiceNew ON paymentItemNew.invoiceId = invoiceNew.id
          where paymentItemNew.serviceProviderId =2087
         ORDER BY paymentItemNew.id
         LIMIT ${batchSize} OFFSET ${offset}
        `
      );

      const data = [];
      for (const r of rows) {
        // Provider name
        let providerName = "N/A";
        if (r.serviceProviderId) {
          const [[prov]] = await mysqlConn.execute(
            "SELECT legalName as name FROM serviceProvider WHERE id = ?",
            [r.serviceProviderId]
          );
          providerName = prov?.name || "N/A";
        }

        // Location name
        let locationName = "N/A";
        if (r.locationId) {
          const [[loc]] = await mysqlConn.execute(
            "SELECT name FROM location WHERE id = ?",
            [r.locationId]
          );
          locationName = loc?.name || "N/A";
        }

        // POS Terminal
        let posTerminalName = "N/A";
        if (r.posTerminalId) {
          const [[posTerminal]] = await mysqlConn.execute(
            "SELECT name FROM posTerminal WHERE id = ?",
            [r.posTerminalId]
          );
          posTerminalName = posTerminal?.name || "N/A";
        } else {
          // console.log(`posterminal id missing for payment id: ${r.paymentId}, ${(r.posTerminalId)}`);
          posTerminalName = "N/A";
        }

        data.push({
          id: r.paymentId,
          franchise: "dummy",
          provider: providerName,
          provider_id: r.serviceProviderId || 0,
          location: locationName,
          location_id: r.locationId || 0,
          invoice_id: r.invoiceId || 0,
          pos_terminal: posTerminalName,
          pos_terminal_id: r.posTerminalId || 0,
          franchise_id: 0, // default
          payment_method: safeStr(r.paymentMethodId),
          amount_paid: safeNum(r.amount),
          refund_amount: safeNum(r.refundAmount),
          reference_number: safeStr(r.code),
          notes: safeStr(r.notes),
          payment_date: formatDateOnly(r.paymentDate || new Date()),
          created_at: formatDate(r.createdAt || new Date()),
          updated_at: formatDate(r.updatedAt || new Date()),
        });
      }

      if (data.length > 0) {
        if (offset === 0) {
          console.log("Sample record for ClickHouse insert:");
          console.log(JSON.stringify(data[0], null, 2));
        }

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

    console.log(`🎉 Payments migration completed. Total migrated: ${totalMigrated}`);
  } catch (err) {
    console.error("❌ Payments migration error:", err.message);
    console.error(err);
  }
}



async function migrateData() {

  const mysqlConn = await mysql.createConnection({
    host: 'bizzflo-production-aurora3-cluster.cluster-ro-cs3e3cx0hfys.us-west-2.rds.amazonaws.com',   // or your DB host
    user: 'bizzflo',        // your DB username
    password: 'my5qlskeedazz!!',// your DB password
    database: 'bizzflo'   // your DB name
  });

  const clickhouse = createClient({
    url: 'http://localhost:8123',
    username: 'default',
    password: '',
    database: 'clickHouseInvoice',
  });

  try {
    await migratePayments(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();