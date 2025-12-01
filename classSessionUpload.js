import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

const CONFIG = {
  serviceProviderId: process.env.SERVICE_PROVIDER_ID || null,
  dateFrom: process.env.DATE_FROM || null,
  dateTo: process.env.DATE_TO || null,
  batchSize: 2000
};


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
 * MIGRATE CLASS SESSIONS
 */
export async function migrateClassSession(mysqlConnection, clickhouseClient, batchSize = 2000) {

  function buildMySQLQuery() {
    let whereClause = 'WHERE ces.status = 1 AND c.status > 0';

    if (CONFIG.serviceProviderId) {
      whereClause += ` AND c.serviceProviderId = ${CONFIG.serviceProviderId}`;
    }
    if (CONFIG.dateFrom) {
      whereClause += ` AND cs.date >= '${CONFIG.dateFrom}'`;
    }
    if (CONFIG.dateTo) {
      whereClause += ` AND cs.date <= '${CONFIG.dateTo}'`;
    }

    const sqlQuery = `
      SELECT
        -- Primary Keys
        cs.id AS session_id,
        c.id AS class_id,
        ce.id AS enrollment_id,
        ces.id AS enrollment_session_id,
        ce.customerId AS customer_id,
        ce.invoiceId AS invoice_id,
        
        -- Service Provider & Location
        c.serviceProviderId AS service_provider_id,
        COALESCE(ce.locationId, 0) AS location_id,
        COALESCE(l.name, '') AS location_name,
        
        -- Class Information
        c.name AS class_name,
        c.classTypeId AS class_type_id,
        COALESCE(ct.name, '') AS class_type_name,
        c.classCategoryId AS class_category_id,
        COALESCE(cc.name, '') AS class_category_name,
        c.attendies AS class_capacity,
        c.status AS class_status,
      CASE 
    WHEN COALESCE(c.enrollmentStatus, 1) = 1 THEN 'Enrollment Open'
    WHEN COALESCE(c.enrollmentStatus, 1) = 2 THEN 'Do Not Publish'
    WHEN COALESCE(c.enrollmentStatus, 1) = 0 THEN 'Enrollment Closed'
    ELSE 'Unknown'
END AS class_enrollment_status,
        
        -- Session Information
        cs.name AS session_name,
        cs.date AS session_date,
        COALESCE(cs.startTime, '') AS session_start_time,
        COALESCE(cs.endTime, '') AS session_end_time,
        COALESCE(cs.duration, '') AS session_duration,
         CASE 
    WHEN cs.status = 1 THEN 'Active'
    WHEN cs.status= 0 THEN 'deleted'
    ELSE 'Unknown'
END AS session_status,
        IF(COALESCE(cs.parentId, 0) = 0, 1, 0) AS is_parent_session,
        
        -- Instructor/Resource Information
        COALESCE(cs.resourceId, 0) AS resource_id,
        CONCAT(COALESCE(r.firstName, ''), ' ', COALESCE(r.lastName, '')) AS resource_name,
        COALESCE(cs.additionalResourceId, '') AS additional_resource_ids,
        
        -- Customer/Member Information
        COALESCE(ce.customerMemberId, 0) AS customer_member_id,
        IF(ce.customerMemberId > 0, 
           CONCAT(COALESCE(cm.firstName, ''), ' ', COALESCE(cm.lastName, '')), 
           'Self') AS member_name,
        CONCAT(COALESCE(spcd.firstName, ''), ' ', COALESCE(spcd.lastName, '')) AS customer_name,
        
        -- Enrollment Details
        CASE 
    WHEN ce.status = 1 THEN 'Enrolled'
    WHEN ce.status = 3 THEN 'Waiting List'
    WHEN ce.status= 0 THEN 'deleted'
    ELSE 'Unknown'
END AS enrollment_status,
        COALESCE(ce.quantity, 1) AS enrollment_quantity,
        ce.creationDate AS enrollment_creation_date,
         CASE 
    WHEN COALESCE(inv.bookingType, 1) = 1 THEN 'Online'
    WHEN COALESCE(inv.bookingType, 1) = 3 THEN 'Walk_in'
    WHEN COALESCE(inv.bookingType, 1)= 0 THEN 'Online'
    WHEN COALESCE(inv.bookingType, 1)= 2 THEN 'phone_in'
    WHEN COALESCE(inv.bookingType, 1)= 4 THEN 'Mobile_app'
    ELSE 'Unknown'
END AS booking_method,
        COALESCE(ce.payment, 0) AS payment_status,
        COALESCE(ce.paymentId, 0) AS payment_type_id,
        COALESCE(ces.checkedin, 0) AS is_checked_in,
        
        -- Financial Information from invoiceitemnew
        COALESCE(iin.price, 0) AS item_price,
        COALESCE(iin.qty, 0) AS quantity,
        COALESCE(iin.totalPrice, 0) AS total_price,
        COALESCE(iin.discount, 0) AS discount_amount,
        COALESCE(iin.tax, 0) AS tax_amount,
        COALESCE(iin.totalPrice - iin.discount, 0) AS net_amount,
        
        -- Promotion Information (not available in new schema)
        0 AS promotion_id,
        '' AS promotion_name,
        
        -- Timestamps from invoicenew
        COALESCE(inv.invoiceDate, ce.creationDate) AS invoice_created_at
        
      FROM classEnrollmentSessions ces
      INNER JOIN classEnrollment ce ON ces.classEnrollmentId = ce.id
      INNER JOIN class c ON ce.classId = c.id
      INNER JOIN classSession cs ON ces.sessionId = cs.id
      LEFT JOIN classType ct ON c.classTypeId = ct.id
      LEFT JOIN classCategory cc ON c.classCategoryId = cc.id
      LEFT JOIN resource r ON cs.resourceId = r.id
      LEFT JOIN location l ON ce.locationId = l.id
      LEFT JOIN serviceProviderCustomerDetails spcd 
        ON ce.customerId = spcd.customerId 
        AND c.serviceProviderId = spcd.serviceProviderId
      LEFT JOIN customerMembers cm ON ce.customerMemberId = cm.id
      LEFT JOIN invoicenew inv ON ce.invoiceId = inv.id
      LEFT JOIN invoiceitemnew iin 
        ON ce.invoiceId = iin.invoiceId 
        AND c.id = iin.itemId 
        AND iin.type = 'class'
      ${whereClause}
      ORDER BY cs.date DESC, cs.id DESC
    `;
    
    return sqlQuery;
  }

  function mapRowToClickHouseFormat(row) {
    return {
      session_id: row.session_id || 0,
      class_id: row.class_id || 0,
      enrollment_id: row.enrollment_id || 0,
      enrollment_session_id: row.enrollment_session_id || 0,
      customer_id: row.customer_id || 0,
      invoice_id: row.invoice_id || 0,
      service_provider_id: row.service_provider_id || 0,
      location_id: row.location_id || 0,
      location_name: row.location_name || '',
      class_name: row.class_name || '',
      class_type_id: row.class_type_id || 0,
      class_type_name: row.class_type_name || '',
      class_category_id: row.class_category_id || 0,
      class_category_name: row.class_category_name || '',
      class_capacity: row.class_capacity || 0,
      class_status: row.class_status || 0,
      class_enrollment_status: row.class_enrollment_status || 1,
      session_name: row.session_name || '',
      session_date: formatDateOnly(row.session_date),
      session_start_time: row.session_start_time || '',
      session_end_time: row.session_end_time || '',
      session_duration: row.session_duration || '',
      session_status: row.session_status || 1,
      is_parent_session: row.is_parent_session || 0,
      resource_id: row.resource_id || 0,
      resource_name: row.resource_name?.trim() || '',
      additional_resource_ids: row.additional_resource_ids || '',
      customer_member_id: row.customer_member_id || 0,
      member_name: row.member_name?.trim() || 'Self',
      customer_name: row.customer_name?.trim() || '',
      enrollment_status: row.enrollment_status || 0,
      enrollment_quantity: row.enrollment_quantity || 1,
      enrollment_creation_date: formatDate(row.enrollment_creation_date),
      booking_method: row.booking_method || 0,
      payment_status: row.payment_status || 0,
      payment_type_id: row.payment_type_id || 0,
      is_checked_in: row.is_checked_in || 0,
      item_price: parseFloat(row.item_price) || 0,
      quantity: row.quantity || 0,
      total_price: parseFloat(row.total_price) || 0,
      sale_discount: 0, // Not available in new schema
      discount_amount: parseFloat(row.discount_amount) || 0,
      tax_amount: parseFloat(row.tax_amount) || 0,
      net_amount: parseFloat(row.net_amount) || 0,
      promotion_id: row.promotion_id || 0,
      promotion_name: row.promotion_name || '',
      invoice_created_at: formatDate(row.invoice_created_at ),
      created_at: formatDate(new Date()),
      updated_at: formatDate(new Date()),
    };
  }

  async function insertBatch(batch) {
    if (!batch.length) return;

    await clickhouseClient.insert({
      table: "class_sessions",
      values: batch,
      format: "JSONEachRow",
    });
  }

  console.log("⏳ Fetching MySQL data ...");
  const [rows] = await mysqlConnection.query(buildMySQLQuery());

  console.log(`📦 ${rows.length} rows fetched. Migrating...`);

  let batch = [];
  let inserted = 0;

  for (const row of rows) {
    batch.push(mapRowToClickHouseFormat(row));

    if (batch.length >= batchSize) {
      await insertBatch(batch);
      inserted += batch.length;
      batch = [];
      console.log(`✔ Inserted ${inserted}`);
    }
  }

  if (batch.length > 0) {
    await insertBatch(batch);
    inserted += batch.length;
  }

  console.log(`\n🎉 Migration completed. Total inserted: ${inserted}`);
}

/**
 * WRAPPER FUNCTION
 */
async function migrateData() {
  const mysqlConn = await mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "bizzflo",
  });

  const clickhouse = createClient({
    url: "http://localhost:8123",
    username: "default",
    password: "",
    database: "clickHouseInvoice",
  });

  try {
    await migrateClassSession(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();