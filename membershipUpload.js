import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

// ------------------------------
// DATE FORMAT UTILITIES
// ------------------------------
function formatDateTime(dateValue) {
  if (!dateValue) return '1970-01-01 00:00:00';

  let date;
  try {
    if (dateValue instanceof Date) {
      date = dateValue;
    } else {
      const val = String(dateValue).trim();
      date = new Date(val.includes(" ") && !val.includes("T") ? val.replace(" ", "T") + "Z" : val);
    }

    if (isNaN(date.getTime())) return null;

    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    const h = String(date.getHours()).padStart(2, "0");
    const mi = String(date.getMinutes()).padStart(2, "0");
    const s = String(date.getSeconds()).padStart(2, "0");
    return `${y}-${m}-${d} ${h}:${mi}:${s}`;
  } catch {
    return null;
  }
}

function formateDateOnly(dateValue) {
  if (!dateValue) return null;

  let date;
  try {
    date = dateValue instanceof Date ? dateValue : new Date(String(dateValue).trim());
    if (isNaN(date.getTime())) return null;

    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, "0");
    const d = String(date.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  } catch {
    return Null;
  }
}

// =====================================================================
// MIGRATE MEMBERSHIP ANALYTICS
// =====================================================================
async function migrateMembership(mysqlConn, clickhouse, batchSize = 2000) {
  let offset = 0;
  let total = 0;

  console.log(`🚀 Starting Membership Migration`);

  while (true) {
    const [rows] = await mysqlConn.execute(
      `
      SELECT 
      -- Primary Keys & IDs
          me.id AS enrollment_id,
          me.membershipId AS membership_id,
          me.customerId AS customer_id,
          me.memberId AS member_id,
          me.serviceProviderId AS service_provider_id,
          me.subscriptionId AS subscription_id,
          me.invoiceId AS invoice_id,
          me.originalInvoiceId AS parent_invoice_id,
          me.locationId AS location_id,
          m.franchiseId AS franchise_id,
-- Customer & Member Info
          CONCAT(IFNULL(c.firstName,''),' ',IFNULL(c.lastName,'')) AS customer_name,
          CONCAT(IFNULL(cm.firstName,''),' ',IFNULL(cm.lastName,'')) AS member_name,
          me.customerMemberId AS customer_member_id,
          me.primaryMember AS primary_member,
          me.parentEnrollmentId AS parent_enrollment_id,
-- Membership Details
          m.name AS membership_name,
          mt.type AS membership_type,
      m.type AS membership_type_id,
CASE 
    WHEN me.subscriptionId IS NULL THEN 0 
    ELSE 1 
END AS hasFull_membership,


          CASE 
              WHEN me.status = 1 AND (me.expirationDate IS NULL OR me.expirationDate >= CURDATE()) THEN 'Active'
              WHEN me.status = 1 AND me.expirationDate < CURDATE() THEN 'Expired'
              WHEN me.status = 2 THEN 'Cancelled'
              WHEN me.status = 3 THEN 'Expired'
              ELSE 'Unknown'
          END AS membership_status,

          CASE 
              WHEN me.status = 1 THEN 'Active'
              WHEN me.status = 2 THEN 'Cancelled'
              WHEN me.status = 3 THEN 'Expired'
              ELSE 'Unknown'
          END AS enrollment_status,

          m.onlineVisible AS online_visible,

          CASE 
              WHEN s.subscriptionType = 0 THEN 'Monthly'
              WHEN s.subscriptionType = 1 THEN 'Quarterly'
              WHEN s.subscriptionType = 2 THEN 'Yearly'
              WHEN s.subscriptionType = 3 THEN 'Contract'
              ELSE 'Unknown'
          END AS subscription_type,

          CASE
              WHEN s.status = 1 AND s.flag = 0 THEN 'Current'
              WHEN s.status = 1 AND s.flag = 1 THEN 'Pending'
              WHEN s.status IN (3,6,11) THEN 'OnHold'
              WHEN s.status = 10 THEN 'PaymentHeight'
              WHEN s.status = 7 THEN 'Frozen'
              ELSE 'Unknown'
          END AS subscription_status,

          CASE WHEN s.subscriptionType IN (1,3) THEN 1 ELSE 0 END AS auto_renew,
          s.paymentMethod AS payment_method,

          me.creationDate AS enrollment_date,
          me.startDate AS start_date,
          me.contractDurationDate AS contract_duration_date,
          me.expirationDate AS expiration_date,
          s.renewalDate AS next_renewal_date,
          s.nextBillingDate AS next_billing_date,
           me.renewalDate AS renewal_date,
           s.firstRenewalDate AS first_renewal_date,
      s.renewalNotificationDate AS renewal_notification_date,
      s.cancellationDate AS cancellation_date,
      me.deletedOn AS deleted_date,
      (
    SELECT inv.lastUpdated
    FROM subscriptioninvoice si
    INNER JOIN invoicenew inv ON si.invoiceId = inv.id
    WHERE si.subscriptionId = me.subscriptionId
    ORDER BY si.id DESC
    LIMIT 1
) AS last_payment_date,

(
    SELECT inv.grandTotal
    FROM subscriptioninvoice si
    INNER JOIN invoicenew inv ON si.invoiceId = inv.id
    WHERE si.subscriptionId = me.subscriptionId
    ORDER BY si.id DESC
    LIMIT 1
) AS last_payment_amount,

(
    SELECT 
        CASE 
            WHEN inv.outstandingBalance = 0 
                 AND s.nextBillingDate > CURDATE() 
                THEN 'UPTODATE'
            WHEN inv.outstandingBalance > 0 AND s.flag = 0
                THEN 'FAILED'
            ELSE 'PENDING'
        END
    FROM subscriptioninvoice si
    INNER JOIN invoicenew inv ON si.invoiceId = inv.id
    WHERE si.subscriptionId = me.subscriptionId
    ORDER BY si.id DESC
    LIMIT 1
) AS last_payment_status,

          -- Membership Pricing
          m.price AS membership_price,
          s.recurringAmount AS recurring_amount,
          s.amount AS subscription_amount,
          m.registrationFee AS registration_fee,
          (IFNULL(m.price,0) + IFNULL(m.registrationFee,0)) AS total_amount,
          -- Membership Configuration
      m.durationCount AS duration_count,
      m.duration AS duration_type,
      m.renewalType AS renewal_type,
      m.noOfMembersIncluded AS no_of_members_included,
      m.noOfAdditionalMembers AS no_of_additional_members,
      
      -- Auto-Renewal Tracking
      s.autoException AS auto_exception,
      s.declinedCount AS declined_count,
      s.noOfPayments AS no_of_payments,
      s.paymentDay AS payment_day,
      
      -- Cancellation Info
      s.cancelReason AS cancel_reason,
      s.cancelNotes AS cancel_notes,
      s.deletedBy AS cancelled_by,
      me.deletedBy AS deleted_by,
      
      -- Calculated Metrics
      DATEDIFF(me.expirationDate, CURDATE()) AS days_to_expiration,
      CASE WHEN me.status = 1 AND (me.expirationDate IS NULL OR me.expirationDate >= CURDATE()) THEN 1 ELSE 0 END AS is_active,
      CASE WHEN me.expirationDate BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 30 DAY) THEN 1 ELSE 0 END AS is_expiring_30days,
      CASE WHEN me.expirationDate BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 60 DAY) THEN 1 ELSE 0 END AS is_expiring_60days,
      CASE WHEN me.status = 1 AND me.expirationDate < CURDATE() THEN 1 ELSE 0 END AS is_lapsed,
      CASE WHEN s.declinedCount > 0 THEN 1 ELSE 0 END AS is_auto_renew_failed,
      CASE WHEN s.cancellationDate IS NOT NULL THEN DATEDIFF(CURDATE(), s.cancellationDate) ELSE NULL END AS days_since_cancellation,
      
      -- Categorization
      me.bookingMethod AS booking_method,
      m.departmentId AS department_id

      FROM membershipenrollment me
      INNER JOIN membership m ON me.membershipId = m.id
      LEFT JOIN membershiptype mt ON m.type = mt.id
      LEFT JOIN subscriptionsnew s ON me.subscriptionId = s.id
      LEFT JOIN customer c ON me.customerId = c.id
      LEFT JOIN customer cm ON me.memberId = cm.id

      ORDER BY me.id
      LIMIT ? OFFSET ?
      `,
      [batchSize, offset]
    );

    if (rows.length === 0) break;

    const cleanedRows = rows.map(row => ({
      // Primary Keys & IDs
    enrollment_id: row.enrollment_id,
    membership_id: row.membership_id,
    customer_id: row.customer_id,
    member_id: row.member_id,
    service_provider_id: row.service_provider_id,
    subscription_id: row.subscription_id || null,
    invoice_id: row.invoice_id || null,
    parent_invoice_id: row.parent_invoice_id || null,
    location_id: row.location_id || null,
    franchise_id: row.franchise_id || null,
    
    // Customer & Member Info
    customer_name: row.customer_name || '',
    member_name: row.member_name || '',
    customer_member_id: row.customer_member_id || null,
    primary_member: row.primary_member || 0,
    parent_enrollment_id: row.parent_enrollment_id || null,
    hasFull_membership: row.hasFull_membership ? "Yes" : "No",
    
    // Membership Details
    membership_name: row.membership_name || '',
    membership_type: row.membership_type || '',
    membership_type_id: row.membership_type_id || 0,
    membership_status: row.membership_status || 'Unknown',
    enrollment_status: row.enrollment_status || 0,
    online_visible: row.online_visible || 0,
    
    // Subscription Details
    subscription_type: row.subscription_type || 0,
    subscription_status: row.subscription_status || 'Unknown',
    auto_renew: row.auto_renew || 0,
    payment_method: row.payment_method || 0,
    
    // Dates - Format for ClickHouse DateTime/Date
    enrollment_date: formatDateTime(row.enrollment_date),
    start_date: formateDateOnly(row.start_date),
    contract_duration_date: formateDateOnly(row.contract_duration_date),
    expiration_date: formateDateOnly(row.expiration_date),
    next_billing_date: formateDateOnly(row.next_billing_date),
    renewal_date: formateDateOnly(row.renewal_date),
    first_renewal_date: formateDateOnly(row.first_renewal_date),
    next_renewal_date: formateDateOnly(row.next_renewal_date),
    renewal_notification_date: formateDateOnly(row.renewal_notification_date),
    cancellation_date: formatDateTime(row.cancellation_date),
    deleted_date: formatDateTime(row.deleted_date),
    
    // Financial Data
    membership_price: parseFloat(row.membership_price) || 0.00,
    recurring_amount: parseFloat(row.recurring_amount) || 0.00,
    subscription_amount: parseFloat(row.subscription_amount) || 0.00,
    registration_fee: parseFloat(row.registration_fee) || 0.00,
    total_amount: parseFloat(row.total_amount) || 0.00,
    last_payment_date: formateDateOnly(row.last_payment_date),
    last_payment_amount: parseFloat(row.last_payment_amount) || 0.00,
    last_payment_status: row.last_payment_status ,
    
    // Membership Configuration
    duration_count: row.duration_count || 0,
    duration_type: row.duration_type || 0,
    renewal_type: row.renewal_type || 0,
    no_of_members_included: row.no_of_members_included || 0,
    no_of_additional_members: row.no_of_additional_members || 0,
    
    // Auto-Renewal Tracking
    auto_exception: row.auto_exception || 0,
    declined_count: row.declined_count || 0,
    no_of_payments: row.no_of_payments || null,
    payment_day: row.payment_day || null,
    
    // Cancellation Info
    cancel_reason: row.cancel_reason || null,
    cancel_notes: row.cancel_notes || null,
    cancelled_by: row.cancelled_by || null,
    deleted_by: row.deleted_by || null,
    
    // Calculated Metrics
    days_to_expiration: row.days_to_expiration || 0,
    is_active: row.is_active || 0,
    is_expiring_30days: row.is_expiring_30days || 0,
    is_expiring_60days: row.is_expiring_60days || 0,
    is_lapsed: row.is_lapsed || 0,
    is_auto_renew_failed: row.is_auto_renew_failed || 0,
    days_since_cancellation: row.days_since_cancellation || null,
    
    // Categorization
    booking_method: row.booking_method || 0,
    department_id: row.department_id || null,
  }));
  console.log(`last payment status - ${rows[0].last_payment_status}`);

    await clickhouse.insert({
      table: 'memberships',
      values: cleanedRows,
      format: 'JSONEachRow',
    });

    total += rows.length;
    offset += batchSize;

    console.log(`➡️ Migrated ${total} rows...`);
  }

  console.log(`✅ Migration Completed | Total Rows: ${total}`);
}

// =====================================================================
// MAIN RUNNER
// =====================================================================
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
    await migrateMembership(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();
