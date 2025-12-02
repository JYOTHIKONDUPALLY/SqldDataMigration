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

  // Helper function to create placeholders for IN clause
  const createPlaceholders = (arr) => arr.map(() => '?').join(',');

  while (true) {
    // Fetch enrollment data only
    const [enrollments] = await mysqlConn.execute(
      `SELECT * FROM membershipEnrollment 
       WHERE serviceProviderId = 22
       ORDER BY id
       LIMIT ? OFFSET ?`,
      [batchSize, offset]
    );

    if (enrollments.length === 0) break;

    console.log(`Fetched ${enrollments.length} enrollments from MySQL`);

    // Extract unique IDs for batch queries
    const membershipIds = [...new Set(enrollments.map(e => e.membershipId).filter(id => id != null))];
    const subscriptionIds = [...new Set(enrollments.map(e => e.subscriptionId).filter(id => id != null))];
    const customerIds = [...new Set(enrollments.map(e => e.customerId).filter(id => id != null))];
    const memberIds = [...new Set(enrollments.map(e => e.memberId).filter(id => id != null))];

    // Fetch all related data in parallel
    const [memberships, membershipTypes, subscriptions, customers, members] = await Promise.all([
      // Memberships
      membershipIds.length > 0
        ? mysqlConn.execute(
            `SELECT id, name, type, franchiseId, onlineVisible, price, registrationFee, 
                    durationCount, duration, renewalType, noOfMembersIncluded, 
                    noOfAdditionalMembers, departmentId 
             FROM membership 
             WHERE id IN (${createPlaceholders(membershipIds)})`,
            membershipIds
          ).then(([rows]) => rows)
        : [],

      // Membership Types (fetch all unique types from memberships)
      membershipIds.length > 0
        ? mysqlConn.execute(
            `SELECT DISTINCT mt.id, mt.type 
             FROM membershipType mt
             INNER JOIN membership m ON mt.id = m.type
             WHERE m.id IN (${createPlaceholders(membershipIds)})`,
            membershipIds
          ).then(([rows]) => rows)
        : [],

      // Subscriptions
      subscriptionIds.length > 0
        ? mysqlConn.execute(
            `SELECT id, subscriptionType, status, flag, autoException, declinedCount, 
                    noOfPayments, paymentDay, paymentMethod, renewalDate, nextBillingDate, 
                    firstRenewalDate, renewalNotificationDate, cancellationDate, 
                    recurringAmount, amount, cancelReason, cancelNotes
             FROM subscriptionsNew 
             WHERE id IN (${createPlaceholders(subscriptionIds)})`,
            subscriptionIds
          ).then(([rows]) => rows)
        : [],

      // Customers
      customerIds.length > 0
        ? mysqlConn.execute(
            `SELECT id, CONCAT(IFNULL(firstName,''), ' ', IFNULL(lastName,'')) as name 
             FROM customer 
             WHERE id IN (${createPlaceholders(customerIds)})`,
            customerIds
          ).then(([rows]) => rows)
        : [],

      // Members
      memberIds.length > 0
        ? mysqlConn.execute(
            `SELECT id, CONCAT(IFNULL(firstName,''), ' ', IFNULL(lastName,'')) as name 
             FROM customer 
             WHERE id IN (${createPlaceholders(memberIds)})`,
            memberIds
          ).then(([rows]) => rows)
        : []
    ]);

    console.log(`Fetched related data: ${memberships.length} memberships, ${subscriptions.length} subscriptions, ${customers.length} customers`);

    // Fetch subscription invoices for subscriptions
    let subscriptionInvoices = [];
    if (subscriptionIds.length > 0) {
      try {
        const [invoices] = await mysqlConn.execute(
          `SELECT si.subscriptionId, inv.lastUpdated, inv.grandTotal, inv.outstandingBalance, si.id as si_id
           FROM subscriptionInvoice si
           INNER JOIN invoiceNew inv ON si.invoiceId = inv.id
           WHERE si.subscriptionId IN (${createPlaceholders(subscriptionIds)})
           ORDER BY si.subscriptionId, si.id DESC`,
          subscriptionIds
        );
        subscriptionInvoices = invoices;
        console.log(`Fetched ${subscriptionInvoices.length} subscription invoices`);
      } catch (error) {
        console.log('No subscription invoices found or error fetching:', error.message);
      }
    }

    // Create lookup maps
    const membershipMap = Object.fromEntries(memberships.map(m => [m.id, m]));
    const membershipTypeMap = Object.fromEntries(membershipTypes.map(mt => [mt.id, mt]));
    const subscriptionMap = Object.fromEntries(subscriptions.map(s => [s.id, s]));
    const customerMap = Object.fromEntries(customers.map(c => [c.id, c]));
    const memberMap = Object.fromEntries(members.map(m => [m.id, m]));

    // Group invoices by subscription (keep only the latest)
    const latestInvoiceMap = {};
    subscriptionInvoices.forEach(inv => {
      if (!latestInvoiceMap[inv.subscriptionId]) {
        latestInvoiceMap[inv.subscriptionId] = inv;
      }
    });

    // Transform data
    const cleanedRows = enrollments.map(me => {
      const membership = membershipMap[me.membershipId] || {};
      const membershipType = membershipTypeMap[membership.type] || {};
      const subscription = subscriptionMap[me.subscriptionId] || {};
      const customer = customerMap[me.customerId] || {};
      const member = memberMap[me.memberId] || {};
      const latestInvoice = latestInvoiceMap[me.subscriptionId] || {};

      // Calculate membership status
      const currentDate = new Date();
      const expirationDate = me.expirationDate ? new Date(me.expirationDate) : null;
      
      let membershipStatus = 'Unknown';
      if (me.status === 1 && (!expirationDate || expirationDate >= currentDate)) {
        membershipStatus = 'Active';
      } else if (me.status === 1 && expirationDate < currentDate) {
        membershipStatus = 'Expired';
      } else if (me.status === 2) {
        membershipStatus = 'Cancelled';
      } else if (me.status === 3) {
        membershipStatus = 'Expired';
      }

      let enrollmentStatus = 'Unknown';
      if (me.status === 1) enrollmentStatus = 'Active';
      else if (me.status === 2) enrollmentStatus = 'Cancelled';
      else if (me.status === 3) enrollmentStatus = 'Expired';

      // Subscription type mapping
      const subscriptionTypeMap = {
        0: 'Monthly',
        1: 'Quarterly',
        2: 'Yearly',
        3: 'Contract'
      };

      // Subscription status logic
      let subscriptionStatus = 'Unknown';
      console.log("subscription.status", subscription.status, subscription.flag);
      if (subscription.status === 1 && subscription.flag === 0) subscriptionStatus = 'Current';
      else if (subscription.status === 1 && subscription.flag === 1) subscriptionStatus = 'Pending';
      else if ([3, 6, 11].includes(subscription.status)) subscriptionStatus = 'OnHold';
      else if (subscription.status === 10) subscriptionStatus = 'PaymentHeight';
      else if (subscription.status === 7) subscriptionStatus = 'Frozen';

      // Last payment status
      let lastPaymentStatus = null;
      if (latestInvoice.subscriptionId) {
        const nextBillingDate = subscription.nextBillingDate ? new Date(subscription.nextBillingDate) : null;
        if (latestInvoice.outstandingBalance === 0 && nextBillingDate && nextBillingDate > currentDate) {
          lastPaymentStatus = 'UPTODATE';
        } else if (latestInvoice.outstandingBalance > 0 && subscription.flag === 0) {
          lastPaymentStatus = 'FAILED';
        } else {
          lastPaymentStatus = 'PENDING';
        }
      }

      // Calculate metrics
      const daysToExpiration = expirationDate ? Math.floor((expirationDate - currentDate) / (1000 * 60 * 60 * 24)) : 0;
      const isActive = (me.status === 1 && (!expirationDate || expirationDate >= currentDate)) ? 1 : 0;
      const isExpiring30days = (expirationDate && expirationDate >= currentDate && expirationDate <= new Date(currentDate.getTime() + 30 * 24 * 60 * 60 * 1000)) ? 1 : 0;
      const isExpiring60days = (expirationDate && expirationDate >= currentDate && expirationDate <= new Date(currentDate.getTime() + 60 * 24 * 60 * 60 * 1000)) ? 1 : 0;
      const isLapsed = (me.status === 1 && expirationDate && expirationDate < currentDate) ? 1 : 0;
      const isAutoRenewFailed = (subscription.declinedCount > 0) ? 1 : 0;
      
      const cancellationDate = subscription.cancellationDate ? new Date(subscription.cancellationDate) : null;
      const daysSinceCancellation = cancellationDate ? Math.floor((currentDate - cancellationDate) / (1000 * 60 * 60 * 24)) : null;

      return {
        // Primary Keys & IDs
        enrollment_id: me.id,
        membership_id: me.membershipId,
        customer_id: me.customerId,
        member_id: me.memberId,
        service_provider_id: me.serviceProviderId,
        subscription_id: me.subscriptionId || null,
        invoice_id: me.invoiceId || null,
        parent_invoice_id: me.originalInvoiceId || null,
        location_id: me.locationId || null,
        franchise_id: membership.franchiseId || null,
        
        // Customer & Member Info
        customer_name: customer.name || '',
        member_name: member.name || '',
        customer_member_id: me.customerMemberId || null,
        primary_member: me.primaryMember || 0,
        parent_enrollment_id: me.parentEnrollmentId || null,
        hasFull_membership: me.subscriptionId ? "No" : "Yes",
        
        // Membership Details
        membership_name: membership.name || '',
        membership_type: membershipType.type || '',
        membership_type_id: membership.type || 0,
        membership_status: membershipStatus,
        enrollment_status: enrollmentStatus,
        online_visible: membership.onlineVisible || 0,
        
        // Subscription Details
        subscription_type: subscriptionTypeMap[subscription.subscriptionType] || 'Unknown',
        subscription_status: subscriptionStatus,
        auto_renew: ([1, 3].includes(subscription.subscriptionType)) ? 1 : 0,
        payment_method: subscription.paymentMethod || 0,
        
        // Dates - Format for ClickHouse DateTime/Date
        enrollment_date: formatDateTime(me.creationDate),
        start_date: formateDateOnly(me.startDate),
        contract_duration_date: formateDateOnly(me.contractDurationDate),
        expiration_date: formateDateOnly(me.expirationDate),
        next_billing_date: formateDateOnly(subscription.nextBillingDate),
        renewal_date: formateDateOnly(me.renewalDate),
        first_renewal_date: formateDateOnly(subscription.firstRenewalDate),
        next_renewal_date: formateDateOnly(subscription.renewalDate),
        renewal_notification_date: formateDateOnly(subscription.renewalNotificationDate),
        cancellation_date: subscription.cancellationDate
  ? formatDateTime(subscription.cancellationDate)
  : null,
       
        // Financial Data
        membership_price: parseFloat(membership.price) || 0.00,
        recurring_amount: parseFloat(subscription.recurringAmount) || 0.00,
        subscription_amount: parseFloat(subscription.amount) || 0.00,
        registration_fee: parseFloat(membership.registrationFee) || 0.00,
        total_amount: (parseFloat(membership.price) || 0) + (parseFloat(membership.registrationFee) || 0),
        last_payment_date: formateDateOnly(latestInvoice.lastUpdated),
        last_payment_amount: parseFloat(latestInvoice.grandTotal) || 0.00,
        last_payment_status: lastPaymentStatus,
        
        // Membership Configuration
        duration_count: membership.durationCount || 0,
        duration_type: membership.duration || 0,
        renewal_type: membership.renewalType || 0,
        no_of_members_included: membership.noOfMembersIncluded || 0,
        no_of_additional_members: membership.noOfAdditionalMembers || 0,
        
        // Auto-Renewal Tracking
        auto_exception: subscription.autoException || 0,
        declined_count: subscription.declinedCount || 0,
        no_of_payments: subscription.noOfPayments || null,
        payment_day: subscription.paymentDay || null,
        
        // Cancellation Info
        cancel_reason: subscription.cancelReason || null,
        cancel_notes: subscription.cancelNotes || null,
        
        // Calculated Metrics
        days_to_expiration: daysToExpiration,
        is_active: isActive,
        is_expiring_30days: isExpiring30days,
        is_expiring_60days: isExpiring60days,
        is_lapsed: isLapsed,
        is_auto_renew_failed: isAutoRenewFailed,
        days_since_cancellation: daysSinceCancellation,
        
        // Categorization
        booking_method: me.bookingMethod || 0,
        department_id: membership.departmentId || null,
      };
    });

    if (cleanedRows.length > 0) {
      console.log(`Last payment status - ${cleanedRows[0].last_payment_status}`);
    }

    await clickhouse.insert({
      table: 'memberships',
      values: cleanedRows,
      format: 'JSONEachRow',
    });

    total += enrollments.length;
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
