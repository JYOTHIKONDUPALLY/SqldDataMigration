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

export async function migrateClassSession(mysqlConnection, clickhouseClient, batchSize = 2000) {
  console.log('Starting appointment migration with range and rental data...');
  
  // Create ClickHouse table
  await createClickHouseTable(clickhouseClient);
  
  // Build WHERE clause for filtering
  let whereConditions = ['1=1'];
  const params = [];
  
  if (CONFIG.serviceProviderId) {
    whereConditions.push('a.serviceProviderId = ?');
    params.push(CONFIG.serviceProviderId);
  }
  
  if (CONFIG.dateFrom) {
    whereConditions.push('a.date >= ?');
    params.push(CONFIG.dateFrom);
  }
  
  if (CONFIG.dateTo) {
    whereConditions.push('a.date <= ?');
    params.push(CONFIG.dateTo);
  }
  
  const whereClause = whereConditions.join(' AND ');
  
  // Get total count
  const [countResult] = await mysqlConnection.execute(
    `SELECT COUNT(*) as total FROM appointment a WHERE ${whereClause}`,
    params
  );
  const totalRecords = countResult[0].total;
  console.log(`Total records to migrate: ${totalRecords}`);
  
  if (totalRecords === 0) {
    console.log('No records found to migrate.');
    return;
  }
  
  let offset = 0;
  let migratedCount = 0;
const bookingMap = {
    1: 'online',
    0: 'online',
    2: 'phone_in',
    3: 'walk_in',
    4: 'mobile_app'
};
  
  while (offset < totalRecords) {
    console.log(`\n=== Processing batch: ${offset + 1} to ${Math.min(offset + batchSize, totalRecords)} ===`);
    
    // Fetch enriched appointment data with all related tables
    const query = `
      SELECT 
        a.*,
        CONCAT(COALESCE(c.firstName, ''), ' ', COALESCE(c.lastName, '')) as customerName,
        sp.legalName as providerName,
        sl.name as serviceLocationName,
        s.name as serviceName,
        l.name as locationName,
        CONCAT(COALESCE(r.firstName, ''), ' ', COALESCE(r.lastName, '')) as resourceName,
        rec.pattern as recurringPattern,
        CONCAT(COALESCE(cm.firstName, ''), ' ', COALESCE(cm.lastName, '')) as customerMemberName,
        p.name as packageName,
        
        -- Range ticket data (one-to-one with appointment)
        rt.id as rangeTicketId,
        rt.lane,
        rt.timeIn,
        rt.timeOut,
        rt.membersCount,
        rt.nonMembersCount,
        rt.firearmProducts,
        rt.firearmItems,
        rt.firearmItemsCount,
        rt.ammoItems,
        rt.ammoItemsCount,
        rt.createdDate as rangeCreatedDate,
        rt.createdBy as rangeCreatedBy,
        rt.status as rangeStatus,
        
        -- Addon data (one-to-one with appointment)
ad.name as addonName, 
        ad.addonType as addonType, 
        ad.duration as addonDuration,
        ad.actualPrice as addonActualPrice,
        ad.price as addonPrice, 
        ad.itemId as addonItemId



        
      FROM appointment a
      LEFT JOIN customer c ON a.customerId = c.id
      LEFT JOIN serviceprovider sp ON a.serviceProviderId = sp.id
      LEFT JOIN location sl ON a.serviceLocation = sl.id
      LEFT JOIN service s ON a.serviceId = s.id
      LEFT JOIN location l ON a.locationId = l.id
      LEFT JOIN resource r ON a.resourceId = r.id
      LEFT JOIN reccuring rec ON a.recurringId = rec.id
      LEFT JOIN customermembers cm ON a.customerMemberId = cm.id
      LEFT JOIN package p ON a.packageId = p.id
      inner JOIN rangeticket rt ON a.id = rt.appointmentId
      Left JOIN addon ad on a.id=a.addonId
      WHERE ${whereClause}
      ORDER BY a.date, a.id
      LIMIT ? OFFSET ?
    `;
    
    const [appointments] = await mysqlConnection.execute(query, [...params, batchSize, offset]);
    
    if (appointments.length === 0) {
      console.log('No more appointments to process.');
      break;
    }
    
    console.log(`Fetched ${appointments.length} appointments from MySQL`);
    
    // For each appointment, fetch rental items
    const appointmentIds = appointments.map(a => a.id);
    
    let rentalItems = [];
    if (appointmentIds.length > 0) {
      try {
        const [rentals] = await mysqlConnection.execute(
          `SELECT * FROM rentalitems WHERE rangeTicketId IN (
            SELECT id FROM rangeticket WHERE appointmentId IN (?)
          )`,
          [appointmentIds]
        );
        rentalItems = rentals;
        console.log(`Fetched ${rentalItems.length} rental items`);
      } catch (error) {
        console.log('No rental items found or error fetching:', error.message);
      }
    }
    
    // Group rental items by range ticket
    const rentalsByRangeTicket = {};
    rentalItems.forEach(rental => {
      if (!rentalsByRangeTicket[rental.rangeTicketId]) {
        rentalsByRangeTicket[rental.rangeTicketId] = [];
      }
      rentalsByRangeTicket[rental.rangeTicketId].push(rental);
    });
    
    // Transform data
    const transformedData = appointments.map(appt => {
      const rentals = rentalsByRangeTicket[appt.rangeTicketId] || [];
      
      return {
        // Appointment fields
        id: appt.id,
        customerId: appt.customerId ,
        customerName: appt.customerName || '',
        serviceProviderId: appt.serviceProviderId ,
        providerName: appt.providerName || '',
        serviceLocation: appt.serviceLocation ,
        serviceLocationName: appt.serviceLocationName || '',
        approval: getApprovalStatus(appt.approval),
        appointmentDate: formatDateOnly(appt.date),
        slotTime: appt.slotTime || '00:00:00',
        status: appt.status ,
        serviceId: appt.serviceId ,
        serviceName: appt.serviceName || '',
        locationId: appt.locationId ,
        locationName: appt.locationName || '',
        resourceId: appt.resourceId,
        resourceName: appt.resourceName || '',
        recurringId: appt.recurringId ,
        recurringPattern: appt.recurringPattern || '',
        invoiceId: appt.invoiceId ,
        customerMemberId: appt.customerMemberId ,
        customerMemberName: appt.customerMemberName || '',
        packageId: appt.packageId ,
        packageName: appt.packageName || '',
        payment: appt.payment =1 ? "Yes":"No",
        packageEnrollmentId: appt.packageEnrollmentId || 0,
        customId: appt.customId || '',
        bookingMethod: bookingMap[appt.bookingMethod] || '',
        creationDate: formatDate(appt.creationDate),
        parentId: appt.parentId || 0,
        cancelType: appt.cancelType || '',
        membershipId: appt.membershipId || 0,
        actualStartTime: appt.actualStartTime || '00:00:00',
        actualEndTime: appt.actualEndTime || '00:00:00',
        additionalStatus: appt.additionalStatus || 0,
        reasonId: appt.reasonId || 0,
        parentAppointmentId: appt.parentAppointmentId || 0,
        addons: appt.addons || '',
        addonId: appt.addonId || 0,
        addonType: appt.addonType || 0,
        additionalServiceParentId: appt.additionalServiceParentId || 0,
        additionalServices: appt.additionalServices || '',
        additionalServiceId: appt.additionalServiceId || 0,
        additionalCustomers: appt.additionalCustomers || '',
        additionalCustomerMembers: appt.additionalCustomerMembers || '',
        instantRedeemable: appt.instantRedeemable || 0,
        customerNoteId: appt.customerNoteId || 0,
        checkoutTogether: appt.checkoutTogether || 0,
        checkDeviceAvailability: appt.checkDeviceAvailability || 0,
        customerConfirmation: appt.customerConfirmation || 0,
        treatmentItemId: appt.treatmentItemId || 0,
        sequenceDelay: appt.sequenceDelay || '',
        sequencePriority: appt.sequencePriority || 0,
        additionalPersonsQty: appt.additionalPersonsQty || 0,
        requiredServices: appt.requiredServices || '',
        bookedBy: appt.bookedBy || 0,
        bookedFrom: appt.bookedFrom || 0,
        bookedNameText: appt.bookedNameText || '',
        customerSessionId: appt.customerSessionId || 0,
        isAddedToCart: appt.isAddedToCart || 0,
        groupId: appt.groupId || 0,
        sessionEnd: appt.sessionEnd || 0,
        sessionEndDate: formatDate(appt.sessionEndDate),
        sessionEndBy: appt.sessionEndBy || 0,
        extendedAppointment: appt.extendedAppointment || 0,
        extendedParentAppointmentId: appt.extendedParentAppointmentId || 0,
        extendedDuration: appt.extendedDuration || 0,
        extendedPrice: appt.extendedPrice || 0,
        isAppointmentExtended: appt.isAppointmentExtended || 0,
        appointmentRequested: appt.appointmentRequested || 0,
        checkinGroupCode: appt.checkinGroupCode || '',
        temp_fetch: appt.temp_fetch || 0,
        internallyDeleted: appt.internallyDeleted || 0,
        loggedInCustomerMemberId: appt.loggedInCustomerMemberId || 0,
        startDate: formatDateOnly(appt.startDate),
        endDate: formatDateOnly(appt.endDate),
        additionalPersonParentId: appt.additionalPersonParentId || 0,
        additionalCustomerId: appt.additionalCustomerId || 0,
        additionalCustomerMemberId: appt.additionalCustomerMemberId || 0,
        overNight: appt.overNight || 0,
        
        // Range ticket fields
        rangeTicketId: appt.rangeTicketId ,
        lane: appt.lane || 0,
        timeIn: appt.timeIn || '00:00:00',
        timeOut: appt.timeOut || '00:00:00',
        membersCount: appt.membersCount || 0,
        nonMembersCount: appt.nonMembersCount || 0,
        firearmProducts: appt.firearmProducts || '',
        firearmItems: appt.firearmItems || '',
        firearmItemsCount: appt.firearmItemsCount || '',
        ammoItems: appt.ammoItems || '',
        ammoItemsCount: appt.ammoItemsCount || '',
        rangeCreatedDate: formatDate(appt.rangeCreatedDate),
        rangeCreatedBy: appt.rangeCreatedBy || 0,
        rangeStatus: appt.rangeStatus || 0,
        
        // Aggregated rental items data
        totalRentals: rentals.length,
        rentalIds: rentals.map(r => r.id),
        rentalProductIds: rentals.map(r => r.productId || 0),
        rentalInventoryIds: rentals.map(r => r.inventoryId || 0),
        rentalSerialNumbers: rentals.map(r => r.serialNumber || ''),
        rentalProductTypes: rentals.map(r => r.productType || ''),
        rentalQuantities: rentals.map(r => r.quantity || 0),
        rentalRentTimes: rentals.map(r => formatDate(r.rentTime)),
        rentalReturnTimes: rentals.map(r => formatDate(r.returnTime)),
        rentalPaidStatuses: rentals.map(r => r.paidStatus || 0),
        rentalInvoiceIds: rentals.map(r => r.invoiceId || 0),
        rentalStatuses: rentals.map(r => r.status || 0),
        rentalAmmoUsedCounts: rentals.map(r => r.ammoUsedCount || 0),
        
        // Calculated analytics fields
        sessionDuration: calculateSessionDuration(appt.timeIn, appt.timeOut),
        totalVisitors: (appt.membersCount || 0) + (appt.nonMembersCount || 0),
        dayOfWeek: appt.date ? new Date(appt.date).getDay() : 0,
        monthOfYear: appt.date ? new Date(appt.date).getMonth() + 1 : 0,
        year: appt.date ? new Date(appt.date).getFullYear() : 1970,
        timeOfDay: getTimeOfDay(appt.timeIn),
        isWeekend: appt.date ? [0, 6].includes(new Date(appt.date).getDay()) ? 1 : 0 : 0,
        totalAmmoUsed: rentals.reduce((sum, r) => sum + (r.ammoUsedCount || 0), 0),
        hasFirearms: (appt.firearmItems || '').length > 0 ? 1 : 0,
        hasAmmo: (appt.ammoItems || '').length > 0 ? 1 : 0
      };
    });
    
    console.log(`Transformed ${transformedData.length} records`);
    
    // Group data by year-month to avoid partition issues
    if (transformedData.length > 0) {
      const groupedByMonth = {};
      
      transformedData.forEach(record => {
        const yearMonth = `${record.year}-${String(record.monthOfYear).padStart(2, '0')}`;
        if (!groupedByMonth[yearMonth]) {
          groupedByMonth[yearMonth] = [];
        }
        groupedByMonth[yearMonth].push(record);
      });
      
      console.log(`Grouped into ${Object.keys(groupedByMonth).length} month(s)`);
      
      // Insert each month's data separately
      for (const [yearMonth, monthData] of Object.entries(groupedByMonth)) {
        try {
          console.log(`  -> Inserting ${monthData.length} records for ${yearMonth}...`);
          
          await clickhouseClient.insert({
            table: 'appointment_enriched',
            values: monthData,
            format: 'JSONEachRow'
          });
          
          migratedCount += monthData.length;
          console.log(`  ✓ Successfully inserted ${monthData.length} records for ${yearMonth}`);
        } catch (insertError) {
          console.error(`  ✗ Failed to insert data for ${yearMonth}:`, insertError.message);
          console.error('Sample record:', JSON.stringify(monthData[0], null, 2));
          throw insertError;
        }
      }
      
      console.log(`Batch complete. Total migrated so far: ${migratedCount}/${totalRecords}`);
    }
    
    offset += batchSize;
  }
  
  console.log(`\n✓ Migration completed successfully!`);
  console.log(`Total records migrated: ${migratedCount}`);
  
  // Verify data in ClickHouse
  const result = await clickhouseClient.query({
    query: 'SELECT count() as total FROM appointment_enriched',
    format: 'JSONEachRow'
  });
  
  const rows = await result.json();
  console.log(`\nVerification: ${rows[0].total} records in ClickHouse table`);
}

async function createClickHouseTable(clickhouseClient) {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS appointment_enriched (
      -- Appointment core fields
      id Int32,
      customerId Int32,
      customerName String,
      paymentOptions Int32,
      serviceProviderId Int32,
      providerName String,
      reminders String,
      firstVisit Int8,
      serviceLocation Int32,
      serviceLocationName String,
      approval Int32,
      approvalStatus String,
      date Date,
      slotTime String,
      status Int32,
      serviceId Int32,
      serviceName String,
      locationId Int32,
      locationName String,
      resourceId Int32,
      resourceName String,
      recurringId Int32,
      recurringPattern String,
      invoiceId Int32,
      customerMemberId Int32,
      customerMemberName String,
      packageId Int32,
      packageName String,
      payment Int8,
      packageEnrollmentId Int32,
      customId String,
      bookingMethod Int32,
      creationDate DateTime,
      parentId Int32,
      cancelType String,
      membershipId Int32,
      actualStartTime String,
      actualEndTime String,
      additionalStatus Int32,
      reasonId Int32,
      parentAppointmentId Int32,
      addons String,
      addonId Int32,
      additionalServiceParentId Int32,
      additionalServices String,
      additionalServiceId Int32,
      additionalCustomers String,
      additionalCustomerMembers String,
      instantRedeemable Int8,
      customerNoteId Int32,
      checkoutTogether Int8,
      checkDeviceAvailability Int8,
      customerConfirmation Int8,
      treatmentItemId Int32,
      sequenceDelay String,
      sequencePriority Int32,
      additionalPersonsQty Int32,
      requiredServices String,
      bookedBy Int32,
      bookedFrom Int32,
      bookedNameText String,
      customerSessionId Int32,
      isAddedToCart Int32,
      groupId Int32,
      sessionEnd Int32,
      sessionEndDate DateTime,
      sessionEndBy Int32,
      extendedAppointment Int32,
      extendedParentAppointmentId Int32,
      extendedDuration Int32,
      extendedPrice Float32,
      isAppointmentExtended Int32,
      appointmentRequested Int32,
      checkinGroupCode String,
      temp_fetch Int32,
      internallyDeleted Int32,
      loggedInCustomerMemberId Int32,
      startDate Date,
      endDate Date,
      additionalPersonParentId Int32,
      additionalCustomerId Int32,
      additionalCustomerMemberId Int32,
      overNight Int32,
      
      -- Range ticket fields
      rangeTicketId Int32,
      lane Int32,
      timeIn String,
      timeOut String,
      membersCount Int32,
      nonMembersCount Int32,
      firearmProducts String,
      firearmItems String,
      firearmItemsCount String,
      ammoItems String,
      ammoItemsCount String,
      rangeCreatedDate DateTime,
      rangeCreatedBy Int32,
      rangeStatus Int32,
      
      -- Rental items (arrays for multiple rentals per appointment)
      totalRentals Int32,
      rentalIds Array(Int32),
      rentalProductIds Array(Int32),
      rentalInventoryIds Array(Int32),
      rentalSerialNumbers Array(String),
      rentalProductTypes Array(String),
      rentalQuantities Array(Int32),
      rentalRentTimes Array(DateTime),
      rentalReturnTimes Array(DateTime),
      rentalPaidStatuses Array(Int32),
      rentalInvoiceIds Array(Int32),
      rentalStatuses Array(Int32),
      rentalAmmoUsedCounts Array(Int32),
      
      -- Analytics fields
      sessionDuration Int32,
      totalVisitors Int32,
      dayOfWeek Int8,
      monthOfYear Int8,
      year Int32,
      timeOfDay String,
      isWeekend Int8,
      totalAmmoUsed Int32,
      hasFirearms Int8,
      hasAmmo Int8
    ) ENGINE = MergeTree()
    ORDER BY (serviceProviderId, date, id)
    PARTITION BY toYear(date);
  `;
  
  await clickhouseClient.exec({ query: createTableQuery });
  console.log('ClickHouse table created/verified successfully');
}

function getApprovalStatus(approval) {
  const statuses = {
    1: 'Pending',
    2: 'Approved',
    3: 'Rejected',
    4: 'Cancelled',
    5: 'Completed'
  };
  return statuses[approval] || 'Unknown';
}

function calculateSessionDuration(timeIn, timeOut) {
  if (!timeIn || !timeOut) return 0;
  
  try {
    const [inHours, inMinutes] = timeIn.split(':').map(Number);
    const [outHours, outMinutes] = timeOut.split(':').map(Number);
    
    const inTotalMinutes = inHours * 60 + inMinutes;
    const outTotalMinutes = outHours * 60 + outMinutes;
    
    return outTotalMinutes - inTotalMinutes;
  } catch {
    return 0;
  }
}

function getTimeOfDay(time) {
  if (!time) return 'Unknown';
  
  try {
    const hour = parseInt(time.split(':')[0]);
    
    if (hour >= 5 && hour < 12) return 'Morning';
    if (hour >= 12 && hour < 17) return 'Afternoon';
    if (hour >= 17 && hour < 21) return 'Evening';
    return 'Night';
  } catch {
    return 'Unknown';
  }
}

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
    await migrateClassSession(mysqlConn, clickhouse, CONFIG.batchSize);
  } catch (error) {
    console.error('\n✗ Migration failed:', error);
    console.error('Error details:', error.message);
    throw error;
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();