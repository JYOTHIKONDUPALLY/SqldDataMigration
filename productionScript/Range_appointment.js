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
  
  if(totalRecords >0){
 await clickhouse.insert({
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

async function getDistinctServiceProviders(mysqlConn) {
  const [rows] = await mysqlConn.execute(`
     SELECT DISTINCT id as serviceProviderId
    FROM serviceProvider
    WHERE status = 1
  `);
  return rows.map(r => r.serviceProviderId);
}

async function createInvoiceTable(clickhouse, tableName) {
  const createQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName}
    (
  id Int32,
  customerId Int32,
  customerName String,
  serviceProviderId Int32,
  providerName String,
  serviceLocation Int32,
  serviceLocationName String,
  approval String,
  appointmentDate Date,
  slotTime String,
  status Int32,
  serviceId Int32,
  serviceName String,
  locationId Int32,
  locationName String,
  resourceId Int32,
  resourceName String,
  resourceStaffType String,
  recurringId Int32,
  recurringPattern String,
  invoiceId Int32,
  customerMemberId Int32,
  customerMemberName String,
  packageId Int32,
  packageName String,
  payment String,
  packageEnrollmentId Int32,
  customId String,
  bookingMethod String,
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
  addonName String, 
  addonType String,
  addonDuration Int32,
  addonActualPrice Int32,
  addonPrice Int32, 
  addonItemId Int32,
  
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
ORDER BY (serviceProviderId, appointmentDate, id)
PARTITION BY toYear(appointmentDate)
  `;

  await clickhouse.exec({ query: createQuery });
  console.log(`📦 Table ready: ${tableName}`);
}


export async function migrateAppointments(mysqlConnection, clickhouseClient, serviceProviderId, batchSize = 2000) {
  const TABLE_KEY = `Range_appointments_${serviceProviderId}`;
      let lastId = await getLastMigratedId(clickhouseClient, TABLE_KEY);
        console.log(`▶ Resuming migration from appointment.id > ${lastId}`);
  console.log('Starting appointment migration with range and rental data...');
  
  // Get total count with parameterized query - NO ID filtering
  const [countResult] = await mysqlConnection.execute(
    `SELECT COUNT(*) as total FROM appointment a inner join rangeTicket r on r.appointmentId = a.id WHERE a.serviceProviderId=${serviceProviderId} and a.id > ${lastId}`  );
  const totalRecords = countResult[0].total;
  console.log(`📊 Total records to migrate: ${totalRecords}`);
  
  if (totalRecords === 0) {
    console.log('No records found to migrate.');
    return;
  }
  
  let offset = 0;
  let migratedCount = 0;
  let lastProcessedId = 0;
  
  const bookingMap = {
    1: 'online',
    0: 'online',
    2: 'phone_in',
    3: 'walk_in',
    4: 'mobile_app'
  };
  
  const StaffMap = {
    1: 'Regular',
    2: 'temperory',
    3: 'Seasional',
    4: 'Contractor',
    5: 'FullTimeStudent',
    6: 'PartTimeStudent'
  };
  
  while (offset < totalRecords) {
    console.log(`\n=== Processing batch: ${offset + 1} to ${Math.min(offset + batchSize, totalRecords)} ===`);
    
    // Fetch appointments only
    const query = `
      SELECT a.* FROM appointment a
      inner join rangeTicket r on r.appointmentId = a.id
      WHERE a.serviceProviderId=${serviceProviderId} and a.id > ${lastId}
      ORDER BY a.id
      LIMIT ${batchSize}
    `;
    
    const [appointments] = await mysqlConnection.execute(query);
    
    if (appointments.length === 0) {
      console.log('No more appointments to process.');
      break;
    }
    
    console.log(`Fetched ${appointments.length} appointments from MySQL`);
    
    // Extract unique IDs for batch queries (filter out null/undefined but keep 0)
    const appointmentIds = appointments.map(a => a.id);
    const customerIds = [...new Set(appointments.map(a => a.customerId).filter(id => id != null))];
    const serviceProviderIds = [...new Set(appointments.map(a => a.serviceProviderId).filter(id => id != null))];
    const serviceLocationIds = [...new Set(appointments.map(a => a.serviceLocation).filter(id => id != null))];
    const serviceIds = [...new Set(appointments.map(a => a.serviceId).filter(id => id != null))];
    const locationIds = [...new Set(appointments.map(a => a.locationId).filter(id => id != null))];
    const resourceIds = [...new Set(appointments.map(a => a.resourceId).filter(id => id != null))];
    const recurringIds = [...new Set(appointments.map(a => a.recurringId).filter(id => id != null))];
    const customerMemberIds = [...new Set(appointments.map(a => a.customerMemberId).filter(id => id != null))];
    const packageIds = [...new Set(appointments.map(a => a.packageId).filter(id => id != null))];
    const addonIds = [...new Set(appointments.map(a => a.addonId).filter(id => id != null))];
    
    // Helper function to create placeholders for IN clause
    const createPlaceholders = (arr) => arr.map(() => '?').join(',');
    
    // Fetch related data in batches
    const [customers, serviceProviders, serviceLocations, services, locations, resources, 
           recurrings, customerMembers, packages, addons, rangeTickets] = await Promise.all([
      // Customers
      customerIds.length > 0 
        ? mysqlConnection.execute(
            `SELECT id, CONCAT(COALESCE(firstName, ''), ' ', COALESCE(lastName, '')) as name FROM customer WHERE id IN (${createPlaceholders(customerIds)})`,
            customerIds
          ).then(([rows]) => rows)
        : [],
      
      // Service Providers
      serviceProviderIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, legalName as name FROM serviceProvider WHERE id IN (${createPlaceholders(serviceProviderIds)})`,
            serviceProviderIds
          ).then(([rows]) => rows)
        : [],
      
      // Service Locations
      serviceLocationIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, name FROM location WHERE id IN (${createPlaceholders(serviceLocationIds)})`,
            serviceLocationIds
          ).then(([rows]) => rows)
        : [],
      
      // Services
      serviceIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, name FROM service WHERE id IN (${createPlaceholders(serviceIds)})`,
            serviceIds
          ).then(([rows]) => rows)
        : [],
      
      // Locations
      locationIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, name FROM location WHERE id IN (${createPlaceholders(locationIds)})`,
            locationIds
          ).then(([rows]) => rows)
        : [],
      
      // Resources
      resourceIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, CONCAT(COALESCE(firstName, ''), ' ', COALESCE(lastName, '')) as name, staffType FROM resource WHERE id IN (${createPlaceholders(resourceIds)})`,
            resourceIds
          ).then(([rows]) => rows)
        : [],
      
      // Recurrings
      recurringIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, pattern FROM reccuring WHERE id IN (${createPlaceholders(recurringIds)})`,
            recurringIds
          ).then(([rows]) => rows)
        : [],
      
      // Customer Members
      customerMemberIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, CONCAT(COALESCE(firstName, ''), ' ', COALESCE(lastName, '')) as name FROM customerMembers WHERE id IN (${createPlaceholders(customerMemberIds)})`,
            customerMemberIds
          ).then(([rows]) => rows)
        : [],
      
      // Packages
      packageIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, name FROM package WHERE id IN (${createPlaceholders(packageIds)})`,
            packageIds
          ).then(([rows]) => rows)
        : [],
      
      // Addons
      addonIds.length > 0
        ? mysqlConnection.execute(
            `SELECT id, name, addonType, duration, actualPrice, price, itemId FROM addon WHERE id IN (${createPlaceholders(addonIds)})`,
            addonIds
          ).then(([rows]) => rows)
        : [],
      
      // Range Tickets
      
      appointmentIds.length > 0
        ? mysqlConnection.execute(
            `SELECT * FROM rangeTicket WHERE appointmentId IN (${createPlaceholders(appointmentIds)})`,
            appointmentIds
          ).then(([rows]) => rows)
        : []
    ]);
    
    console.log(`Fetched related data: ${customers.length} customers, ${serviceProviders.length} providers, ${rangeTickets.length} range tickets`);
    
    // Create lookup maps
    const customerMap = Object.fromEntries(customers.map(c => [c.id, c]));
    const providerMap = Object.fromEntries(serviceProviders.map(sp => [sp.id, sp]));
    const serviceLocationMap = Object.fromEntries(serviceLocations.map(sl => [sl.id, sl]));
    const serviceMap = Object.fromEntries(services.map(s => [s.id, s]));
    const locationMap = Object.fromEntries(locations.map(l => [l.id, l]));
    const resourceMap = Object.fromEntries(resources.map(r => [r.id, r]));
    const recurringMap = Object.fromEntries(recurrings.map(rec => [rec.id, rec]));
    const customerMemberMap = Object.fromEntries(customerMembers.map(cm => [cm.id, cm]));
    const packageMap = Object.fromEntries(packages.map(p => [p.id, p]));
    const addonMap = Object.fromEntries(addons.map(ad => [ad.id, ad]));
    const rangeTicketMap = Object.fromEntries(rangeTickets.map(rt => [rt.appointmentId, rt]));
    
    // Fetch rental items for range tickets
    const rangeTicketIds = rangeTickets.map(rt => rt.id);
    let rentalItems = [];
    
    if (rangeTicketIds.length > 0) {
      try {
        const [rentals] = await mysqlConnection.execute(
          `SELECT * FROM rentalItems WHERE rangeTicketId IN (${createPlaceholders(rangeTicketIds)})`,
          rangeTicketIds
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
      const rangeTicket = rangeTicketMap[appt.id] || {};
      const rentals = rentalsByRangeTicket[rangeTicket.id] || [];
      const customer = customerMap[appt.customerId] || {};
      const provider = providerMap[appt.serviceProviderId] || {};
      const serviceLocation = serviceLocationMap[appt.serviceLocation] || {};
      const service = serviceMap[appt.serviceId] || {};
      const location = locationMap[appt.locationId] || {};
      const resource = resourceMap[appt.resourceId] || {};
      const recurring = recurringMap[appt.recurringId] || {};
      const customerMember = customerMemberMap[appt.customerMemberId] || {};
      const package_ = packageMap[appt.packageId] || {};
      const addon = addonMap[appt.addonId] || {};
      
      return {
        // Appointment fields
        id: appt.id,
        customerId: appt.customerId,
        customerName: customer.name || '',
        serviceProviderId: appt.serviceProviderId,
        providerName: provider.name || '',
        serviceLocation: appt.serviceLocation,
        serviceLocationName: serviceLocation.name || 'Others',
        approval: getApprovalStatus(appt.approval),
        appointmentDate: formatDateOnly(appt.date),
        slotTime: appt.slotTime || '00:00:00',
        status: appt.status,
        serviceId: appt.serviceId,
        serviceName: service.name || '',
        locationId: appt.locationId,
        locationName: location.name || '',
        resourceId: appt.resourceId,
        resourceName: resource.name || '',
        resourceStaffType: resource.staffType ? StaffMap[resource.staffType] : 'Lane',
        recurringId: appt.recurringId,
        recurringPattern: recurring.pattern || 'Others',
        invoiceId: appt.invoiceId,
        customerMemberId: appt.customerMemberId,
        customerMemberName: customerMember.name || '',
        packageId: appt.packageId,
        packageName: package_.name || '',
        payment: appt.payment == 1 ? "Yes" : "No",
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
        addonName: addon.name || '',
        addonType: addon.addonType || 0,
        addonDuration: addon.duration || 0,
        addonActualPrice: addon.actualPrice || 0,
        addonPrice: addon.price || 0,
        addonItemId: addon.itemId || 0,
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
        rangeTicketId: rangeTicket.id || 0,
        lane: rangeTicket.lane || 0,
        timeIn: rangeTicket.timeIn || '00:00:00',
        timeOut: rangeTicket.timeOut || '00:00:00',
        membersCount: rangeTicket.membersCount || 0,
        nonMembersCount: rangeTicket.nonMembersCount || 0,
        firearmProducts: rangeTicket.firearmProducts || '',
        firearmItems: rangeTicket.firearmItems || '',
        firearmItemsCount: rangeTicket.firearmItemsCount || '',
        ammoItems: rangeTicket.ammoItems || '',
        ammoItemsCount: rangeTicket.ammoItemsCount || '',
        rangeCreatedDate: formatDate(rangeTicket.createdDate),
        rangeCreatedBy: rangeTicket.createdBy || 0,
        rangeStatus: rangeTicket.status || 0,
        
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
        sessionDuration: calculateSessionDuration(rangeTicket.timeIn, rangeTicket.timeOut),
        totalVisitors: (rangeTicket.membersCount || 0) + (rangeTicket.nonMembersCount || 0),
        dayOfWeek: appt.date ? new Date(appt.date).getDay() : 0,
        monthOfYear: appt.date ? new Date(appt.date).getMonth() + 1 : 0,
        year: appt.date ? new Date(appt.date).getFullYear() : 1970,
        timeOfDay: getTimeOfDay(rangeTicket.timeIn),
        isWeekend: appt.date ? [0, 6].includes(new Date(appt.date).getDay()) ? 1 : 0 : 0,
        totalAmmoUsed: rentals.reduce((sum, r) => sum + (r.ammoUsedCount || 0), 0),
        hasFirearms: (rangeTicket.firearmItems || '').length > 0 ? 1 : 0,
        hasAmmo: (rangeTicket.ammoItems || '').length > 0 ? 1 : 0
      };
    });
    // Track the highest ID in this batch (query is ordered by id)
    lastProcessedId = Math.max(...appointments.map(a => a.id));
    lastId = lastProcessedId;
    
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
            table: TABLE_KEY,
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
      
      console.log(`✔ Batch complete. Processed up to ID: ${lastProcessedId}, Total so far: ${migratedCount}/${totalRecords}`);
    }
    
    offset += appointments.length; // Increment by actual rows fetched
  }
  
  // Update migration progress ONCE at the end
  if (totalRecords > 0 && migratedCount > 0) {
    await updateLastMigratedId(clickhouseClient, TABLE_KEY, lastId, totalRecords);
  console.log(`✔ Migrated up to ID: ${lastId}`);
   console.log(`\n✓ Migration completed successfully!`);

  }
  
  if (totalRecords === migratedCount) {
    console.log(`\n✅ Migration completed successfully!`);
  } else {
    console.log(`\n⚠️ Migration incomplete: Expected ${totalRecords}, Migrated ${migratedCount}`);
  }
  
  console.log(`📊 Total records migrated: ${migratedCount}`);
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
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'bizzflo',
  });

   console.log("✅ Connected to MySQL!");

    const [resultRows] = await mysqlConn.execute('SELECT NOW() AS now');
    console.log("DB Time:", resultRows[0].now);

  const clickhouse = createClient({
    url: "http://localhost:8123",
    username: "default",
    password: "",
    database: "clickHouseInvoice",
  });

  try {
  //   const providerIds = await getDistinctServiceProviders(mysqlConn);
  //     console.log(`🔑 Found ${providerIds.length} service providers`);
  //  for (const providerId of providerIds) {
  const providerId = 22;
      const tableName = `Range_appointments_${providerId}`;
      // console.log(`\n🚀 Migrating provider ${providerId}`);

      await createInvoiceTable(clickhouse, tableName);
       await migrateAppointments(mysqlConn, clickhouse,providerId, CONFIG.batchSize);
    // }
   
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