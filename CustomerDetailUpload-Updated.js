import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

/**
 * Format MySQL DateTime to ClickHouse DateTime string
 */
function formatDate(dateValue) {
    if (!dateValue) return null;
    if (dateValue === '0000-00-00' || dateValue === '0000-00-00 00:00:00') return null;

    const date = new Date(dateValue);
    if (isNaN(date.getTime())) return null;

    return date.toISOString().replace('T', ' ').substring(0, 19); // YYYY-MM-DD HH:mm:ss
}

/**
 * Format MySQL Date to ClickHouse Date string
 */
function formatDateOnly(dateValue) {
    if (!dateValue) return null;
    if (dateValue === '0000-00-00') return null;

    const date = new Date(dateValue);
    if (isNaN(date.getTime())) return null;

    return date.toISOString().substring(0, 10); // YYYY-MM-DD
}

/**
 * Map MySQL status -> ClickHouse status text
 */
function mapStatus(status) {
    switch (status) {
        case 1: return 'active';
        case 2: return 'inactive';
        case 3: return 'prospect';
        case 4: return 'suspend';
        default: return 'unknown';
    }
}

/**
 * Map unsubscribe preferences
 */
function mapUnsubscribe(pref) {
    if (!pref) return '';
    if ((pref.emailNewsletter === 0 && pref.unsubscribeAutoresponder === 1) || pref.unsubscribeAllEmail === 1) {
        return 'N&A';
    } else if (pref.emailNewsletter === 0) {
        return 'N';
    } else if (pref.unsubscribeAutoresponder === 1) {
        return 'A';
    }
    return '';
}

function Aquire(data) {
    if (!data) return '';
    if (data === 1) return 'DataLoad';
    if (data === 2) return 'Walk-In';
    if (data === 3) return 'Phone-In';
    if (data === 4) return 'Online';
    return '';
}
{

}
async function migrateCustomers(mysqlConn, clickhouse,batchSize = 100) {
    try {
         const [countResult] = await mysqlConn.execute(
      `SELECT COUNT(*) as total  FROM customer c
      inner join serviceProviderCustomerDetails scd on scd.customerId = c.id where scd.serviceProviderId = 22`
    );
    const totalRecords = countResult[0].total;
    console.log(`Total customers to migrate: ${totalRecords}`);

    if (totalRecords === 0) {
      console.log("No customers found to migrate.");
      return;
    }
        const franchiseId = 0 //TODO : change later
        const franchiseName = '88 Tactile' //TODO : change later
        // fetch base customers
         let offset = 0;
            let totalMigrated = 0;
            while(offset < totalRecords){
                 const [rows] = await mysqlConn.execute(`
      SELECT 
        c.id, c.email, c.firstName, c.middleName, c.lastName,
        c.mobile, c.phone, c.dob, c.status, c.gender, 
        c.serviceLocation, c.creationDate, c.idNumber, c.acquired
      FROM customer c
      inner join serviceProviderCustomerDetails scd on scd.customerId = c.id where scd.serviceProviderId = 22 LIMIT ${batchSize} OFFSET ${offset}
    `);

        console.log(`Fetched ${rows.length} rows from MySQL (customers)`);
        const AddressDetails = {
            address: '',
            country: '',
            state: '',
            city: '',
            zipCode: ''
        };

        const [details] = await mysqlConn.query(
            `SELECT address, countryId, stateId, cityId, zipCode 
   FROM serviceProviderCustomerAddress 
   WHERE customerId = ? and serviceProviderId =22 and status=1 `,
            [rows[0]?.id]
        );

        if (details.length > 0) {
            const [country] = await mysqlConn.query(`SELECT name FROM country WHERE id = ?`, [details[0]?.countryId]);
            const [state] = await mysqlConn.query(`SELECT name FROM state WHERE id = ?`, [details[0]?.stateId]);
            const [city] = await mysqlConn.query(`SELECT name FROM city WHERE id = ?`, [details[0]?.cityId]);

            AddressDetails.address = details[0]?.address;
            AddressDetails.country = country[0]?.name || '';
            AddressDetails.state = state[0]?.name || '';
            AddressDetails.city = city[0]?.name || '';
            AddressDetails.zipCode = details[0]?.zipCode || '';
        }



        if (rows.length > 0) {
            const values = [];

            for (const row of rows) {
                // membership check
                // const [serviceProviderRows] = await mysqlConn.execute(
                //     `SELECT serviceProviderId FROM serviceProviderCustomerDetails WHERE customerId = ? LIMIT 1`,
                //     [row.id]
                // );
                const serviceProviderId = 22; //hardcoded
                const [serviceProviderName] = await mysqlConn.query(`SELECT id, legalName as name FROM serviceprovider WHERE id IN (?)`, serviceProviderId);

                let isMember = 'No';
                if (serviceProviderId) {
                    const [membership] = await mysqlConn.execute(
                        `SELECT 1 FROM membershipEnrollment WHERE serviceProviderId = ? AND customerId = ? LIMIT 1`,
                        [serviceProviderId, row.id]
                    );
                    isMember = membership.length > 0 ? 'Yes' : 'No';
                }

                // preferences for unsubscribe
                const [prefs] = await mysqlConn.execute(
                    `SELECT * FROM customerPreferences WHERE customerId = ? LIMIT 1`,
                    [row.id]
                );

                // tags
                const [tagRows] = await mysqlConn.execute(
                    `SELECT t.tagName 
           FROM customerTags ct
           JOIN tags t ON ct.tagId = t.id
           WHERE ct.customerId = ? AND ct.status=1 AND t.status=1`,
                    [row.id]
                );
                const tags = tagRows.map(t => t.tagName).join(',');

                // referral
                const [referralRows] = await mysqlConn.execute(
                    `SELECT referralText  FROM serviceProviderCustomerDetails WHERE customerId = ? LIMIT 1`,
                    [row.id]
                );
                const referral = referralRows[0]?.referralText || '';

                // loyalty points
                const [points] = await mysqlConn.execute(
                    `SELECT SUM(availablePoints) AS points FROM rewardPoints WHERE customerId = ? AND dateExpire >= CURDATE() AND status IN (1,6)`,
                    [row.id]
                );
                const loyaltyPoints = points[0]?.points || 0;
                            const CustomerName = [row.firstName, row.middleName, row.lastName]
  .map(v => v?.trim())
  .filter(Boolean)
  .join(' ');

                // build ClickHouse row
                values.push({
                    id: row.id,
                    franchise_id: franchiseId,
                    franchise: franchiseName,
                    provider_id: serviceProviderId,
                    provider: serviceProviderName[0]?.name || '',
                       CustomerName:  CustomerName.replace(/\s+/g, ' '),
                    FirstName: row.firstName || '',
                    MiddleName: row.middleName || '',
                    LastName: row.lastName || '',
                    Email: row.email || '',
                    Phone: row.phone || '',
                    Mobile: row.mobile || '',
                    DateOfBirth: formatDateOnly(row.dob),
                    Gender: row.gender,
                    IsMember: isMember,
                    MemberId: row.idNumber || '',
                    Status: mapStatus(row.status),
                    Acquisition: Aquire(row.acquired),
                    Address: AddressDetails.address,
                    City: AddressDetails.city,
                    State: AddressDetails.state,
                    Country: AddressDetails.country,
                    Zipcode: AddressDetails.zipCode,
                    Unsubscribed: mapUnsubscribe(prefs[0]),
                    Tag: tags,
                    LoyaltyPoints: loyaltyPoints,
                    Referral: referral,
                    created_at: formatDate(row.creationDate),
                    updated_at: formatDate(new Date()),
                    deleted_at: null
                });
            }

            if (values.length > 0) {
                if (offset === 0) {
          console.log("Sample record for ClickHouse insert:");
          console.log(JSON.stringify(values[0], null, 2));
        }
                await clickhouse.insert({
                    table: 'customers',
                    values,
                    format: 'JSONEachRow'
                });
                 totalMigrated += values.length;
        console.log(
          ` Migrated batch: ${offset + 1} → ${offset + values.length} (total so far: ${totalMigrated})`
        );
            }
               offset += batchSize;
        }
            }
       
        console.log(`🎉 customer migration completed. Total migrated: ${totalMigrated}`);
    } catch (err) {
        console.error('❌ Customers migration error:', err);
    }
}

async function migrateData() {
    const mysqlConn = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'bizzflo'
    });

    const clickhouse = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'clickHouseInvoice'
    });

    try {
        await migrateCustomers(mysqlConn, clickhouse);
    } finally {
        await mysqlConn.end();
        await clickhouse.close();
    }
}

migrateData();
