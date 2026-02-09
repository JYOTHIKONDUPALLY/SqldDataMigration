import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

const CONFIG = {
     batchSize: 2000,
    serviceProviderId: 22
};

function formatDate(dateValue) {
    if (!dateValue) return null;
    if (dateValue === '0000-00-00' || dateValue === '0000-00-00 00:00:00') return null;

    const date = new Date(dateValue);
    if (isNaN(date.getTime())) return null;

    return date.toISOString().replace("T", " ").substring(0, 19);
}

function formatDateOnly(dateValue) {
    if (!dateValue) return null;
    if (dateValue === '0000-00-00') return null;

    const date = new Date(dateValue);
    if (isNaN(date.getTime())) return null;

    return date.toISOString().substring(0, 10);
}

function mapStatus(status) {
    switch (status) {
        case 1: return 'active';
        case 2: return 'inactive';
        case 3: return 'prospect';
        case 4: return 'suspend';
        default: return 'unknown';
    }
}

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
    return {
        1: "DataLoad",
        2: "Walk-In",
        3: "Phone-In",
        4: "Online"
    }[data] || '';
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
      
    id UInt32,
franchise_id UInt32,
franchise String,
provider_id UInt32,
provider String,
    CustomerName String,
    FirstName String,
    MiddleName String,
    LastName String,
    Email String,
    Phone String,
    Mobile String,
    DateOfBirth Date,
    Gender String,
    IsMember String,
    MemberId String,
    Status String,
    Acquisition String,
    Address String,
    City String,
    State String,
    Country String,
    Zipcode String,
    Unsubscribed String,
    Tag String,
     LoyaltyPoints Decimal(18, 2),
    Referral String,
    created_at DateTime,
    updated_at DateTime,
    deleted_at Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY Email
  `;

  await clickhouse.exec({ query: createQuery });
  console.log(`📦 Table ready: ${tableName}`);
}
/**
 * BATCH MIGRATION
 */
async function migrateCustomers(mysqlConn, clickhouse, serviceProviderId, batchSize = 2000) {
    let offset = 0;
    let totalInserted = 0;
    const TABLE_KEY = `customers_${serviceProviderId}`;
    let lastId = await getLastMigratedId(clickhouse, TABLE_KEY);
        console.log(`▶ Resuming migration from customer.id > ${lastId}`);
        const [count]= await mysqlConn.execute(`
            SELECT count(*) as count FROM customer c
            INNER JOIN serviceProviderCustomerDetails scd 
                ON scd.customerId = c.id 
            WHERE scd.serviceProviderId = ${serviceProviderId} and c.id > ${lastId}
        `);
    const totalRecords = count[0].count;
    console.log(`📊 Total records to migrate: ${totalRecords}`);

    while (true) {
        console.log(`📦 Fetching batch OFFSET ${offset} LIMIT ${batchSize}`);

        const [rows] = await mysqlConn.execute(`
            SELECT 
                c.id, c.email, c.firstName, c.middleName, c.lastName,
                c.mobile, c.phone, c.dob, c.status, c.gender,
                c.serviceLocation, c.creationDate, c.idNumber, c.acquired
            FROM customer c
            INNER JOIN serviceProviderCustomerDetails scd 
                ON scd.customerId = c.id 
            WHERE scd.serviceProviderId = ${serviceProviderId} and c.id > ${lastId}
            Order by c.id asc
            LIMIT ${batchSize} 
        `);

        if (rows.length === 0) {
            console.log("🎉 All customers fully migrated.");
            break;
        }

        const batchValues = [];
    lastId = Math.max(...rows.map(r => r.id));
        for (const row of rows) {
         
            // const serviceProviderId = CONFIG.serviceProviderId;

            const [serviceProviderName] = await mysqlConn.query(
                `SELECT legalName AS name FROM serviceProvider WHERE id = ?`,
                [serviceProviderId]
            );

            const [prefs] = await mysqlConn.execute(
                `SELECT * FROM customerPreferences WHERE customerId = ? LIMIT 1`,
                [row.id]
            );

            const [tagRows] = await mysqlConn.execute(
                `SELECT t.tagName 
                 FROM customerTags ct
                 JOIN tags t ON ct.tagId = t.id
                 WHERE ct.customerId = ? AND ct.status=1 AND t.status=1`,
                [row.id]
            );

            const [referralRows] = await mysqlConn.execute(
                `SELECT referralText FROM serviceProviderCustomerDetails WHERE customerId = ? LIMIT 1`,
                [row.id]
            );
              const [addressRows] = await mysqlConn.execute(
                `SELECT sa.address, sa.cityName, sa.stateName, sa.countryName, sa.zipCode 
                 FROM serviceProviderCustomerAddress sa 
                 WHERE sa.customerId = ? and sa.serviceProviderId = ?`,
                [row.id, serviceProviderId]
            );

            const [points] = await mysqlConn.execute(
                `SELECT SUM(availablePoints) AS points 
                 FROM rewardPoints 
                 WHERE customerId = ? 
                   AND dateExpire >= CURDATE() 
                   AND status IN (1,6)`,
                [row.id]
            );
            const [ismember]= await mysqlConn.execute(
                `SELECT id FROM membershipenrollment WHERE customerId = ? AND serviceProviderId = ? LIMIT 1`,
                [row.id, serviceProviderId]
            );
            const CustomerName = [row.firstName, row.middleName, row.lastName]
  .map(v => v?.trim())
  .filter(Boolean)
  .join(' ');

            batchValues.push({
                id: row.id,
                franchise_id: 0,
                franchise: "88 Tactical",
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
                IsMember: ismember.length > 0 ? 'Yes' : 'No',
                MemberId: ismember.length > 0 ? ismember[0].id : 0,
                Status: mapStatus(row.status),
                Acquisition: Aquire(row.acquired),
                Address: addressRows[0]?.address || '',
                City: addressRows[0]?.cityName || '',
                State: addressRows[0]?.stateName || '',
                Country: addressRows[0]?.countryName || '',
                Zipcode: addressRows[0]?.zipCode || '',
                Unsubscribed: mapUnsubscribe(prefs[0]),
                Tag: tagRows.map(t => t.tagName).join(','),
                LoyaltyPoints: points[0]?.points || 0,
                Referral: referralRows[0]?.referralText || '',
                created_at: formatDate(row.creationDate),
                updated_at: formatDate(new Date()),
                deleted_at: null
            });
        }

        // INSERT BATCH INTO CLICKHOUSE
        console.log(`⬆️ Inserting ${batchValues.length} rows into ClickHouse`);
        await clickhouse.insert({
            table: TABLE_KEY,
            values: batchValues,
            format: "JSONEachRow"
        });

        totalInserted += batchValues.length;
        offset += rows.length;
        console.log(`✔ Batch complete. Total so far: ${totalInserted}/${totalRecords}`)
    }
    if(totalRecords >0 && totalInserted >0){
           await updateLastMigratedId(clickhouse, TABLE_KEY, lastId, totalRecords);
console.log(`✔ Migrated up to ID: ${lastId}`);
    console.log(`✅ TOTAL INSERTED INTO CLICKHOUSE: ${totalInserted}`); 
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
      url: 'http://localhost:8123',
    username: 'default',
    password: '',
    database: 'clickHouseInvoice',
    });

    try {
         const providerIds = await getDistinctServiceProviders(mysqlConn);
      console.log(`🔑 Found ${providerIds.length} service providers`);
   for (const providerId of providerIds) {
// const providerId = 2087;
      const tableName = `customers_${providerId}`;
    //   console.log(`\n🚀 Migrating provider ${providerId}`);

      await createInvoiceTable(clickhouse, tableName);
       await migrateCustomers(mysqlConn, clickhouse, providerId);
     }
      
    } finally {
        await mysqlConn.end();
        await clickhouse.close();
    }
}



migrateData();
