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
 * Get serial numbers for a product
 */
async function getSerialNumbers(mysqlConn, productId) {
  try {
    const query = `
      SELECT ps.serialNumber
      FROM productInventorySerialNumbers ps
      JOIN productInventory pi ON ps.inventoryId = pi.id
      WHERE pi.status = 1
        AND ps.productId = ?
        AND ps.status = 1
        AND ps.inventoryStatus NOT IN (
          'SOLD','TRANSFER','TRANSFER-FFL','TRANSFER-C',
          'REPAIRED','INACTIVE','DESTROYED','LOST OR STOLEN',
          'External Repair Out','CANCELED'
        )
    `;
    
    const [rows] = await mysqlConn.execute(query, [productId]);
    return rows.map(r => r.serialNumber).join(', ') || 'N/A';
  } catch (err) {
    console.error(`Error fetching serial numbers for product ${productId}:`, err.message);
    return 'N/A';
  }
}

async function getProductStats(mysqlConn, productId, serviceProviderId) {
  try {
    // 1. Fetch base product data
    const [products] = await mysqlConn.execute(
      `SELECT id, avgCost, wholeSalePrice, salePrice, regularPrice
       FROM product
       WHERE id = ? AND serviceProviderId = ?`,
      [productId, serviceProviderId]
    );

    if (products.length === 0) {
      return {
        qoh: 0, qor: 0, rental: 0, qoo: 0,
        avgCost: 0, salePrice: 0,
        grossProfitPercent: 0, extendedCost: 0, extendedPrice: 0
      };
    }

    const product = products[0];
    const salePrice = product.salePrice || product.regularPrice || 0;
    const avgCost = product.avgCost > 0 ? product.avgCost : product.wholeSalePrice || 0;

    // 2. Fetch inventory records and calculate quantities
    const [inventory] = await mysqlConn.execute(
      `SELECT ps.inventoryStatus, ps.inventoryId
       FROM productInventorySerialNumbers ps
       JOIN productInventory pi ON ps.inventoryId = pi.id
       WHERE pi.productId = ?
         AND pi.status = 1
         AND ps.status = 1
         AND ps.inventoryStatus NOT IN (
           'SOLD','TRANSFER','TRANSFER-FFL','TRANSFER-C',
           'REPAIRED','INACTIVE','DESTROYED','LOST OR STOLEN',
           'External Repair Out','CANCELED'
         )`,
      [productId]
    );

    let qoh = 0, qor = 0, rental = 0;
    let inventoryId = null;
    
    inventory.forEach(row => {
      if (row.inventoryStatus === 'Reserved For Layaway' || row.inventoryStatus === 'RESERVED') {
        qor++;
      } else if (row.inventoryStatus === 'RENTAL') {
        rental++;
      } else {
        qoh++;
      }
      if (!inventoryId) {
        inventoryId = row.inventoryId;
      }
    });

    // 3. Get Quantity on Order (QoO)
    const qoo = await getQuantityOnOrder(mysqlConn, productId, null, inventoryId);

    // 4. Calculations
    const grossProfitPercent = salePrice > 0
      ? ((salePrice - product.wholeSalePrice) / salePrice) * 100
      : 0;

    const extendedCost = (qoh + qor) * avgCost;
    const extendedPrice = (qoh + qor) * salePrice;

    return {
      qoh,
      qor,
      rental,
      qoo,
      avgCost,
      salePrice,
      grossProfitPercent: parseFloat(grossProfitPercent.toFixed(2)),
      extendedCost: parseFloat(extendedCost.toFixed(2)),
      extendedPrice: parseFloat(extendedPrice.toFixed(2))
    };
  } catch (err) {
    console.error(`Error getting product stats for ${productId}:`, err.message);
    return {
      qoh: 0, qor: 0, rental: 0, qoo: 0,
      avgCost: 0, salePrice: 0,
      grossProfitPercent: 0, extendedCost: 0, extendedPrice: 0
    };
  }
}

async function getQuantityOnOrder(mysqlConn, productId, poItemId = null, inventoryId = null) {
  let sql = `
    SELECT pi.id, pi.orderedQuantity, pi.receivedQuantity
    FROM poItems pi
    INNER JOIN purchaseOrder p ON pi.poId = p.id
    WHERE pi.productId = ?
      AND p.status = 1
      AND pi.status = 1
  `;
  
  const params = [productId];

  if (poItemId) {
    sql += " AND pi.id != ?";
    params.push(poItemId);
  }

  if (inventoryId !== null) {
    if (inventoryId !== 'all') {
      sql += " AND pi.inventoryId = ?";
      params.push(inventoryId);
    }
  } else {
    sql += " AND (pi.inventoryId IS NULL OR pi.inventoryId = '' OR pi.inventoryId = 0)";
  }

  sql += " ORDER BY pi.id DESC";

  const [rows] = await mysqlConn.execute(sql, params);
  
  let qty = 0;
  
  for (const item of rows) {
    if (item.orderedQuantity) {
      let receivedQuantity = 0;
      
      if (item.receivedQuantity) {
        const isBilled = await checkReceivedQuantityIsBilled(mysqlConn, item.id);
        if (isBilled) {
          receivedQuantity = item.receivedQuantity;
        }
      }
      
      let qtyR = item.orderedQuantity - receivedQuantity;
      if (qtyR < 0) qtyR = 0;
      qty += qtyR;
    }
  }
  
  return qty;
}

async function checkReceivedQuantityIsBilled(mysqlConn, poItemId) {
  const [rows] = await mysqlConn.execute(
    "SELECT id FROM billItems WHERE poItemId = ? AND status = 1 LIMIT 1",
    [poItemId]
  );
  
  return rows.length > 0 ? 1 : 0;
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
        id UInt64,
    franchise_id UInt64,
    provider_id UInt64,
    franchise String,
    provider String,
    status String,
    -- Product details
    name String,
    sku String,
    upc String,
    serial String,
    brand_id UInt64,
    brand_name String,
    productSerialization Int32,
    department_id UInt32,
    department_name String,
    -- Categorization
    category String,
    sub_category String,
    -- Pricing
    avg_cost Decimal(10, 2),
    avg_sell_price Decimal(10, 2),
    avg_margin Decimal(10, 2),
    margin Decimal(10, 2),
    type_id UInt32,
    type_name String,
    case_cost Decimal(10, 2),
    case_price Decimal(10, 2),
    online String,
     regular_price Decimal(10, 2),
    sale_price Decimal(10, 2),
    store_status String,
    wholesale_price Decimal(10, 2),
    stock_status String,
    stock_quantity Int32,
    price Decimal64(2),
    cost Decimal64(2),
    average_cost Decimal64(2),
    -- Calculated fields
    gross_profit_percent Decimal64(2),
    extended_cost Decimal64(2),
    extended_price Decimal64(2),
    -- Inventory quantities
    qoh Int32,  -- Quantity on Hand
    rental Int32,
    qor Int32,  -- Quantity on Reserved
    qoo Int32,  -- Quantity on Order
    reorderLevel Int32,
    -- Audit fields
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY ( id)
PARTITION BY toYYYYMM(created_at)
  `;

  await clickhouse.exec({ query: createQuery });
  console.log(`📦 Table ready: ${tableName}`);
}

/**
 * Main migration function
 */
async function migrateProductInventory(mysqlConn, clickhouse, serviceProviderId,batchSize = 1000) {
  
  try {
    const TABLE_KEY = `product_inventory_${serviceProviderId}`;
    let lastId = await getLastMigratedId(clickhouse, TABLE_KEY);
        console.log(`▶ Resuming migration from product.id > ${lastId}`);
 // Use the same ID for query and stats

    // Get total count
    const [countResult] = await mysqlConn.execute(
      'SELECT COUNT(*) as total FROM product WHERE serviceProviderId = ? and status = 1 and product.id > ? ',
      [serviceProviderId, lastId]
    );
    const totalRecords = countResult[0].total;
    console.log(`Total records to migrate: ${totalRecords}`);

    let offset = 0;
    let totalMigrated = 0;
    let totalErrors = 0;
    const errors = [];

    while (offset < totalRecords) {
      console.log(`\nFetching batch: OFFSET=${offset}, LIMIT=${batchSize}`);

      // FIXED: Use query() instead of execute() for dynamic LIMIT/OFFSET
      // Or build the query string with offset/limit values directly
      const [rows] = await mysqlConn.query(`
        SELECT 
          p.id,
          p.serviceProviderId,
          p.name,
          p.customId,
          p.barcode,
          p.stockStatus,
          p.stockQuantity,
          p.productBrandId,
          p.category as categoryId,
          p.subcategory as subcategoryId,
          p.departmentId,
          p.typeId,
          p.caseCost,
          p.casePrice,
          p.online,
          p.salePrice,
          p.wholeSalePrice,
          p.regularPrice,
          p.avgCost,
          p.avgSellPrice,
          p.avgMargin,
          p.margin,
          p.reorderLevel,
          p.status,
          p.createdDate,
          p.lastUpdated
        FROM product p
        WHERE p.serviceProviderId = ?
          AND p.status = 1 and p.id > ${lastId}
        ORDER BY p.id
        LIMIT ${batchSize}
      `, [serviceProviderId]);

      console.log(`  🔹 Retrieved ${rows.length} rows from MySQL`);

      if (rows.length === 0) {
        console.log('No more rows to process. Exiting loop.');
        break;
      }

      // Collect unique IDs for batch lookups
      const brandIds = [...new Set(rows.map(r => r.productBrandId).filter(Boolean))];
      const categoryIds = [...new Set(rows.map(r => r.categoryId).filter(Boolean))];
      const subcategoryIds = [...new Set(rows.map(r => r.subcategoryId).filter(Boolean))];
      const providerIds = [...new Set(rows.map(r => r.serviceProviderId).filter(Boolean))];

      // Batch queries with proper IN clause handling
      const [brands] = brandIds.length > 0
        ? await mysqlConn.query(
            `SELECT id, name FROM productBrand WHERE id IN (${brandIds.map(() => '?').join(',')})`,
            brandIds
          )
        : [[]];
      
      const [categories] = categoryIds.length > 0
        ? await mysqlConn.query(
            `SELECT id, name FROM productCategory WHERE id IN (${categoryIds.map(() => '?').join(',')})`,
            categoryIds
          )
        : [[]];
      
      const [subcategories] = subcategoryIds.length > 0
        ? await mysqlConn.query(
            `SELECT id, name FROM productCategory WHERE id IN (${subcategoryIds.map(() => '?').join(',')})`,
            subcategoryIds
          )
        : [[]];
      
      const [providers] = providerIds.length > 0
        ? await mysqlConn.query(
            `SELECT id, legalName as name FROM serviceProvider WHERE id IN (${providerIds.map(() => '?').join(',')})`,
            providerIds
          )
        : [[]];

      const [departments] = await mysqlConn.execute(`SELECT id, name FROM productDepartment`);
      const [types] = await mysqlConn.execute(`SELECT id, name FROM productType`);

      // Create lookup maps
      const brandMap = Object.fromEntries(brands.map(b => [b.id, b.name]));
      const categoryMap = Object.fromEntries(categories.map(c => [c.id, c.name]));
      const subcategoryMap = Object.fromEntries(subcategories.map(s => [s.id, s.name]));
      const providerMap = Object.fromEntries(providers.map(p => [p.id, p.name]));
      const departmentMap = new Map(departments.map(d => [d.id, d.name]));
      const typeMap = new Map(types.map(t => [t.id, t.name]));

      // Process each product
      const processedData = [];
      for (const row of rows) {
        try {
          lastId = row.id;
          // Get serial numbers
          const serialNumbers = await getSerialNumbers(mysqlConn, row.id);
          
          // Get product statistics
          const stats = await getProductStats(mysqlConn, row.id, serviceProviderId);

          processedData.push({
            id: parseInt(row.id),
            franchise_id: 0,
            franchise: 'dummy',
            provider_id: parseInt(serviceProviderId),
            provider: providerMap[serviceProviderId] || 'N/A',
            name: row.name || 'N/A',
            sku: row.customId || 'N/A',
            upc: row.barcode || 'N/A',
            status: row.status || 'active',
            serial: serialNumbers,
            productSerialization: '',
            brand_id: parseInt(row.productBrandId) || 0,
            brand_name: brandMap[row.productBrandId] || 'N/A',
            category: categoryMap[row.categoryId] || 'Uncategorized',
            sub_category: subcategoryMap[row.subcategoryId] || 'N/A',
            regularPrice: parseFloat(row.regularPrice) || 0,
            sale_price: parseFloat(row.salePrice) || 0,
            price: parseFloat(stats.salePrice) || 0,
            cost: parseFloat(row.wholeSalePrice) || 0,
            average_cost: parseFloat(stats.avgCost) || 0,
            gross_profit_percent: stats.grossProfitPercent,
            extended_cost: stats.extendedCost,
            extended_price: stats.extendedPrice,
            qoh: stats.qoh,
            rental: stats.rental,
            qor: stats.qor,
            qoo: stats.qoo,
            store_status: '',
            wholesale_price: parseFloat(row.wholeSalePrice) || 0,
            stock_status: row.stockStatus ? 'In Stock' : 'Out of Stock',
            stock_quantity: row.stockQuantity || 0,
            reorderLevel: row.reorderLevel || 0,
            avg_margin: parseFloat(row.avgMargin) || 0,
            margin: parseFloat(row.margin) || 0,
            department_id: row.departmentId || 0,
            department_name: departmentMap.get(row.departmentId) || 'N/A',
            type_id: row.typeId || 0,
            type_name: typeMap.get(row.typeId) || 'N/A',
            case_cost: parseFloat(row.caseCost) || 0,
            case_price: parseFloat(row.casePrice) || 0,
            online: row.online ? 'Online' : 'Retail',
            avg_cost: parseFloat(row.avgCost) || 0,
            avg_sell_price: parseFloat(row.avgSellPrice) || 0,
            created_at: formatDate(row.createdDate),
            updated_at: formatDate(row.lastUpdated)
          });
        } catch (rowError) {
          console.error(`  ⚠️ Error processing product ${row.id}:`, rowError.message);
          totalErrors++;
          errors.push({ productId: row.id, error: rowError.message });
        }
      }

      console.log(`  📥 Prepared ${processedData.length} rows for ClickHouse insert`);

      // Insert in sub-batches
      const CHUNK_SIZE = 500;
      for (let i = 0; i < processedData.length; i += CHUNK_SIZE) {
        const chunk = processedData.slice(i, i + CHUNK_SIZE);
        try {
          await clickhouse.insert({
            table: TABLE_KEY,
            values: chunk,
            format: 'JSONEachRow'
          });
          console.log(`  ✅ Inserted sub-batch ${i + 1} → ${i + chunk.length}`);
          totalMigrated += chunk.length;
        } catch (insertError) {
          console.error(`  ❌ Sub-batch insert failed (rows ${i + 1} → ${i + chunk.length}):`, insertError.message);
          totalErrors += chunk.length;
          errors.push({ batch: `${i + 1}-${i + chunk.length}`, error: insertError.message });
        }
      }

      offset += batchSize;
      console.log(`➡️ Progress: Migrated=${totalMigrated}, Errors=${totalErrors}`);
    }
if((totalMigrated > 0 && totalErrors === 0)){
     await updateLastMigratedId(clickhouse, TABLE_KEY, lastId, totalRecords);
console.log(`✔ Migrated up to ID: ${lastId}`);
}else if(totalErrors==0 && totalMigrated==0){
  console.log(`✔ No records found`);
}else{
console.log(`❌ Migration failed with ${totalErrors} errors:`, errors);
      return { success: false, totalRecords, migrated: totalMigrated, errors: totalErrors };
    }
    console.log(`\n🏁 Migration finished: ${totalMigrated} migrated, ${totalErrors} errors`);
    
    if (errors.length > 0) {
      console.log('\n❌ Errors encountered:');
      errors.slice(0, 10).forEach(e => console.log(e));
      if (errors.length > 10) {
        console.log(`... and ${errors.length - 10} more errors`);
      }
    }

    return { 
      success: totalErrors === 0, 
      totalRecords, 
      migrated: totalMigrated, 
      errors: totalErrors,
      errorDetails: errors 
    };

  } catch (err) {
    console.error('💥 Critical error in migrateProductInventory:', err.message);
    console.error(err.stack);
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
      const tableName = `product_inventory_${providerId}`;
      console.log(`\n🚀 Migrating provider ${providerId}`);
      await createInvoiceTable(clickhouse, tableName);
    await migrateProductInventory(mysqlConn, clickhouse, providerId);
       }
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
    console.log('MySQL and ClickHouse connections closed');
  }
}

migrateData();