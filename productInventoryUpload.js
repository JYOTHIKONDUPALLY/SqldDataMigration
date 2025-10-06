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
      // Capture an inventoryId for QoO calculation (preferably the first one)
      if (!inventoryId) {
        inventoryId = row.inventoryId;
      }
    });

    // 3. Get Quantity on Order (QoO) - called ONCE after the loop
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
    JOIN purchaseOrder p ON pi.poId = p.id
    WHERE pi.productId = ?
      AND p.status = 1
      AND pi.status = 1
  `;
  
  const params = [productId];

  // Exclude specific poItem if provided
  if (poItemId) {
    sql += " AND pi.id != ?";
    params.push(poItemId);
  }

  // Apply inventory filter
  if (inventoryId !== null) {
    if (inventoryId === 'all') {
      // No filter for 'all'
    } else {
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
        // Check if this receivedQuantity has been billed
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


/**
 * Main migration function
 */
async function migrateProductInventory( mysqlConn,clickhouse, batchSize = 1000) {
  
  try {

    // Get total count
    const [countResult] = await mysqlConn.execute('SELECT COUNT(*) as total FROM product');
    const totalRecords = countResult[0].total;
    console.log(`📊 Total records to migrate: ${totalRecords}`);

    let offset = 0;
    let totalMigrated = 0;
    let totalErrors = 0;
    const errors = [];

    while (offset < totalRecords) {
      console.log(`\n➡️ Fetching batch: OFFSET=${offset}, LIMIT=${batchSize}`);

      // Fetch products batch
      const [rows] = await mysqlConn.execute(`
        SELECT 
          p.id,
          p.serviceProviderId,
          p.name,
          p.customId,
          p.barcode,
          p.productBrandId,
          p.category as categoryId,
          p.subcategory as subcategoryId,
          p.salePrice,
          p.wholeSalePrice,
          p.regularPrice,
          p.avgCost,
          p.status,
          p.createdDate,
          p.lastUpdated
        FROM product p
        ORDER BY p.id
        LIMIT ? OFFSET ?
      `, [batchSize, offset]);

      console.log(`   🔹 Retrieved ${rows.length} rows from MySQL`);

      if (rows.length === 0) {
        console.log('✅ No more rows to process. Exiting loop.');
        break;
      }

      // Collect unique IDs for batch lookups
      const brandIds = [...new Set(rows.map(r => r.productBrandId).filter(Boolean))];
      const categoryIds = [...new Set(rows.map(r => r.categoryId).filter(Boolean))];
      const subcategoryIds = [...new Set(rows.map(r => r.subcategoryId).filter(Boolean))];
      const providerIds = [...new Set(rows.map(r => r.serviceProviderId).filter(Boolean))];

      // Batch queries for related data
      const [brands] = brandIds.length
        ? await mysqlConn.query(`SELECT id, name FROM productbrand WHERE id IN (?)`, [brandIds])
        : [[]];
      
      const [categories] = categoryIds.length
        ? await mysqlConn.query(`SELECT id, name FROM productcategory WHERE id IN (?)`, [categoryIds])
        : [[]];
      
      const [subcategories] = subcategoryIds.length
        ? await mysqlConn.query(`SELECT id, name FROM productcategory WHERE id IN (?)`, [subcategoryIds])
        : [[]];
      
      const [providers] = providerIds.length
        ? await mysqlConn.query(`SELECT id, legalName as name FROM serviceprovider WHERE id IN (?)`, [providerIds])
        : [[]];

      // Create lookup maps
      const brandMap = Object.fromEntries(brands.map(b => [b.id, b.name]));
      const categoryMap = Object.fromEntries(categories.map(c => [c.id, c.name]));
      const subcategoryMap = Object.fromEntries(subcategories.map(s => [s.id, s.name]));
      const providerMap = Object.fromEntries(providers.map(p => [p.id, p.name]));

      // Process each product
      const processedData = [];
      for (const row of rows) {
        try {
          // Get serial numbers
          const serialNumbers = await getSerialNumbers(mysqlConn, row.id);
          
          // Get product statistics
          const stats = await getProductStats(mysqlConn, row.id, row.serviceProviderId);

        //   console.log( `📊 Product ID: ${row.id}, Serial Numbers: ${serialNumbers.length}, Stats: ${JSON.stringify(stats)}`);

          processedData.push({
            id: parseInt(row.id),
            franchise_id: 0,
            franchise: 'dummy',
            provider_id: parseInt(row.serviceProviderId),
            provider: providerMap[row.serviceProviderId] || 'N/A',
            name: row.name || 'N/A',
            sku: row.customId || 'N/A',
            upc: row.barcode || 'N/A',
            status: row.status || 'active',
            serial: serialNumbers,
            brand_id: parseInt(row.productBrandId),
            brand_name: brandMap[row.productBrandId] || 'N/A',
            category: categoryMap[row.categoryId] || 'Uncategorized',
            sub_category: subcategoryMap[row.subcategoryId] || 'N/A',
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
            
            created_at: formatDate(row.createdDate),
            updated_at: formatDate(row.lastUpdated)
          });
        } catch (rowError) {
          console.error(`   ⚠️ Error processing product ${row.id}:`, rowError.message);
          totalErrors++;
          errors.push({ productId: row.id, error: rowError.message });
        }
      }

      console.log(`   📥 Prepared ${processedData.length} rows for ClickHouse insert`);

      // Insert in sub-batches
      const CHUNK_SIZE = 500;
      for (let i = 0; i < processedData.length; i += CHUNK_SIZE) {
        const chunk = processedData.slice(i, i + CHUNK_SIZE);
        try {
          await clickhouse.insert({
            table: 'product_inventory',
            values: chunk,
            format: 'JSONEachRow'
          });
          console.log(`   ✅ Inserted sub-batch ${i + 1} → ${i + chunk.length}`);
          totalMigrated += chunk.length;
        } catch (insertError) {
          console.error(`   ❌ Sub-batch insert failed (rows ${i + 1} → ${i + chunk.length}):`, insertError.message);
          totalErrors += chunk.length;
          errors.push({ batch: `${i + 1}-${i + chunk.length}`, error: insertError.message });
        }
      }

      offset += batchSize;
      console.log(`➡️ Progress: Migrated=${totalMigrated}, Errors=${totalErrors}`);
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
    return { success: false, error: err.message };
  } finally {
    if (mysqlConn) {
      console.log('MySQL connection closed');
    }
  }
}

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
    await migrateProductInventory(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();
