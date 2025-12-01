import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';

// Configuration
const CONFIG = {
    mysql: {
        host: 'localhost',
        user: 'root',
        password: '',
        database: 'bizzflo'
    },
    clickhouse: {
        url: 'http://localhost:8123',
        database: 'clickHouseInvoice'
    },
    batchSize: 1000,
    serviceProviderId: 22
};

/**
 * Convert dates from MySQL to ClickHouse-friendly ISO format
 */
function formatDate(dateValue) {
    if (!dateValue) return null;
    if (dateValue === '0000-00-00' || dateValue === '0000-00-00 00:00:00') return null;

    const d = new Date(dateValue);
    if (isNaN(d.getTime())) return null;

    return d.toISOString().replace('T', ' ').replace('Z', '');
}

/**
 * Migrate products from MySQL to ClickHouse
 */
async function migrateProducts(mysqlConn, clickhouse) {
    try {
        console.log('Starting products migration...');

        // Load all lookup data upfront
        console.log('Loading lookup data...');
        const [brands] = await mysqlConn.execute(`SELECT id, name FROM productbrand`);
        const [providers] = await mysqlConn.execute(`SELECT id, legalName as name FROM serviceprovider`);
        const [categories] = await mysqlConn.execute(`SELECT id, name FROM productcategory`);
        const [departments] = await mysqlConn.execute(`SELECT id, name FROM productdepartment`);
        const [types] = await mysqlConn.execute(`SELECT id, name FROM producttype`);

        // Create lookup maps
        const brandMap = new Map(brands.map(b => [b.id, b.name]));
        const providerMap = new Map(providers.map(p => [p.id, p.name]));
        const categoryMap = new Map(categories.map(c => [c.id, c.name]));
        const departmentMap = new Map(departments.map(d => [d.id, d.name]));
        const typeMap = new Map(types.map(t => [t.id, t.name]));

        console.log('✓ Lookup data loaded');

        // Fetch products
        const [rows] = await mysqlConn.execute(`
            SELECT 
                id,
                name,
                productBrandId,
                subBrandId,
                serviceProviderId,
                description,
                customId,
                barCode,
                productSerialization,
                regularPrice,
                salePrice,
                storeStatus,
                wholeSalePrice,
                stockStatus,
                stockQuantity,
                category,
                subCategory,
                departmentId,
                typeId,
                caseCost,
                casePrice,
                online,
                avgCost,
                avgSellPrice,
                avgMargin,
                margin,
                reorderLevel
            FROM product
        `);

        console.log(`Fetched ${rows.length} products from MySQL`);

        if (rows.length === 0) {
            console.log('No products to migrate');
            return;
        }

        // Transform and insert data
        await clickhouse.insert({
            table: 'products',
            values: rows.map(r => ({
                id: r.id,
                name: r.name || '',
                product_brand_id: r.productBrandId || 0,
                product_brand_name: brandMap.get(r.productBrandId) || 'N/A',
                sub_brand_id: r.subBrandId || 0,
                sub_brand_name: brandMap.get(r.subBrandId) || 'N/A',
                service_provider_id: r.serviceProviderId || 0,
                service_provider_name: providerMap.get(r.serviceProviderId) || 'N/A',
                description: r.description || '',
                productSerialization: r.productSerialization || '',
                regular_price: r.regularPrice || 0,
                sale_price: r.salePrice || 0,
                store_status: r.storeStatus || '',
                wholesale_price: r.wholeSalePrice || 0,
                stock_status: r.stockStatus ? 'In Stock' : 'Out of Stock',
                stock_quantity: r.stockQuantity || 0,
                category: categoryMap.get(r.category) || 'N/A',
                sub_category: categoryMap.get(r.subCategory) || 'N/A',
                department_id: r.departmentId || 0,
                department_name: departmentMap.get(r.departmentId) || 'N/A',
                type_id: r.typeId || 0,
                type_name: typeMap.get(r.typeId) || 'N/A',
                case_cost: r.caseCost || 0,
                case_price: r.casePrice || 0,
                online: r.online ? 'Online' : 'Retail',
                avg_cost: r.avgCost || 0,
                avg_sell_price: r.avgSellPrice || 0,
                avg_margin: r.avgMargin || 0,
                margin: r.margin || 0,
                sku: r.customId || null,
          upc: r.barCode || null,
          reorderLevel: r.reorderLevel || 0
            })),
            format: 'JSONEachRow'
        });

        console.log(`✓ Products migrated successfully: ${rows.length} records`);

    } catch (err) {
        console.error('✗ Product migration error:', err);
        throw err;
    }
}

/**
 * Main migration function
 */
async function migrateData() {
    let mysqlConn;
    let clickhouse;

    try {
        console.log('=== Starting MySQL to ClickHouse Migration ===\n');

        // Create connections
        mysqlConn = await mysql.createConnection(CONFIG.mysql);
        clickhouse = createClient(CONFIG.clickhouse);

        console.log('✓ Database connections established\n');

        // Run migrations
        await migrateProducts(mysqlConn, clickhouse);

        console.log('\n=== Migration Completed Successfully ===');

    } catch (err) {
        console.error('\n✗ Migration failed:', err);
        process.exit(1);
    } finally {
        // Clean up connections
        if (mysqlConn) await mysqlConn.end();
        if (clickhouse) await clickhouse.close();
        console.log('\n✓ Connections closed');
    }
}

// Run migration
migrateData();