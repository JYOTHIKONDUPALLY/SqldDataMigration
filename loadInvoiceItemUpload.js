import mysql from 'mysql2/promise';
import { createClient } from '@clickhouse/client';


/**
 * Get category & subcategory for an item
 * @param {string} type - Item type (service, product, class, package, membership, etc.)
 * @param {number} itemId - The ID of the item in its main table
 * @param {object} mysqlConn - MySQL connection (promise-based)
 * @returns {Promise<{category: string|null, subcategory: string|null}>}
 */
async function getCategoryAndSubcategory(type, itemId, mysqlConn) {
  let category = null;
  let subcategory = null;

  switch (type.toLowerCase()) {
    // ---------------------- SERVICE ----------------------
    case "service":
    case "appointment":
    case "advancebookingfee": {
      const [[service]] = await mysqlConn.execute(
        "SELECT serviceTypeId FROM service WHERE id = ?",
        [itemId]
      );
      if (service?.serviceTypeId) {
        const [[stype]] = await mysqlConn.execute(
          "SELECT serviceCategoryId FROM servicetype WHERE id = ?",
          [service.serviceTypeId]
        );
        if (stype?.serviceCategoryId) {
          const [[scat]] = await mysqlConn.execute(
            "SELECT name FROM servicecategory WHERE id = ?",
            [stype.serviceCategoryId]
          );
          category = scat?.name || null;
        }
      }
      break;
    }

    // ---------------------- PRODUCT ----------------------
    case "product":
    case "rental":
    case "membershiprental": {
      let productId = itemId;
      if (type === "rental") {
        category = "";
        subcategory = "";
        break;
      }
      if (productId) {
        const [[product]] = await mysqlConn.execute(
          "SELECT category, subCategory FROM product WHERE id = ?",
          [productId]
        );
        if (product?.category) {
          const [[cat]] = await mysqlConn.execute(
            "SELECT name FROM productcategory WHERE id = ?",
            [product.category]
          );
          category = cat?.name || null;
        }
        if (product?.subCategory) {
          const [[subcat]] = await mysqlConn.execute(
            "SELECT name FROM productcategory WHERE id = ?",
            [product.subCategory]
          );
          subcategory = subcat?.name || null;
        }
      }
      break;
    }

    // ---------------------- FORFEITED DEPOSIT ----------------------
    case "forfeiteddeposit":
      category = "";
      subcategory = "";
      break;

    // ---------------------- CLASS ----------------------
    case "class":
    case "classes": {
      const [[classRow]] = await mysqlConn.execute(
        "SELECT classCategoryId FROM class WHERE id = ?",
        [itemId]
      );
      if (classRow?.classCategoryId) {
        const [[ccat]] = await mysqlConn.execute(
          "SELECT name FROM classcategory WHERE id = ?",
          [classRow.classCategoryId]
        );
        category = ccat?.name || null;
      }
      subcategory = null; // no subcategory
      break;
    }

    // ---------------------- PACKAGE ----------------------
    case "package":
    case "packages": {
      const [[pkg]] = await mysqlConn.execute(
        "SELECT type FROM package WHERE id = ?",
        [itemId]
      );
      if (pkg?.type) {
        const [[ptype]] = await mysqlConn.execute(
          "SELECT type AS name FROM packagetype WHERE id = ?",
          [pkg.type]
        );
        category = ptype?.name || null;
      }
      subcategory = null;
      break;
    }

    // ---------------------- MEMBERSHIP ----------------------
    case "membership":
    case "memberships": {
      const [[membership]] = await mysqlConn.execute(
        "SELECT type FROM membership WHERE id = ?",
        [itemId]
      );
      if (membership?.type) {
        const [[mtype]] = await mysqlConn.execute(
          "SELECT type FROM membershiptype WHERE id = ?",
          [membership.type]
        );
        category = mtype?.type || "Membership";
      } else {
        category = "Membership";
      }
      subcategory = null;
      break;
    }

    // ---------------------- GIFT CARD ----------------------
    case "giftcard": {
      const [[gift]] = await mysqlConn.execute(
        "SELECT itemType FROM giftcard WHERE id = ?",
        [itemId]
      );
      category = gift?.itemType || "Gift Card";
      subcategory = null;
      break;
    }

    // ---------------------- WARRANTY ----------------------
    case "warranty": {
      const [[warranty]] = await mysqlConn.execute(
        "SELECT type FROM warranty WHERE id = ?",
        [itemId]
      );
      if (warranty?.type === 1) category = "Limited";
      else if (warranty?.type === 2) category = "Lifetime";
      else category = "Warranty";
      subcategory = null;
      break;
    }

    // ---------------------- TRADE-IN ----------------------
    case "tradein":
      category = "Trade-in";
      subcategory = null;
      break;

    // ---------------------- MISC ----------------------
    default: {
      if (type.startsWith("misc")) {
        const [[misc]] = await mysqlConn.execute(
          "SELECT type FROM misc WHERE id = ?",
          [itemId]
        );
        category = misc?.type || "Miscellaneous Items";
        subcategory = null;
      }
    }
  }

  return { category, subcategory };
}

async function deriveItemName(type, itemId, rawName, mysqlConn) {
  if (!type || !itemId) return removeItemPrefix(rawName);

  switch (type.toLowerCase()) {

    case "service":
    case "appointment":
    case "advancebookingfee": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM service WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "class":
    case "classes": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM class WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "package":
    case "packages": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM package WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "membership":
    case "memberships":
    case "membershipregistrationfee": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM membership WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "product":
    case "membershiprental": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM product WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "rental": {
      const [[rental]] = await mysqlConn.execute(
        "SELECT productId FROM rentalItems WHERE id = ?",
        [itemId]
      );
      if (rental?.productId) {
        const [[prod]] = await mysqlConn.execute(
          "SELECT name FROM product WHERE id = ?",
          [rental.productId]
        );
        return prod?.name || removeItemPrefix(rawName);
      }
      return removeItemPrefix(rawName);
    }

    case "giftcard": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM giftCard WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "warranty": {
      const [[row]] = await mysqlConn.execute(
        "SELECT name FROM warranty WHERE id = ?",
        [itemId]
      );
      return row?.name || removeItemPrefix(rawName);
    }

    case "tradein":
      return "Trade-in";

    case "forfeiteddeposit":
      return "Forfeited Deposit";

    default:
      if (type.toLowerCase().startsWith("misc")) {
        return "Miscellaneous Item";
      }
      return removeItemPrefix(rawName);
  }
}

function normalizeItemType(type) {
  if (!type) return type;

  const t = type.toLowerCase();

  if (t === 'advancebookingfee') return 'service';
  if (t === 'classes') return 'class';
  if (t === 'packages') return 'package';
  if (t === 'membershipregistrationfee') return 'membership';

  if (t.startsWith('misc')) return 'misc';

  return t;
}
  function removeItemPrefix(name) {
    if (!name) return name;
    const str = String(name).trim();
    const prefixes = ['product:', 'service:', 'class:', 'membership:', 'appointment:', 'package:'];
    for (const prefix of prefixes) {
      if (str.toLowerCase().startsWith(prefix.toLowerCase())) {
        return str.substring(prefix.length).trim();
      }
    }
    return str;
  }

async function migrateInvoiceItems(mysqlConn, clickhouse, batchSize = 1000) {
  const safeNum = (v, def = 0) =>
    v === null || v === undefined || v === '' || isNaN(Number(v)) ? def : Number(v);
  const safeStr = (v, def = '') => (v === null || v === undefined ? def : String(v));

  // Format dates for ClickHouse (DateTime)
  function formatDate(dateValue) {
    if (!dateValue) return new Date().toISOString().slice(0, 19).replace('T', ' ');
    let date = new Date(dateValue);
    if (isNaN(date.getTime())) return new Date().toISOString().slice(0, 19).replace('T', ' ');
    return date.toISOString().slice(0, 19).replace('T', ' ');
  }

  try {
    // ✅ Preload brand/category/subcategory mappings
    const [brands] = await mysqlConn.execute(`SELECT id, name FROM productbrand`);
    const [categories] = await mysqlConn.execute(`SELECT id, name FROM productcategory`);
    const [subcategories] = await mysqlConn.execute(`SELECT id, name FROM productcategory`);

    const brandMap = new Map(brands.map(b => [b.name.toLowerCase(), b.id]));
    const categoryMap = new Map(categories.map(c => [c.name.toLowerCase(), c.id]));
    const subCategoryMap = new Map(subcategories.map(s => [s.name.toLowerCase(), s.id]));

    // Count total rows
    const [countResult] = await mysqlConn.execute(`SELECT COUNT(*) as total FROM invoiceItemNew where serviceProviderId = 22`);
    const totalRecords = countResult[0].total;
    console.log(`Total invoice items to migrate: ${totalRecords}`);

    let offset = 0;
    while (offset < totalRecords) {
      const [rows] = await mysqlConn.execute(
        `SELECT i.*
         FROM invoiceItemNew i
         where i.serviceProviderId = 22
         ORDER BY i.id
         LIMIT ${batchSize} OFFSET ${offset}`
      );

      const data = [];
      for (const r of rows) {
        // console.log(`processing datat row id: ${JSON.stringify(r)}`);
        // console.log(`Processing invoice item ID: ${r.id}, Type: ${r.type}, Item type ID: ${r.itemTypeId} with cogs: ${r.cogs}, commission: ${r.commission}`);
        // --- brand/sku/upc lookup for products ---
        let brandName = "N/A";
        let skuValue = null;
        let upcValue = null;
        const rawInvoiceItemName = safeStr(r.itemName);

        const normalizedType = normalizeItemType(r.type);
        const derivedItemName = await deriveItemName(
          normalizedType,
          r.itemId,
          rawInvoiceItemName,
          mysqlConn
        );


        if (r.type === "product" && r.itemId) {
          const [[productRow]] = await mysqlConn.execute(
            "SELECT productBrandId, customId, barCode FROM product WHERE id = ?",
            [r.itemId]
          );
          if (productRow) {
            skuValue = productRow.customId || null;
            upcValue = productRow.barCode || null;
            if (productRow.productBrandId) {
              const [[brandRow]] = await mysqlConn.execute(
                "SELECT name FROM productbrand WHERE id = ?",
                [productRow.productBrandId]
              );
              brandName = brandRow?.name || "N/A";
            }
          }

          // console.log(` Product ID ${r.itemId} → Brand: ${brandName}, SKU: ${skuValue}, UPC: ${upcValue}`);
        }

        // --- category/subcategory ---
        const categoryData = await getCategoryAndSubcategory(r.type, r.itemId, mysqlConn);

        // --- discounts ---
        async function calcRedeemDiscount(type) {
          const [redeemRows] = await mysqlConn.execute(
            "SELECT id FROM invoiceItemNew WHERE type = ? AND parentInvoiceItemId = ? AND status = 1",
            [type, r.itemId]
          );
          if (redeemRows.length === 0) return 0;

          let total = 0;
          for (const redeem of redeemRows) {
            const [cashSales] = await mysqlConn.execute(
              "SELECT amount FROM cashSaleInvoice WHERE invoiceItemId = ? AND status = 1",
              [redeem.id]
            );
            for (const cs of cashSales) {
              total += Number(cs.amount) || 0;
            }
          }
          return total;
        }

        const membershipDiscount = await calcRedeemDiscount("membershipRedeem");
        const packageDiscount = await calcRedeemDiscount("packageRedeem");
        const guestpassDiscount = await calcRedeemDiscount("guestpassRedeem");
        const [CogsData] = await mysqlConn.execute(
    `SELECT c.cogs, c.commission 
     FROM cashSaleInvoice c 
     WHERE c.invoiceItemId = ?`,
    [r.id]
  );

  const { Cogs, Commission } = CogsData[0] || {};
        // --- build record ---
         data.push({
          id: r.id,
          invoice_id: r.invoiceId,
          item_type_id: r.itemTypeId || 0,
          item_type_raw: r.type,
          item_type: normalizedType,

          category: categoryData.category || "N/A",
          sub_category: categoryData.subcategory || "N/A",
          brand: brandName,
          SKU: skuValue || null,
          UPC: upcValue || null,
          item_id: r.itemId,
          item_name: removeItemPrefix(safeStr(r.itemName)),
          invoice_item_name: rawInvoiceItemName,
          item_name: derivedItemName,
          quantity: safeNum(r.qty),
          unit_price: safeNum(r.price),
          discount_amount: safeNum(r.discount),
          discount_value: safeNum(r.saleDiscount || r.discount),
          tax_rate: safeNum(r.tax || r.GST || r.PST || r.HST || r.QST).toFixed(2),
          total_price: safeNum(r.totalPrice),
          resource_id: r.resourceId || 0,
          department_id: r.departmentId || 0,
          cogs: Cogs,
          co_faet_tax: Math.round(safeNum(r.pifTax)),
          commission: Commission,
          guest_pass_discount: guestpassDiscount,
          membership_discount: membershipDiscount,
          package_discount: packageDiscount,
          refund_amount: r.refund_amount < 0 ? 0 : Math.round(safeNum(r.returnAmount || r.totalReturnAmount)),
          refund_tax: Math.round(safeNum(r.taxReturnAmount)),
          refund_co_faet_tax: safeNum(r.pifTaxReturnAmount),
          created_at: formatDate(r.createdDate),
          updated_at: formatDate(r.checkoutDate || r.shipOrPickupDate || new Date())
        });
      }

      // Insert rows into ClickHouse `invoice_items` table
      if (data.length > 0) {
        try {
          await clickhouse.insert({
            table: 'invoice_items_detail',
            values: data,
            format: 'JSONEachRow',
          });
          console.log(` Migrated batch ${offset + 1} → ${offset + data.length}`);
        } catch (insertErr) {
          console.error('❌ Batch insert failed, falling back to individual inserts:', insertErr.message);
          for (const rec of data) {
            try {
              await clickhouse.insert({
                table: 'invoice_items_detail',
                values: [rec],
                format: 'JSONEachRow',
              });
            } catch (rowErr) {
              console.error(`⚠️ Failed to insert invoice_item ID ${rec.id}:`, rowErr.message);
            }
          }
        }
      }

      offset += batchSize;
    }

    console.log('🎉 Invoice items migration completed!');
  } catch (err) {
    console.error('❌ Invoice items migration error:', err);
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
    await migrateInvoiceItems(mysqlConn, clickhouse);
  } finally {
    await mysqlConn.end();
    await clickhouse.close();
  }
}

migrateData();