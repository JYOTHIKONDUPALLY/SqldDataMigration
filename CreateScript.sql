CREATE TABLE invoice_items_detail (
    id UInt64,
    invoice_id UInt64 NOT NULL,
    item_type String,
    item_type_id UInt64,
    category String,
    subcategory String,
    brand String,
    deaprtment String,
    SKU String,
    UPC String,
    item_id UInt64,
    item_name String,
    COGS UInt64,
    Commission String,
    co_faet_tax UInt64 DEFAULT 0,,
    guest_pass_discount UInt64 DEFAULT 0,
    membership_discount UInt64 DEFAULT 0,
    package_discount UInt64 DEFAULT 0,
    refund_amount UInt64 DEFAULT 0,
    refund_co_faet_tax UInt64 DEFAULT 0,
    refund_tax UInt64 DEFAULT 0,
    quantity Int32 DEFAULT 1,
    unit_price Decimal(12,2),
    discount_value Decimal(12,2) DEFAULT 0.00,
    discount_amount Decimal(12,2) DEFAULT 0.00,
    tax_rate Decimal(5,2),
    total_price Decimal(12,2) DEFAULT 0.00,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY (invoice_id, id);


CREATE TABLE invoice_details
(
    id                UInt64,
    invoice_date      Date,
    customer_name     String,
    customer_id       UInt64,
    customer_email    String,
    is_member         Enum8('no' = 0, 'yes' = 1),
    company           String,
    sales_clerk       String,
    commission_clerk  String,
    resource          String,
    pos_terminal      String,
    pos_terminal_id   UInt64,
    retail_discount   Decimal(12, 2),
    tax               Decimal(12, 2),
    total_amount      Decimal(12, 2),
    franchise         String,
    franchise_id      UInt64,
    provider          String,
    provider_id       UInt64,
    location          String,
    location_id       UInt64,
    parent_invoice_id UInt64,
    invoice_number    String,
    due_date          Date,
    status            String,
    notes             String,
    created_at        DateTime,
    updated_at        DateTime
)
ENGINE = MergeTree
ORDER BY (id);

CREATE TABLE paymentDetails (
    id UInt64,
    franchise String,
    franchise_id UInt64,
    provider String,
    provider_id UInt64,
    location String,
    location_id UInt64,
    invoice_id UInt64 NOT NULL,
    pos_terminal String,
    pos_terminal_id UInt64,
    payment_date Date,
    amount_paid Decimal(12,2),
    payment_method String,
    refund_amount Decimal(12,2),
    notes String,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, invoice_id, id);

CREATE TABLE customers
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
    LoyaltyPoints UInt32,
    Referral String,
    created_at DateTime,
    updated_at DateTime,
    deleted_at DateTime DEFAULT NULL
)
ENGINE = MergeTree()
ORDER BY Email;
