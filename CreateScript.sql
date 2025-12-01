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
    booking_type      String,
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
    payment_method_id UInt64,
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
     LoyaltyPoints Decimal(18, 2),
    Referral String,
    created_at DateTime,
    updated_at DateTime,
    deleted_at DateTime DEFAULT NULL
)
ENGINE = MergeTree()
ORDER BY Email;




CREATE TABLE IF NOT EXISTS product_inventory (
    -- Primary identifiers
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
PARTITION BY toYYYYMM(created_at);

CREATE TABLE clickHouseInvoice.serviceprovider
(
    id UInt32,
    serviceCategoryId UInt32,
    serviceCategoryName String,
    legalName String,
    creationDate DateTime,
    expiryDate DateTime,
    hasMembership String   -- 0 = No, 1 = Yes
)
ENGINE = MergeTree()
ORDER BY id;


CREATE TABLE IF NOT EXISTS class_sessions (
    -- Primary Keys
    session_id UInt32,
    class_id UInt32,
    enrollment_id UInt32,
    enrollment_session_id UInt32,
    customer_id UInt32,
    invoice_id UInt32,
    
    -- Service Provider & Location
    service_provider_id UInt32,
    location_id UInt32,
    location_name String,
    
    -- Class Information
    class_name String,
    class_type_id UInt32,
    class_type_name String,
    class_category_id UInt32,
    class_category_name String,
    class_capacity UInt16,
    class_status String,  -- 0=Inactive, 1=Active
    class_enrollment_status String,  -- 1=Enrollment Open, 2=Do Not Publish, 3=Enrollment Closed
    
    -- Session Information
    session_name String,
    session_date Date,
    session_start_time String,
    session_end_time String,
    session_duration String,
    session_status String,  -- 1=Active, 0=Cancelled
    is_parent_session UInt8,
    
    -- Instructor/Resource Information
    resource_id UInt32,
    resource_name String,
    additional_resource_ids String,
    
    -- Customer/Member Information (Simplified)
    customer_member_id UInt32,
    member_name String,
    customer_name String,  -- Combined first + last name
    
    -- Enrollment Details
    enrollment_status String,  -- 0=Deleted, 1=Enrolled, 3=Waiting List
    enrollment_quantity UInt16,
    enrollment_creation_date DateTime,
    booking_method String,  -- 1=Online, 2=PhoneIn, 3=WalkIn
    payment_status UInt8,
    payment_type_id UInt32,
    is_checked_in UInt8,
    
    -- Financial Information (from invoice_items_detail)
    item_price Decimal(10, 2),
    quantity UInt16,
    total_price Decimal(10, 2),
    sale_discount Decimal(10, 2),
    discount_amount Decimal(10, 2),
    tax_amount Decimal(10, 2),
    net_amount Decimal(10, 2),  -- total_price - sale_discount
    
    -- Promotion Information
    promotion_id UInt32,
    promotion_name String,
    
    -- Timestamps
    invoice_created_at DateTime,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(session_date)
ORDER BY (service_provider_id, session_date, class_id, session_id)
SETTINGS index_granularity = 8192;



CREATE TABLE IF NOT EXISTS memberships
(
    -- Primary Keys & IDs
    enrollment_id UInt64,
    membership_id UInt32,
    customer_id UInt32,
    member_id UInt32,
    service_provider_id UInt32,
    subscription_id Nullable(UInt64),
    invoice_id Nullable(UInt64),
    parent_invoice_id Nullable(UInt64),
    location_id Nullable(UInt32),
    franchise_id Nullable(UInt32),
    
    -- Customer & Member Info
    customer_name String,
    member_name String,
    customer_member_id Nullable(UInt64),
    primary_member UInt8,  -- boolean 0/1
    parent_enrollment_id Nullable(UInt64),
    
    -- Membership Details
    membership_name String,
    membership_type String,
    membership_type_id UInt32,
    membership_status String,  -- 'Active', 'Expired', 'Cancelled', etc.
    enrollment_status String,
    hasFull_membership String,
    online_visible UInt8,
    
    -- Subscription Details
    subscription_type String,
    subscription_status String,  -- 'Active', 'Cancelled', 'Suspended'
    auto_renew UInt8,  -- boolean 0/1
    payment_method UInt8,
    
    -- Dates (Critical for Analytics)
    enrollment_date DateTime,
    start_date Date,
    contract_duration_date Nullable(Date),
    expiration_date Nullable(Date),
    next_billing_date Nullable(Date),
    renewal_date Nullable(Date),
    first_renewal_date Nullable(Date),
    next_renewal_date Nullable(Date),
    renewal_notification_date Nullable(Date),
    cancellation_date Nullable(DateTime),
    deleted_date Nullable(DateTime),
    
    -- Financial Data
    membership_price Decimal(10,2),
    recurring_amount Decimal(10,2),
    subscription_amount Decimal(10,2),
    registration_fee Decimal(10,2),
    total_amount Decimal(10,2),
    last_payment_status String,
    last_payment_amount Decimal(10,2),
    last_payment_date Nullable(Date),
    
    -- Membership Configuration
    duration_count UInt16,
    duration_type UInt8,  -- days/months/years
    renewal_type UInt8,
    no_of_members_included UInt16,
    no_of_additional_members UInt16,
    
    -- Auto-Renewal Tracking
    auto_exception UInt8,
    declined_count UInt16,
    no_of_payments Nullable(String),
    payment_day Nullable(UInt8),
    
    -- Cancellation Info
    cancel_reason Nullable(String),
    cancel_notes Nullable(String),
    cancelled_by Nullable(UInt32),
    deleted_by Nullable(UInt32),
    
    -- Churn & Retention Metrics (Calculated)
    days_to_expiration Int32,  -- can be negative if expired
    is_active UInt8,  -- boolean
    is_expiring_30days UInt8,  -- boolean
    is_expiring_60days UInt8,  -- boolean
    is_lapsed UInt8,  -- boolean
    is_auto_renew_failed UInt8,  -- boolean
    days_since_cancellation Nullable(Int32),
    
    -- Categorization
    booking_method UInt8,
    department_id Nullable(UInt32),
    
    -- Timestamps
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(enrollment_date)
ORDER BY (service_provider_id, enrollment_id, enrollment_date)
SETTINGS index_granularity = 8192;
ALTER TABLE memberships ADD INDEX idx_expiration_date expiration_date TYPE minmax GRANULARITY 4;
ALTER TABLE memberships ADD INDEX idx_membership_status membership_status TYPE set(0) GRANULARITY 4;
ALTER TABLE memberships ADD INDEX idx_auto_renew auto_renew TYPE set(0) GRANULARITY 4;