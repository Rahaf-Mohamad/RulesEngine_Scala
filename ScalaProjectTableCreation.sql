CREATE TABLE Scala_Orders (
    order_date DATE,
    expiry_date DATE,
    days_to_expiry NUMBER,
    product_category VARCHAR2(100),
    product_name VARCHAR2(100),
    quantity NUMBER,
    unit_price NUMBER,
    channel VARCHAR2(50),
    payment_method VARCHAR2(50),
    discount NUMBER,
    final_price NUMBER
);
select * from scala_orders
