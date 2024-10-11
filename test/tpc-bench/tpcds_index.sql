CREATE INDEX idx_store_sales_customer ON store_sales (ss_customer_sk);
CREATE INDEX idx_store_sales_sold_date ON store_sales (ss_sold_date_sk);
CREATE INDEX idx_catalog_sales_customer ON catalog_sales (cs_ship_customer_sk);
CREATE INDEX idx_catalog_sales_sold_date ON catalog_sales (cs_sold_date_sk);
CREATE INDEX idx_web_sales_customer ON web_sales (ws_bill_customer_sk);
CREATE INDEX idx_web_sales_sold_date ON web_sales (ws_sold_date_sk);
CREATE INDEX idx_customer_address_county ON customer_address (ca_county);
