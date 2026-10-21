SELECT
    100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1+sum(ol_amount)) AS promo_revenue
FROM
    order_line, item
WHERE
    ol_i_id = i_id
    AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
    AND ol_delivery_d < '2030-01-02 00:00:00.000000';
