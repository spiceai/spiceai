WITH revenue0 (supplier_no, total_revenue) AS (
    SELECT
        mod((s_w_id * s_i_id), 10000) AS supplier_no,
        sum(ol_amount) AS total_revenue
    FROM
        order_line, stock
    WHERE
        ol_i_id = s_i_id
        AND ol_supply_w_id = s_w_id
        AND ol_delivery_d >= '2007-01-02 00:00:00.000000'
    GROUP BY
        supplier_no
)
SELECT
    su_suppkey, su_name, su_address, su_phone, total_revenue
FROM
    supplier, revenue0
WHERE
    su_suppkey = supplier_no
    AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
ORDER BY
    su_suppkey;
