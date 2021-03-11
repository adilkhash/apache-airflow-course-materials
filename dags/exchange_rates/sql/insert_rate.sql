INSERT INTO
    currency_exchange_rates
VALUES ('{{ params.base_currency }}', '{{ params.currency }}', {{ ti.xcom_pull(task_ids="get_rate") }}, '{{ execution_date.strftime("%Y-%m-%d") }}')
ON CONFLICT (base, currency, date) DO
    UPDATE
        SET rate = excluded.rate;
