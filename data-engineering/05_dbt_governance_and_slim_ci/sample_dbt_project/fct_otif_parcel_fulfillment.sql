{{
    config(
        materialized='incremental',
        unique_key='parcel_fulfillment_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'dispatched_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['fulfillment_hub_id', 'carrier_code', 'otif_status'],
        require_partition_filter=true,
        contract={'enforced': true},
        tags=['marts', 'supply_chain', 'otif', 'black_friday_critical']
    )
}}

/*
    Mart: fct_otif_parcel_fulfillment
    Domain: Global Supply Chain & Warehouse Robotics (16.5M+ Parcels)
    Grain: One row per parcel shipment leg (parcel_id + fulfillment_hub_id)
    Purpose: Tracks On-Time In-Full (OTIF) SLA compliance, automated robotics pick-to-pack
             cycle times, and carrier delivery performance across international hubs.
*/

WITH parcel_events AS (
    SELECT
        parcel_id,
        order_id,
        fulfillment_hub_id,
        robotics_zone_id,
        carrier_code,
        service_level,
        CAST(order_placed_at AS TIMESTAMP) AS order_placed_at,
        CAST(pick_completed_at AS TIMESTAMP) AS pick_completed_at,
        CAST(packed_at AS TIMESTAMP) AS packed_at,
        CAST(dispatched_at AS TIMESTAMP) AS dispatched_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        CAST(promised_delivery_at AS TIMESTAMP) AS promised_delivery_at,
        CAST(dispatched_at AS DATE) AS dispatched_date,
        ordered_units,
        fulfilled_units,
        shipping_cost_gbp,
        order_revenue_gbp,
        updated_at
    FROM {{ ref('stg_wms_parcel_shipments') }}
    {% if is_incremental() %}
        WHERE CAST(dispatched_at AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY parcel_id, fulfillment_hub_id
        ORDER BY updated_at DESC
    ) = 1
),

otif_evaluated AS (
    SELECT
        TO_HEX(SHA256(CONCAT(parcel_id, '|', fulfillment_hub_id))) AS parcel_fulfillment_sk,
        parcel_id,
        order_id,
        fulfillment_hub_id,
        robotics_zone_id,
        carrier_code,
        service_level,
        dispatched_date,
        order_placed_at,
        dispatched_at,
        delivered_at,
        promised_delivery_at,
        ordered_units,
        fulfilled_units,
        ROUND(TIMESTAMP_DIFF(packed_at, pick_completed_at, SECOND) / 60.0, 2) AS robotics_pick_to_pack_mins,
        ROUND(TIMESTAMP_DIFF(dispatched_at, order_placed_at, MINUTE) / 60.0, 2) AS warehouse_turnaround_hours,
        CASE
            WHEN delivered_at IS NOT NULL AND delivered_at <= promised_delivery_at THEN TRUE
            ELSE FALSE
        END AS is_on_time,
        CASE
            WHEN fulfilled_units >= ordered_units AND ordered_units > 0 THEN TRUE
            ELSE FALSE
        END AS is_in_full,
        shipping_cost_gbp,
        order_revenue_gbp,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM parcel_events
)

SELECT
    parcel_fulfillment_sk,
    parcel_id,
    order_id,
    fulfillment_hub_id,
    robotics_zone_id,
    carrier_code,
    service_level,
    dispatched_date,
    order_placed_at,
    dispatched_at,
    delivered_at,
    promised_delivery_at,
    ordered_units,
    fulfilled_units,
    robotics_pick_to_pack_mins,
    warehouse_turnaround_hours,
    is_on_time,
    is_in_full,
    (is_on_time AND is_in_full) AS is_otif_compliant,
    CASE
        WHEN is_on_time AND is_in_full THEN 'OTIF_MET'
        WHEN NOT is_on_time AND is_in_full THEN 'LATE_DELIVERY'
        WHEN is_on_time AND NOT is_in_full THEN 'SHORT_SHIPPED'
        ELSE 'OTIF_FAILED_BOTH'
    END AS otif_status,
    shipping_cost_gbp,
    order_revenue_gbp,
    _dbt_updated_at
FROM otif_evaluated
