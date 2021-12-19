view: demo_pop_stocks_data {
  label: "[Test] Demo: PoP Development of Stocks Data"

  derived_table: {
    sql:
    SELECT
    "last_period" as period,
    datediff(cast({% date_end date_filter %} as timestamp), cast({% date_start date_filter %} as timestamp)) as period_range,
    a.*, c.stock_name as company, c.sector as sector
    FROM dramadanti.demo_all_stocks a
    LEFT JOIN dramadanti.stock_lookup c ON a.name = c.stock_symbol
    WHERE {% condition date_filter %} a.ymd {% endcondition %}
    AND {% condition sector_filter %} c.sector {% endcondition %}
    UNION ALL
    SELECT
    "prev_period" as period,
    datediff(cast({% date_end date_filter %} as timestamp), cast({% date_start date_filter %} as timestamp)) as period_range,
    b.*, d.stock_name as company, d.sector as sector
    FROM dramadanti.demo_all_stocks b
    LEFT JOIN dramadanti.stock_lookup d ON b.name = d.stock_symbol
    WHERE
    b.ymd >= date_add(cast({% date_start date_filter %} as timestamp), -datediff(cast({% date_end date_filter %} as timestamp), cast({% date_start date_filter %} as timestamp)))
    AND b.ymd < cast({% date_start date_filter %} as timestamp)
    AND {% condition sector_filter %} d.sector {% endcondition %}
    ;;
  }

  suggestions: no

  # Data Source: https://github.com/CNuge/kaggle-code/tree/master/stock_data
  # All the files have the following columns:
  # Date - in format: yy-mm-dd
  # Open - price of the stock at market open (this is NYSE data so all in USD)
  # High - Highest price reached in the day
  # Low Close - Lowest price reached in the day
  # Volume - Number of shares traded
  # Name - the stock's ticker name

  ### Filter & Parameter ###

  filter: date_filter {
    type: date
    datatype: date
  }

  filter: sector_filter {
    type: string
    case_sensitive: no
    suggestions: ["Communication Services", "Consumer Discretionary", "Consumer Staples", "Energy", "Financials",
      "Health Care", "Industrials", "Information Technology", "Materials", "Real Estate", "Utilities"]
  }

  parameter: price_type {
    type: unquoted
    hidden: no
    allowed_value: {
      label: "Opening Price"
      value: "open"
    }
    allowed_value: {
      label: "Closing Price"
      value: "close"
    }
    allowed_value: {
      label: "High Price"
      value: "high"
    }
    allowed_value: {
      label: "Low Price"
      value: "low"
    }
    default_value: "close"
  }


  ### Date Dimension ###

  dimension: ymd {
    group_label: "Datetime"
    type: date
    hidden: no
    sql: cast(${TABLE}.ymd as timestamp) ;;
  }

  dimension: now {
    group_label: "Datetime"
    type: date
    hidden: yes
    sql: now() ;;
  }

  dimension: now_diff {
    group_label: "Datetime"
    type:  number
    hidden: yes
    sql:  mod(datediff(${now},${time_stamp_date}),365);;
  }

  dimension: year {
    group_label: "Datetime"
    type: number
    label: "Year"
    sql: substr(${TABLE}.ymd,1,4) ;;
  }

  dimension: month {
    group_label: "Datetime"
    type: string
    label: "Month YoY"
    sql: substr(${TABLE}.ymd,6,2);;
  }


  dimension: month_name {
    group_label: "Datetime"
    type: date_month_name
    label: "Month Name"
    sql: ${date};;
  }

## TODO: change this into the right format
  dimension: date_of_year {
    group_label: "Datetime"
    type: string
    label: "Date of Year"
    sql: substr(${TABLE}.ymd,6,5);;
  }


  dimension: all {
    type: string
    hidden: no
    label: "All"
    sql: "All";;
  }

  dimension: date_yoy {
    group_label: "Datetime"
    label: "Time of Year"
    sql:
    {% if date_granularity_yoy._parameter_value == 'day' %}
      ${date_of_year}
    {% elsif date_granularity_yoy._parameter_value == 'month' %}
      ${month}
    {% elsif date_granularity_yoy._parameter_value == 'year' %}
      ${all}
    {% else %}
      ${date_of_year}
    {% endif %};;
  }

  parameter: date_granularity_yoy {
    group_label: "Datetime filter"
    type: unquoted
    hidden: no
    allowed_value: {
      label: "Day"
      value: "day"
    }
    allowed_value: {
      label: "Week"
      value: "week"
    }
    allowed_value: {
      label: "Month"
      value: "month"
    }
    allowed_value: {
      label: "Year"
      value: "year"
    }
    default_value: "day"
  }

  dimension_group: time_stamp {
    group_label: "Datetime"
    type: time
    hidden: no
    datatype: date
    convert_tz: no ## already in UTC time
    timeframes: [
      date,
      week,
      week_of_year,
      month,
      quarter,
      year,
      month_name,
      day_of_week,
      day_of_month,
      day_of_year
    ]
    sql: ${ymd};;
  }

  parameter: date_granularity {
    group_label: "Datetime filter"
    type: unquoted
    hidden: no
    allowed_value: {
      label: "Day"
      value: "day"
    }
    allowed_value: {
      label: "Week"
      value: "week"
    }
    allowed_value: {
      label: "Month"
      value: "month"
    }
    allowed_value: {
      label: "Quarter"
      value: "quarter"
    }
    allowed_value: {
      label: "Year"
      value: "year"
    }
    default_value: "day"
  }

  dimension: date {
    group_label: "Datetime"
    sql:
    {% if date_granularity._parameter_value == 'month' %}
      ${time_stamp_month}
    {% elsif date_granularity._parameter_value == 'quarter' %}
      ${time_stamp_quarter}
    {% elsif date_granularity._parameter_value == 'week' %}
      ${time_stamp_week}
    {% elsif date_granularity._parameter_value == 'day' %}
      ${time_stamp_date}
    {% elsif date_granularity._parameter_value == 'year' %}
      ${time_stamp_year}
    {% else %}
      ${time_stamp_date}
    {% endif %};;
  }


### Other Dimensions ###

  dimension: period {
    type: string
    hidden: no
    label: "Period"
    sql: ${TABLE}.period ;;
  }

  dimension: range {
    type: number
    hidden: no
    label: "Range"
    sql: ${TABLE}.period_range ;;
  }

  dimension: stock_name {
    group_label: "Company"
    type: string
    case_sensitive: no
    suggestions: []
    sql: ${TABLE}.name ;;
  }

  dimension: company {
    group_label: "Company"
    type: string
    case_sensitive: no
    suggestions: []
    sql: ${TABLE}.company ;;
  }

  dimension: sector {
    group_label: "Company"
    type: string
    case_sensitive: no
    suggestions: ["Communication Services", "Consumer Discretionary", "Consumer Staples", "Energy", "Financials",
      "Health Care", "Industrials", "Information Technology", "Materials", "Real Estate", "Utilities"]
    sql: ${TABLE}.sector ;;
  }

### Dimension for Measure ###

  dimension: dim_open {
    sql: coalesce(${TABLE}.open,0) ;;
    hidden: yes
    type: number
    value_format: "$#,##0.00"
    label: "Open Price"
  }

  dimension: dim_close {
    sql: coalesce(${TABLE}.close,0) ;;
    hidden: yes
    type: number
    value_format: "$#,##0.00"
    label: "Close Price"
  }

  dimension: dim_high {
    sql: coalesce(${TABLE}.high,0) ;;
    hidden: yes
    type: number
    value_format: "$#,##0.00"
    label: "High Price"
  }

  dimension: dim_low {
    sql: coalesce(${TABLE}.low,0) ;;
    hidden: yes
    type: number
    value_format: "$#,##0.00"
    label: "Low Price"
  }

  dimension: dim_volume {
    sql: coalesce(${TABLE}.volume,0) ;;
    hidden: yes
    type: number
    value_format: "#,##0"
    label: "Volume"
  }


### Measure ###

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  set: detail {
    fields: [
      date, stock_name, avg_close
    ]
  }

### Measure (Aggregated) ###

  measure: distinct_stock {
    sql: ${stock_name} ;;
    type: count_distinct
    value_format: "#,##0"
    label: "Distinct Stock"
  }

  measure: sum_volume {
    sql: ${dim_volume} ;;
    type: sum
    group_label: "Metrics"
    value_format: "#,##0"
    label: "Sum Volume"
  }

  measure: avg_open {
    sql: ${dim_open} ;;
    type: average
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Simple Avg Open Price"
  }

  measure: avg_close {
    sql: ${dim_close} ;;
    type: average
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Simple Avg Close Price"
  }

  measure: avg_high {
    sql: ${dim_high} ;;
    type: average
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Simple Avg High Price"
  }

  measure: avg_low {
    sql: ${dim_low} ;;
    type: average
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Simple Avg Low Price"
  }

  measure: avg_volume {
    sql: ${dim_volume} ;;
    type: average
    group_label: "Metrics"
    value_format: "#,##0"
    label: "Simple Avg Volume"
  }

  measure: avg_price {
    group_label: "Metrics"
    label: "Simple Avg Price"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${avg_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${avg_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${avg_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${avg_low}
    {% else %}
      ${avg_close}
    {% endif %};;
  }

### Measure (Calculated) ###

  measure: dollar_volume_open {
    sql: (${dim_open}*${dim_volume}) ;;
    type: sum
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Dollar Volume with Open Price"
  }

  measure: dollar_volume_close {
    sql: (${dim_close}*${dim_volume}) ;;
    type: sum
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Dollar Volume with Close Price"
  }

  measure: dollar_volume_high {
    sql: (${dim_high}*${dim_volume}) ;;
    type: sum
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Dollar Volume with High Price"
  }

  measure: dollar_volume_low {
    sql: (${dim_low}*${dim_volume}) ;;
    type: sum
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Dollar Volume with Low Price"
  }

  measure: dollar_volume {
    group_label: "Metrics"
    label: "Dollar Volume"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${dollar_volume_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${dollar_volume_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${dollar_volume_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${dollar_volume_low}
    {% else %}
      ${dollar_volume_close}
    {% endif %};;
  }

  measure: vwap_open {
    sql: ${dollar_volume_open}/(${sum_volume}) ;;
    type: number
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Volume-Weighted Average Open Price (VWAP)"
  }

  measure: vwap_close {
    sql: ${dollar_volume_close}/(${sum_volume}) ;;
    type: number
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Volume-Weighted Average Close Price (VWAP)"
  }

  measure: vwap_high {
    sql: ${dollar_volume_high}/(${sum_volume}) ;;
    type: number
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Volume-Weighted Average High Price (VWAP)"
  }

  measure: vwap_low {
    sql: ${dollar_volume_low}/(${sum_volume}) ;;
    type: number
    group_label: "Metrics"
    value_format: "$#,##0.00"
    label: "Volume-Weighted Average Low Price (VWAP)"
  }

  measure: vwap_price {
    group_label: "Metrics"
    label: "Volume-Weighted Average Price (VWAP)"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${vwap_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${vwap_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${vwap_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${vwap_low}
    {% else %}
      ${vwap_close}
    {% endif %};;
  }


## Measure to Calculate Growth ##

  #### Last Period ####

  measure: last_sum_open {
    hidden: yes
    sql: ${dim_open} ;;
    type: sum
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Sum Open Price"
  }

  measure: last_sum_close {
    hidden: yes
    sql: ${dim_close} ;;
    type: sum
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Sum Close Price"
  }

  measure: last_sum_high {
    hidden: yes
    sql: ${dim_high} ;;
    type: sum
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Sum High Price"
  }

  measure: last_sum_low {
    hidden: yes
    sql: ${dim_low} ;;
    type: sum
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Sum Low Price"
  }

  measure: last_sum_volume {
    sql: ${dim_volume} ;;
    type: sum
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "#,##0"
    label: "Last Sum Volume"
  }

  measure: last_avg_open {
    sql: ${dim_open} ;;
    type: average
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Avg Open Price"
  }

  measure: last_avg_close {
    sql: ${dim_close} ;;
    type: average
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Avg Close Price"
  }

  measure: last_avg_high {
    sql: ${dim_high} ;;
    type: average
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Avg High Price"
  }

  measure: last_avg_low {
    sql: ${dim_low} ;;
    type: average
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Simple Avg Low Price"
  }

  measure: last_avg_volume {
    sql: ${dim_volume} ;;
    type: average
    filters: [period: "last_period"]
    group_label: "Last Period Metrics"
    value_format: "#,##0"
    label: "Last Simple Avg Volume"
  }

  measure: last_avg_price {
    group_label: "Last Period Metrics"
    label: "Last Simple Avg Price"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${last_avg_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${last_avg_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${last_avg_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${last_avg_low}
    {% else %}
      ${last_avg_close}
    {% endif %};;
  }

  measure: last_dollar_volume_open {
    sql: (${dim_open}*${dim_volume}) ;;
    type: sum
    group_label: "Last Period Metrics"
    filters: [period: "last_period"]
    value_format: "$#,##0.00"
    label: "Last Dollar Volume with Open Price"
  }

  measure: last_dollar_volume_close {
    sql: (${dim_close}*${dim_volume}) ;;
    type: sum
    group_label: "Last Period Metrics"
    filters: [period: "last_period"]
    value_format: "$#,##0.00"
    label: "Last Dollar Volume with Close Price"
  }

  measure: last_dollar_volume_high {
    sql: (${dim_high}*${dim_volume}) ;;
    type: sum
    group_label: "Last Period Metrics"
    filters: [period: "last_period"]
    value_format: "$#,##0.00"
    label: "Last Dollar Volume with High Price"
  }

  measure: last_dollar_volume_low {
    sql: (${dim_low}*${dim_volume}) ;;
    type: sum
    group_label: "Last Period Metrics"
    filters: [period: "last_period"]
    value_format: "$#,##0.00"
    label: "Last Dollar Volume with Low Price"
  }

  measure: last_dollar_volume {
    group_label: "Last Period Metrics"
    label: "Last Dollar Volume"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${last_dollar_volume_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${last_dollar_volume_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${last_dollar_volume_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${last_dollar_volume_low}
    {% else %}
      ${last_dollar_volume_close}
    {% endif %};;
  }

  measure: last_vwap_open {
    sql: (${last_dollar_volume_open})/(${last_sum_volume}) ;;
    type: number
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Volume-Weighted Average Open Price (VWAP)"
  }

  measure: last_vwap_close {
    sql: (${last_dollar_volume_close})/(${last_sum_volume}) ;;
    type: number
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Volume-Weighted Average Close Price (VWAP)"
  }

  measure: last_vwap_high {
    sql: (${last_dollar_volume_high})/(${last_sum_volume}) ;;
    type: number
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Volume-Weighted Average High Price (VWAP)"
  }

  measure: last_vwap_low {
    sql: (${last_dollar_volume_low})/(${last_sum_volume}) ;;
    type: number
    group_label: "Last Period Metrics"
    value_format: "$#,##0.00"
    label: "Last Volume-Weighted Average Low Price (VWAP)"
  }

  measure: last_vwap_price {
    group_label: "Last Period Metrics"
    label: "Last Volume-Weighted Average Price (VWAP)"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${last_vwap_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${last_vwap_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${last_vwap_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${last_vwap_low}
    {% else %}
      ${last_vwap_close}
    {% endif %};;
  }

#### Prev Period ####

  measure: prev_sum_open {
    hidden: yes
    sql: ${dim_open} ;;
    type: sum
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Sum Open Price"
  }

  measure: prev_sum_close {
    hidden: yes
    sql: ${dim_close} ;;
    type: sum
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Sum Close Price"
  }

  measure: prev_sum_high {
    hidden: yes
    sql: ${dim_high} ;;
    type: sum
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Sum High Price"
  }

  measure: prev_sum_low {
    hidden: yes
    sql: ${dim_low} ;;
    type: sum
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Sum Low Price"
  }

  measure: prev_sum_volume {
    sql: ${dim_volume} ;;
    type: sum
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "#,##0"
    label: "Prev Sum Volume"
  }

  measure: prev_avg_open {
    sql: ${dim_open} ;;
    type: average
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Avg Open Price"
  }

  measure: prev_avg_close {
    sql: ${dim_close} ;;
    type: average
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Avg Close Price"
  }

  measure: prev_avg_high {
    sql: ${dim_high} ;;
    type: average
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Avg High Price"
  }

  measure: prev_avg_low {
    sql: ${dim_low} ;;
    type: average
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Simple Avg Low Price"
  }

  measure: prev_avg_volume {
    sql: ${dim_volume} ;;
    type: average
    filters: [period: "prev_period"]
    group_label: "Prev Period Metrics"
    value_format: "#,##0"
    label: "Prev Simple Avg Volume"
  }

  measure: prev_avg_price {
    group_label: "Prev Period Metrics"
    label: "Prev Simple Avg Price"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${prev_avg_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${prev_avg_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${prev_avg_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${prev_avg_low}
    {% else %}
      ${prev_avg_close}
    {% endif %};;
  }

  measure: prev_dollar_volume_open {
    sql: (${dim_open}*${dim_volume}) ;;
    type: sum
    group_label: "Prev Period Metrics"
    filters: [period: "prev_period"]
    value_format: "$#,##0.00"
    label: "Prev Dollar Volume with Open Price"
  }

  measure: prev_dollar_volume_close {
    sql: (${dim_close}*${dim_volume}) ;;
    type: sum
    group_label: "Prev Period Metrics"
    filters: [period: "prev_period"]
    value_format: "$#,##0.00"
    label: "Prev Dollar Volume with Close Price"
  }

  measure: prev_dollar_volume_high {
    sql: (${dim_high}*${dim_volume}) ;;
    type: sum
    group_label: "Prev Period Metrics"
    filters: [period: "prev_period"]
    value_format: "$#,##0.00"
    label: "Prev Dollar Volume with High Price"
  }

  measure: prev_dollar_volume_low {
    sql: (${dim_low}*${dim_volume}) ;;
    type: sum
    group_label: "Prev Period Metrics"
    filters: [period: "prev_period"]
    value_format: "$#,##0.00"
    label: "Prev Dollar Volume with Low Price"
  }

  measure: prev_dollar_volume {
    group_label: "Prev Period Metrics"
    label: "Prev Dollar Volume"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${prev_dollar_volume_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${prev_dollar_volume_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${prev_dollar_volume_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${prev_dollar_volume_low}
    {% else %}
      ${prev_dollar_volume_close}
    {% endif %};;
  }

  measure: prev_vwap_open {
    sql: (${prev_dollar_volume_open})/(${prev_sum_volume}) ;;
    type: number
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Volume-Weighted Average Open Price (VWAP)"
  }

  measure: prev_vwap_close {
    sql: (${prev_dollar_volume_close})/(${prev_sum_volume}) ;;
    type: number
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Volume-Weighted Average Close Price (VWAP)"
  }

  measure: prev_vwap_high {
    sql: (${prev_dollar_volume_high})/(${prev_sum_volume}) ;;
    type: number
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Volume-Weighted Average High Price (VWAP)"
  }

  measure: prev_vwap_low {
    sql: (${prev_dollar_volume_low})/(${prev_sum_volume}) ;;
    type: number
    group_label: "Prev Period Metrics"
    value_format: "$#,##0.00"
    label: "Prev Volume-Weighted Average Low Price (VWAP)"
  }

  measure: prev_vwap_price {
    group_label: "Prev Period Metrics"
    label: "Prev Volume-Weighted Average Price (VWAP)"
    type: number
    value_format: "$#,##0.00"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${prev_vwap_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${prev_vwap_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${prev_vwap_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${prev_vwap_low}
    {% else %}
      ${prev_vwap_close}
    {% endif %};;
  }

#### PoP Growth ####

  measure: growth_sum_volume {
    sql: (1.00*(${last_sum_volume}-${prev_sum_volume})/${prev_sum_volume}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Sum Volume"
  }

  measure: growth_avg_open {
    sql: (1.00*(${last_avg_open}-${prev_avg_open})/${prev_avg_open}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Simple Avg Open Price"
  }

  measure: growth_avg_close {
    sql: (1.00*(${last_avg_close}-${prev_avg_close})/${prev_avg_close}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Simple Avg Close Price"
  }

  measure: growth_avg_high {
    sql: (1.00*(${last_avg_high}-${prev_avg_high})/${prev_avg_high}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Simple Avg High Price"
  }

  measure: growth_avg_low {
    sql: (1.00*(${last_avg_low}-${prev_avg_low})/${prev_avg_low}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Simple Avg Low Price"
  }

  measure: growth_avg_volume {
    sql: (1.00*(${last_avg_volume}-${prev_avg_volume})/${prev_avg_volume}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Simple Avg Volume"
  }

  measure: growth_avg_price {
    group_label: "Growth Metrics"
    label: "Growth Simple Avg Price"
    type: number
    value_format: "0.00%"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${growth_avg_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${growth_avg_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${growth_avg_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${growth_avg_low}
    {% else %}
      ${growth_avg_close}
    {% endif %};;
  }

  measure: growth_vwap_open {
    sql: (1.00*(${last_vwap_open}-${prev_vwap_open})/${prev_vwap_open}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Volume-Weighted Average Open Price (VWAP)"
  }

  measure: growth_vwap_close {
    sql: (1.00*(${last_vwap_close}-${prev_vwap_close})/${prev_vwap_close}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Volume-Weighted Average Close Price (VWAP)"
  }

  measure: growth_vwap_high {
    sql: (1.00*(${last_vwap_high}-${prev_vwap_high})/${prev_vwap_high}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Volume-Weighted Average High Price (VWAP)"
  }

  measure: growth_vwap_low {
    sql: (1.00*(${last_vwap_low}-${prev_vwap_low})/${prev_vwap_low}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Volume-Weighted Average Low Price (VWAP)"
  }

  measure: growth_vwap_price {
    group_label: "Growth Metrics"
    label: "Growth Volume-Weighted Average Price (VWAP)"
    type: number
    value_format: "0.00%"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${growth_vwap_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${growth_vwap_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${growth_vwap_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${growth_vwap_low}
    {% else %}
      ${growth_vwap_close}
    {% endif %};;
  }

  measure: growth_dollar_volume_open {
    sql: (1.00*(${last_dollar_volume_open}-${prev_dollar_volume_open})/${prev_dollar_volume_open}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Dollar Volume with Open Price"
  }

  measure: growth_dollar_volume_close {
    sql: (1.00*(${last_dollar_volume_close}-${prev_dollar_volume_close})/${prev_dollar_volume_close}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Dollar Volume with Close Price"
  }

  measure: growth_dollar_volume_high {
    sql: (1.00*(${last_dollar_volume_high}-${prev_dollar_volume_high})/${prev_dollar_volume_high}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Dollar Volume with High Price"
  }

  measure: growth_dollar_volume_low {
    sql: (1.00*(${last_dollar_volume_low}-${prev_dollar_volume_low})/${prev_dollar_volume_low}) ;;
    type: number
    group_label: "Growth Metrics"
    value_format: "0.00%"
    label: "Growth Dollar Volume with Low Price"
  }

  measure: growth_dollar_volume {
    group_label: "Growth Metrics"
    label: "Growth Dollar Volume"
    type: number
    value_format: "0.00%"
    sql:
    {% if price_type._parameter_value == 'open' %}
      ${growth_dollar_volume_open}
    {% elsif price_type._parameter_value == 'close' %}
      ${growth_dollar_volume_close}
    {% elsif price_type._parameter_value == 'high' %}
      ${growth_dollar_volume_high}
    {% elsif price_type._parameter_value == 'low' %}
      ${growth_dollar_volume_low}
    {% else %}
      ${growth_dollar_volume_close}
    {% endif %};;
  }

}
