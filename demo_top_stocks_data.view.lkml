view: test_demo_top_stocks_data {
  label: "[Test] Demo: Dynamic Top Stocks Data"

  derived_table: {
    sql:
    with top_stock as (
    SELECT
        "top_stock" AS top_stock, -- column to flag top stocks
        c.name,
        RANK () OVER (ORDER BY
            {% if rank_parameter._parameter_value == "dollar_volume" %}
              SUM(COALESCE(c.close,0)*COALESCE(c.volume,0))
            {% elsif rank_parameter._parameter_value == "avg_closing_price" %}
              AVG(COALESCE(c.close,0))
            {% elsif rank_parameter._parameter_value == "stock_volume" %}
              SUM(COALESCE(c.volume,0))
            {% else %}
              SUM(COALESCE(c.close,0)*COALESCE(c.volume,0))
            {% endif %}
          DESC ) rank_no, -- column to assign rank from 1 to N
        SUM(COALESCE(c.close,0)*COALESCE(c.volume,0)) as sum_dollar_volume,
        AVG(COALESCE(c.close,0)) as avg_closing_price,
        SUM(COALESCE(c.volume,0)) as sum_stock_volume
      FROM dramadanti.demo_all_stocks c
      LEFT JOIN dramadanti.stock_lookup d ON c.name = d.stock_symbol
      WHERE {% condition date_filter %} c.ymd {% endcondition %}
      AND {% condition sector_filter %} d.sector {% endcondition %}
      GROUP BY
        1, 2
      ORDER BY
        {% if rank_parameter._parameter_value == "dollar_volume" %}
            sum_dollar_volume DESC
        {% elsif rank_parameter._parameter_value == "avg_closing_price" %}
            avg_closing_price DESC
        {% elsif rank_parameter._parameter_value == "stock_volume" %}
            sum_stock_volume DESC
        {% else %}
            sum_dollar_volume DESC
        {% endif %}
      LIMIT {% parameter top_N %} -- parameter to change Top N
    )
    SELECT
    a.*, b.stock_name as company, b.sector as sector,
    top_stock.top_stock, top_stock.rank_no
    FROM dramadanti.demo_all_stocks a
    LEFT JOIN dramadanti.stock_lookup b ON a.name = b.stock_symbol
    LEFT JOIN top_stock ON a.name = top_stock.name
    WHERE {% condition date_filter %} a.ymd {% endcondition %}
    AND {% condition sector_filter %} b.sector {% endcondition %}
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

  parameter: top_N {
    type: number
    label: "Top N"
    allowed_value: {
      label: "Top 5"
      value: "5"
    }
    allowed_value: {
      label: "Top 10"
      value: "10"
    }
    allowed_value: {
      label: "Top 20"
      value: "20"
    }
    default_value: "10"
  }

  parameter: rank_parameter {
    type: unquoted
    allowed_value: {
      label: "Sum of Dollar Volume (closing price)"
      value: "dollar_volume"
    }
    allowed_value: {
      label: "Simple Avg Closing Price"
      value: "avg_closing_price"
    }
    allowed_value: {
      label: "Sum of Stock Volume"
      value: "stock_volume"
    }
    default_value: "dollar_volume"
  }

  measure: rank_measure {
    label: "Rank Metric based on Rank Parameter Filter"
    type:  number
    value_format: "#,##0"
    sql:
    {% if rank_parameter._parameter_value == 'dollar_volume' %}
      ${dollar_volume_close}
    {% elsif rank_parameter._parameter_value == 'avg_closing_price' %}
      ${avg_close}
    {% elsif rank_parameter._parameter_value == 'stock_volume' %}
      ${sum_volume}
    {% else %}
      ${dollar_volume_close}
    {% endif %};;
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
    group_label: "Datetime YoY"
    type:  number
    hidden: yes
    sql:  mod(datediff(${now},${time_stamp_date}),365);;
  }

  dimension: year {
    group_label: "Datetime YoY"
    type: number
    label: "Year"
    sql: substr(${TABLE}.ymd,1,4) ;;
  }

  dimension: month {
    group_label: "Datetime YoY"
    type: string
    label: "Month YoY"
    sql: substr(${TABLE}.ymd,6,2);;
  }

  dimension: week {
    group_label: "Datetime YoY"
    type: number
    label: "Week YoY"
    hidden: yes
    sql: floor(${now_diff}/7)*(-1);;
  }

  dimension: month_name {
    group_label: "Datetime YoY"
    type: date_month_name
    label: "Month Name"
    sql: ${date};;
  }

## TODO: change this into the right format
  dimension: date_of_year {
    group_label: "Datetime YoY"
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
    group_label: "Datetime YoY"
    label: "Time of Year"
    sql:
    {% if date_granularity_yoy._parameter_value == 'day' %}
      ${date_of_year}
    {% elsif date_granularity_yoy._parameter_value == 'week' %}
      ${time_stamp_week_of_year}
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


### Dimension for top stock ###

  dimension: rank {
    group_label: "Company"
    type: number
    sql: ${TABLE}.rank_no;;
  }

  dimension: is_top_stock {
    group_label: "Company"
    type: number
    case_sensitive: no
    sql:
      CASE
      WHEN ${TABLE}.top_stock = "top_stock" THEN 1
      ELSE 0
      END ;;
  }

  dimension: top_stock {
    group_label: "Company"
    type: string
    case_sensitive: no
    sql:
      CASE
      WHEN ${is_top_stock} = 1 THEN ${stock_name}
      ELSE " Others"
      END ;;
  }

  dimension: top_company {
    group_label: "Company"
    type: string
    case_sensitive: no
    sql:
      CASE
      WHEN ${is_top_stock} = 1 THEN ${company}
      ELSE " Others"
      END ;;
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

}
