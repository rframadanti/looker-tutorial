connection: "impala3"

include: "/**/view.lkml"    # include all views in this project

persist_for: "24 hours"

## Explore for demo using open data ##

explore: demo_yoy_stocks_data {
  label: "[Test] Demo of YoY Development with Open Stocks Data"
  always_filter: {
    filters: [demo_yoy_stocks_data.date_filter: "2017-01-01 to 2017-07-01"]
  }
}

explore: demo_pop_stocks_data {
  label: "[Test] Demo of PoP Development with Open Stocks Data"
  always_filter: {
    filters: [demo_pop_stocks_data.date_filter: "2017-01-01 to 2017-07-01"]
  }
}

explore: demo_top_stocks_data {
  label: "[Test] Demo of Dynamic Ranking with Open Stocks Data"
  always_filter: {
    filters: [demo_top_stocks_data.date_filter: "2017-01-01 to 2017-07-01"]
  }
}
