"""Idempotently provision the smart-meter Superset database, dataset, and dashboard."""

import json

from superset import create_app

DATABASE_NAME = "Smart Meter Mooncake"
DATABASE_URI = "postgresql+psycopg2://mooncake:mooncake@mooncake:5432/warehouse"
DATASET_SCHEMA = "presentation"
DATASET_TABLE = "mart_meter_operations_periodic"
DASHBOARD_TITLE = "Smart Meter Operations"
QUALITY_DASHBOARD_TITLE = "Smart Meter Data Quality"
FINANCE_DATASET_SCHEMA = "presentation"
FINANCE_DATASET_TABLE = "mart_executive_billing_periodic"
FINANCE_DASHBOARD_TITLE = "Executive Billing & Revenue"
DEFAULT_TIME_RANGE = "No filter"
GRAIN_SORT_METRIC = "period_grain_order"

OPERATIONS_GLOSSARY = """### Metric glossary

| Term | Meaning |
| --- | --- |
| Energy (kWh) | Sum of recorded electrical energy in the selected period. |
| Meter reading | One accepted smart-meter event. |
| Reporting meter | Distinct meter with at least one reading in scope. |
| Average voltage | Reading-weighted mean voltage in volts. |
| Power factor | Real power divided by apparent power; values nearer 1 are more efficient. |
| Detail level | Calendar rollup: Year, Month, Week, Day, or Hour. |
"""

QUALITY_GLOSSARY = """### Metric glossary

| Term | Meaning |
| --- | --- |
| Average voltage | Reading-weighted mean voltage in volts. |
| Power factor | Real power divided by apparent power; values nearer 1 are more efficient. |
| Electrical health | Voltage and power-factor behavior over the selected period. |
| Detail level | Calendar rollup: Year, Month, Week, Day, or Hour. |
"""

BILLING_GLOSSARY = """### Metric glossary

| Term | Meaning |
| --- | --- |
| Billed energy | Priced energy consumption in kWh. |
| Net revenue | Sum of energy multiplied by the effective state price. |
| Gross billed | Net revenue plus the modeled 5% tax amount. |
| Active billed meter | Distinct meter with priced consumption in scope. |
| Realized price/kWh | Net revenue divided by billed energy; an energy-weighted rate. |
| Pricing zone | US state mapped to its Census region and currency. |
| Detail level | Calendar rollup: Year, Month, Week, Day, or Hour. |
"""


def metric(label: str, aggregate: str, column_name: str) -> dict:
    return {
        "aggregate": aggregate,
        "column": {"column_name": column_name},
        "expressionType": "SIMPLE",
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{aggregate.lower()}_{column_name}",
        "sqlExpression": None,
    }


def sql_metric(label: str, expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{label.lower().replace(' ', '_')}",
        "sqlExpression": expression,
    }


def billing_chart_params(dataset_id: int) -> list[tuple[str, str, dict]]:
    datasource = f"{dataset_id}__table"
    common = {
        "datasource": datasource,
        "adhoc_filters": [],
        "time_range": DEFAULT_TIME_RANGE,
    }
    gross = metric("Gross Billed (USD)", "SUM", "gross_billed")
    net = metric("Net Revenue (USD)", "SUM", "net_revenue")
    energy = metric("Billed Energy (kWh)", "SUM", "energy_kwh")
    return [
        (
            "Gross Billed",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": gross,
                "y_axis_format": "$,.2f",
            },
        ),
        (
            "Net Revenue",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": net,
                "y_axis_format": "$,.2f",
            },
        ),
        (
            "Billed Energy",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": energy,
                "y_axis_format": ",.2f",
            },
        ),
        (
            "Active Billed Meters",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": metric("Billed Meters", "COUNT_DISTINCT", "meter_id"),
                "y_axis_format": ",d",
            },
        ),
        (
            "Billing Trend",
            "echarts_timeseries_line",
            {
                **common,
                "viz_type": "echarts_timeseries_line",
                "x_axis": "period_start",
                "metrics": [gross, net],
                "groupby": ["us_region"],
                "row_limit": 10000,
                "show_legend": True,
                "rich_tooltip": True,
                "y_axis_format": "$,.2f",
            },
        ),
        (
            "Revenue by US Region",
            "echarts_timeseries_bar",
            {
                **common,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "us_region",
                "metrics": [net],
                "groupby": [],
                "show_legend": False,
                "rich_tooltip": True,
                "y_axis_format": "$,.2f",
            },
        ),
        (
            "Revenue by State",
            "echarts_timeseries_bar",
            {
                **common,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "state_code",
                "metrics": [net],
                "groupby": [],
                "show_legend": False,
                "rich_tooltip": True,
                "y_axis_format": "$,.2f",
            },
        ),
        (
            "Realized Price per kWh",
            "echarts_timeseries_line",
            {
                **common,
                "viz_type": "echarts_timeseries_line",
                "x_axis": "period_start",
                "metrics": [
                    sql_metric(
                        "Realized USD/kWh",
                        "SUM(net_revenue) / NULLIF(SUM(energy_kwh), 0)",
                    )
                ],
                "groupby": ["us_region"],
                "row_limit": 10000,
                "show_legend": False,
                "rich_tooltip": True,
                "y_axis_format": "$,.4f",
            },
        ),
    ]


def chart_params(dataset_id: int) -> list[tuple[str, str, dict]]:
    datasource = f"{dataset_id}__table"
    common = {
        "datasource": datasource,
        "adhoc_filters": [],
        "time_range": DEFAULT_TIME_RANGE,
    }
    return [
        (
            "Total Energy Consumed",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": metric("Energy (kWh)", "SUM", "energy_kwh"),
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",.2f",
            },
        ),
        (
            "Total Meter Readings",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": metric("Readings", "SUM", "reading_count"),
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",d",
            },
        ),
        (
            "Reporting Meters",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": metric("Meters", "COUNT_DISTINCT", "meter_id"),
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",d",
            },
        ),
        (
            "Energy Consumption Trend",
            "echarts_timeseries_line",
            {
                **common,
                "viz_type": "echarts_timeseries_line",
                "x_axis": "period_start",
                "metrics": [metric("Energy (kWh)", "SUM", "energy_kwh")],
                "groupby": [],
                "row_limit": 10000,
                "show_legend": True,
                "legendType": "scroll",
                "markerEnabled": True,
                "markerSize": 8,
                "rich_tooltip": True,
                "tooltipTimeFormat": "%Y-%m-%d",
                "x_axis_time_format": "smart_date",
                "y_axis_format": ",.2f",
                "truncate_metric": True,
            },
        ),
        (
            "Top Meters by Energy",
            "echarts_timeseries_bar",
            {
                **common,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "meter_id",
                "metrics": [metric("Energy (kWh)", "SUM", "energy_kwh")],
                "groupby": [],
                "row_limit": 10,
                "order_desc": True,
                "show_legend": False,
                "rich_tooltip": True,
                "y_axis_format": ",.2f",
            },
        ),
        (
            "Electrical Health Trend",
            "echarts_timeseries_line",
            {
                **common,
                "viz_type": "echarts_timeseries_line",
                "x_axis": "period_start",
                "metrics": [
                    sql_metric(
                        "Average Voltage (V)",
                        "SUM(voltage_sum) / NULLIF(SUM(reading_count), 0)",
                    ),
                    sql_metric(
                        "Average Power Factor",
                        "SUM(power_factor_sum) / NULLIF(SUM(reading_count), 0)",
                    ),
                ],
                "groupby": [],
                "row_limit": 10000,
                "show_legend": True,
                "legendType": "scroll",
                "markerEnabled": True,
                "markerSize": 8,
                "rich_tooltip": True,
                "tooltipTimeFormat": "%Y-%m-%d",
                "x_axis_time_format": "smart_date",
                "y_axis_format": ",.3f",
            },
        ),
        (
            "Average Voltage",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": sql_metric(
                    "Average Voltage (V)",
                    "SUM(voltage_sum) / NULLIF(SUM(reading_count), 0)",
                ),
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",.2f",
            },
        ),
        (
            "Average Power Factor",
            "big_number_total",
            {
                **common,
                "viz_type": "big_number_total",
                "metric": sql_metric(
                    "Average Power Factor",
                    "SUM(power_factor_sum) / NULLIF(SUM(reading_count), 0)",
                ),
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",.3f",
            },
        ),
    ]


def add_glossary(
    positions: dict,
    *,
    row_id: str,
    markdown_id: str,
    content: str,
) -> None:
    """Append a full-width Markdown glossary to a dashboard layout."""

    positions["GRID_ID"]["children"].append(row_id)
    positions[row_id] = {
        "id": row_id,
        "type": "ROW",
        "parents": ["ROOT_ID", "GRID_ID"],
        "children": [markdown_id],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }
    positions[markdown_id] = {
        "id": markdown_id,
        "type": "MARKDOWN",
        "parents": ["ROOT_ID", "GRID_ID", row_id],
        "children": [],
        "meta": {"code": content, "height": 28, "width": 12},
    }


def dashboard_layout(charts) -> str:
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASHBOARD_TITLE}},
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-KPIS", "ROW-TREND", "ROW-BREAKDOWN", "ROW-HEALTH"],
        },
    }
    rows = [
        ("ROW-KPIS", charts[:3], 4, 14),
        ("ROW-TREND", charts[3:4], 12, 34),
        ("ROW-BREAKDOWN", charts[4:5], 12, 30),
        ("ROW-HEALTH", charts[5:6], 12, 34),
    ]
    for row_id, row_charts, width, height in rows:
        child_ids = [f"CHART-{chart.id}" for chart in row_charts]
        positions[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": child_ids,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in row_charts:
            chart_id = f"CHART-{chart.id}"
            positions[chart_id] = {
                "id": chart_id,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "uuid": str(chart.uuid),
                    "height": height,
                    "width": width,
                    "sliceName": chart.slice_name,
                },
            }
    add_glossary(
        positions,
        row_id="ROW-OPERATIONS-GLOSSARY",
        markdown_id="MARKDOWN-OPERATIONS-GLOSSARY",
        content=OPERATIONS_GLOSSARY,
    )
    return json.dumps(positions)


def quality_dashboard_layout(charts) -> str:
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": QUALITY_DASHBOARD_TITLE}},
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-QUALITY-KPIS", "ROW-QUALITY-TREND"],
        },
    }
    rows = [
        ("ROW-QUALITY-KPIS", charts[:2], 6, 14),
        ("ROW-QUALITY-TREND", charts[2:3], 12, 34),
    ]
    for row_id, row_charts, width, height in rows:
        child_ids = [f"CHART-{chart.id}" for chart in row_charts]
        positions[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": child_ids,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in row_charts:
            chart_id = f"CHART-{chart.id}"
            positions[chart_id] = {
                "id": chart_id,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "uuid": str(chart.uuid),
                    "height": height,
                    "width": width,
                    "sliceName": chart.slice_name,
                },
            }
    add_glossary(
        positions,
        row_id="ROW-QUALITY-GLOSSARY",
        markdown_id="MARKDOWN-QUALITY-GLOSSARY",
        content=QUALITY_GLOSSARY,
    )
    return json.dumps(positions)


def billing_dashboard_layout(charts) -> str:
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": FINANCE_DASHBOARD_TITLE}},
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": [
                "ROW-BILLING-KPIS",
                "ROW-BILLING-TREND",
                "ROW-BILLING-MIX",
                "ROW-BILLING-PRICE",
            ],
        },
    }
    rows = [
        ("ROW-BILLING-KPIS", charts[:4], 3, 14),
        ("ROW-BILLING-TREND", charts[4:5], 12, 34),
        ("ROW-BILLING-MIX", charts[5:7], 6, 30),
        ("ROW-BILLING-PRICE", charts[7:8], 12, 30),
    ]
    for row_id, row_charts, width, height in rows:
        child_ids = [f"CHART-{chart.id}" for chart in row_charts]
        positions[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": child_ids,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in row_charts:
            chart_id = f"CHART-{chart.id}"
            positions[chart_id] = {
                "id": chart_id,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "uuid": str(chart.uuid),
                    "height": height,
                    "width": width,
                    "sliceName": chart.slice_name,
                },
            }
    add_glossary(
        positions,
        row_id="ROW-BILLING-GLOSSARY",
        markdown_id="MARKDOWN-BILLING-GLOSSARY",
        content=BILLING_GLOSSARY,
    )
    return json.dumps(positions)


def billing_dashboard_metadata(dataset_id: int, charts) -> str:
    """Default wide executive views to monthly aggregates with explicit drill-down."""

    grain_filter_id = "NATIVE_FILTER-billing-detail-level-v2"
    return json.dumps(
        {
            "color_scheme": "supersetColors",
            "expanded_slices": {},
            "native_filter_configuration": [
                {
                    "id": grain_filter_id,
                    "controlValues": {
                        "enableEmptyFilter": False,
                        "defaultToFirstItem": True,
                        "multiSelect": False,
                        "searchAllOptions": False,
                        "inverseSelection": False,
                        "sortAscending": True,
                    },
                    "sortMetric": GRAIN_SORT_METRIC,
                    "name": "Detail level",
                    "filterType": "filter_select",
                    "targets": [
                        {
                            "column": {"name": "period_grain"},
                            "datasetId": dataset_id,
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {
                            "filters": [
                                {
                                    "col": "period_grain",
                                    "op": "IN",
                                    "val": ["Year"],
                                }
                            ]
                        },
                        "filterState": {
                            "label": "Year",
                            "validateMessage": False,
                            "validateStatus": False,
                            "value": ["Year"],
                        },
                    },
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "type": "NATIVE_FILTER",
                    "description": (
                        "Use Year/Month for wide views and Week/Day/Hour to drill down."
                    ),
                    "chartsInScope": [chart.id for chart in charts],
                    "tabsInScope": [],
                },
                {
                    "id": "NATIVE_FILTER-billing-time-range-v2",
                    "controlValues": {},
                    "name": "Time range",
                    "filterType": "filter_time",
                    "targets": [
                        {
                            "column": {"name": "period_start"},
                            "datasetId": dataset_id,
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {"time_range": DEFAULT_TIME_RANGE},
                        "filterState": {"value": DEFAULT_TIME_RANGE},
                    },
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "type": "NATIVE_FILTER",
                    "chartsInScope": [chart.id for chart in charts],
                    "tabsInScope": [],
                },
            ],
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
        }
    )


def operations_dashboard_metadata(dataset_id: int, charts) -> str:
    """Default wide operational views to monthly rollups with drill controls."""

    chart_ids = [chart.id for chart in charts]
    return json.dumps(
        {
            "color_scheme": "supersetColors",
            "expanded_slices": {},
            "native_filter_configuration": [
                {
                    "id": "NATIVE_FILTER-operations-detail-level-v2",
                    "controlValues": {
                        "enableEmptyFilter": False,
                        "defaultToFirstItem": True,
                        "multiSelect": False,
                        "searchAllOptions": False,
                        "inverseSelection": False,
                        "sortAscending": True,
                    },
                    "sortMetric": GRAIN_SORT_METRIC,
                    "name": "Detail level",
                    "filterType": "filter_select",
                    "targets": [
                        {
                            "column": {"name": "period_grain"},
                            "datasetId": dataset_id,
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {
                            "filters": [
                                {"col": "period_grain", "op": "IN", "val": ["Year"]}
                            ]
                        },
                        "filterState": {
                            "label": "Year",
                            "validateMessage": False,
                            "validateStatus": False,
                            "value": ["Year"],
                        },
                    },
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "type": "NATIVE_FILTER",
                    "description": "Choose Year/Month/Week/Day/Hour as you drill.",
                    "chartsInScope": chart_ids,
                    "tabsInScope": [],
                },
                {
                    "id": "NATIVE_FILTER-operations-time-range-v2",
                    "controlValues": {},
                    "name": "Time range",
                    "filterType": "filter_time",
                    "targets": [
                        {
                            "column": {"name": "period_start"},
                            "datasetId": dataset_id,
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {"time_range": DEFAULT_TIME_RANGE},
                        "filterState": {"value": DEFAULT_TIME_RANGE},
                    },
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "type": "NATIVE_FILTER",
                    "chartsInScope": chart_ids,
                    "tabsInScope": [],
                },
            ],
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
        }
    )


def provision() -> None:
    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable, SqlMetric
        from superset.extensions import db
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin = app.appbuilder.sm.find_user(username="admin")

        def ensure_grain_sort_metric(target_dataset: SqlaTable) -> None:
            sort_metric = (
                db.session.query(SqlMetric)
                .filter_by(
                    table_id=target_dataset.id,
                    metric_name=GRAIN_SORT_METRIC,
                )
                .one_or_none()
            )
            if sort_metric is None:
                sort_metric = SqlMetric(
                    table_id=target_dataset.id,
                    metric_name=GRAIN_SORT_METRIC,
                    created_by=admin,
                )
                db.session.add(sort_metric)
            sort_metric.verbose_name = "Detail level order"
            sort_metric.expression = "MIN(period_grain_order)"
            sort_metric.description = (
                "Orders dashboard grains from year through hour."
            )

        database = (
            db.session.query(Database)
            .filter_by(database_name=DATABASE_NAME)
            .one_or_none()
        )
        if database is None:
            database = Database(
                database_name=DATABASE_NAME, sqlalchemy_uri=DATABASE_URI
            )
            db.session.add(database)
        else:
            database.sqlalchemy_uri = DATABASE_URI
        database.expose_in_sqllab = True
        database.allow_ctas = False
        database.allow_cvas = False
        database.allow_dml = False
        database.created_by = database.created_by or admin
        db.session.commit()

        dataset = (
            db.session.query(SqlaTable)
            .filter_by(
                database_id=database.id, schema=DATASET_SCHEMA, table_name=DATASET_TABLE
            )
            .one_or_none()
        )
        if dataset is None:
            dataset = SqlaTable(
                database=database,
                schema=DATASET_SCHEMA,
                table_name=DATASET_TABLE,
                created_by=admin,
            )
            db.session.add(dataset)
            db.session.commit()
        dataset.fetch_metadata()
        dataset.main_dttm_col = "period_start"
        ensure_grain_sort_metric(dataset)
        db.session.commit()

        charts = []
        for name, viz_type, params in chart_params(dataset.id):
            chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
            if chart is None:
                chart = Slice(slice_name=name, created_by=admin)
                db.session.add(chart)
            chart.slice_name = name
            chart.datasource_id = dataset.id
            chart.datasource_type = "table"
            chart.datasource_name = dataset.table_name
            chart.viz_type = viz_type
            chart.params = json.dumps(params)
            chart.description = f"Smart-meter portfolio KPI sourced from {DATASET_SCHEMA}.{DATASET_TABLE}."
            charts.append(chart)
        db.session.commit()

        dashboard = (
            db.session.query(Dashboard)
            .filter_by(slug="smart-meter-operations")
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(slug="smart-meter-operations", created_by=admin)
            db.session.add(dashboard)
        dashboard.dashboard_title = DASHBOARD_TITLE
        dashboard.description = (
            "Operational consumption, fleet health, and electrical KPIs."
        )
        dashboard.published = True
        dashboard.slices = charts[:6]
        dashboard.position_json = dashboard_layout(charts[:6])
        dashboard.json_metadata = operations_dashboard_metadata(dataset.id, charts[:6])
        db.session.commit()

        quality_charts = [charts[6], charts[7], charts[5]]
        quality_dashboard = (
            db.session.query(Dashboard)
            .filter_by(slug="smart-meter-data-quality")
            .one_or_none()
        )
        if quality_dashboard is None:
            quality_dashboard = Dashboard(
                slug="smart-meter-data-quality", created_by=admin
            )
            db.session.add(quality_dashboard)
        quality_dashboard.dashboard_title = QUALITY_DASHBOARD_TITLE
        quality_dashboard.description = (
            "Voltage, power factor, and electrical health monitoring."
        )
        quality_dashboard.published = True
        quality_dashboard.slices = quality_charts
        quality_dashboard.position_json = quality_dashboard_layout(quality_charts)
        quality_dashboard.json_metadata = operations_dashboard_metadata(
            dataset.id, quality_charts
        )
        db.session.commit()

        finance_dataset = (
            db.session.query(SqlaTable)
            .filter_by(
                database_id=database.id,
                schema=FINANCE_DATASET_SCHEMA,
                table_name=FINANCE_DATASET_TABLE,
            )
            .one_or_none()
        )
        if finance_dataset is None:
            finance_dataset = SqlaTable(
                database=database,
                schema=FINANCE_DATASET_SCHEMA,
                table_name=FINANCE_DATASET_TABLE,
                created_by=admin,
            )
            db.session.add(finance_dataset)
            db.session.commit()
        finance_dataset.fetch_metadata()
        finance_dataset.main_dttm_col = "period_start"
        ensure_grain_sort_metric(finance_dataset)
        db.session.commit()

        finance_charts = []
        for name, viz_type, params in billing_chart_params(finance_dataset.id):
            chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
            if chart is None:
                chart = Slice(slice_name=name, created_by=admin)
                db.session.add(chart)
            chart.slice_name = name
            chart.datasource_id = finance_dataset.id
            chart.datasource_type = "table"
            chart.datasource_name = finance_dataset.table_name
            chart.viz_type = viz_type
            chart.params = json.dumps(params)
            chart.description = (
                "Executive billing KPI sourced from the governed finance mart."
            )
            finance_charts.append(chart)
        db.session.commit()

        finance_dashboard = (
            db.session.query(Dashboard)
            .filter_by(slug="executive-billing-revenue")
            .one_or_none()
        )
        if finance_dashboard is None:
            finance_dashboard = Dashboard(
                slug="executive-billing-revenue", created_by=admin
            )
            db.session.add(finance_dashboard)
        finance_dashboard.dashboard_title = FINANCE_DASHBOARD_TITLE
        finance_dashboard.description = "Executive view of billed energy, revenue composition, customer reach, and realization."
        finance_dashboard.published = True
        finance_dashboard.slices = finance_charts
        finance_dashboard.position_json = billing_dashboard_layout(finance_charts)
        finance_dashboard.json_metadata = billing_dashboard_metadata(
            finance_dataset.id, finance_charts
        )
        db.session.commit()

        print(
            json.dumps(
                {
                    "dashboard": dashboard.dashboard_title,
                    "dashboard_id": dashboard.id,
                    "dataset_id": dataset.id,
                    "operations_charts": len(dashboard.slices),
                    "quality_dashboard_id": quality_dashboard.id,
                    "quality_charts": len(quality_dashboard.slices),
                    "finance_dashboard_id": finance_dashboard.id,
                    "finance_charts": len(finance_dashboard.slices),
                }
            )
        )


if __name__ == "__main__":
    provision()
