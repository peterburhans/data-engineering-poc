"""Canonical constants for the smart-meter event contracts."""

UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
SUPPORTED_SCHEMA_VERSIONS = ("1.0",)
US_CENSUS_REGIONS = ("Northeast", "Midwest", "South", "West")

US_STATE_REGIONS = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast",
    "NH": "Northeast", "RI": "Northeast", "VT": "Northeast",
    "NJ": "Northeast", "NY": "Northeast", "PA": "Northeast",
    "IN": "Midwest", "IL": "Midwest", "MI": "Midwest",
    "OH": "Midwest", "WI": "Midwest", "IA": "Midwest",
    "KS": "Midwest", "MN": "Midwest", "MO": "Midwest",
    "NE": "Midwest", "ND": "Midwest", "SD": "Midwest",
    "DE": "South", "DC": "South", "FL": "South", "GA": "South",
    "MD": "South", "NC": "South", "SC": "South", "VA": "South",
    "WV": "South", "AL": "South", "KY": "South", "MS": "South",
    "TN": "South", "AR": "South", "LA": "South", "OK": "South",
    "TX": "South", "AZ": "West", "CO": "West", "ID": "West",
    "MT": "West", "NV": "West", "NM": "West", "UT": "West",
    "WY": "West", "AK": "West", "CA": "West", "HI": "West",
    "OR": "West", "WA": "West",
}
US_STATE_CODES = tuple(US_STATE_REGIONS)
US_STATE_REGION_KEYS = tuple(
    f"{state_code}:{region}" for state_code, region in US_STATE_REGIONS.items()
)


def region_for_state(state_code: str) -> str:
    """Return the US Census region for a supported state or district code."""
    return US_STATE_REGIONS[state_code]
