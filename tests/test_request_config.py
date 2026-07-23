from src.request_config import (
    EXPECTED_MEASUREMENTS_PER_RESPONSE,
    FORECAST_DAYS,
    PAST_DAYS,
)
# Test the requested configuration
def test_request_config():
    # Act and assert: use import to calculate and aassert
    expected = (
        FORECAST_DAYS + PAST_DAYS
    ) * 24

    # Assert expected
    assert expected == EXPECTED_MEASUREMENTS_PER_RESPONSE