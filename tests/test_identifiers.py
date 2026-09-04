import pytest

from framework.errors import MetadataError
from framework.identifiers import qualified_name


def test_qualified_name_rejects_injection():
    with pytest.raises(MetadataError):
        qualified_name("demo_catalog", "silver", "x; DROP TABLE y")


def test_qualified_name_quotes_each_part():
    assert qualified_name("demo_catalog", "silver", "customers") == "`demo_catalog`.`silver`.`customers`"