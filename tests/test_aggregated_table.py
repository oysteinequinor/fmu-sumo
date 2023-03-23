"""Testing of Aggregated table class"""
import pandas as pd
import pyarrow as pa
from fmu.sumo.explorer import Explorer, AggregatedTable
import pytest


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns explorer"""
    return Explorer("dev", token=token)


# @pytest.fixture(name="case")
# def case_fixture():
#     """Init of case"""
#     exp = Explorer("dev")
#     case = exp.cases.filter(name="drogon_ahm-2023-02-22")[0]
#     return case


def test_aggregated_summary_arrow(explorer: Explorer):
    """Test usage of Aggregated class with default type"""

    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]

    table = AggregatedTable(case, "summary", "eclipse", "iter-0")

    assert len(table.columns) == 972 + 2
    column = table["FOPT"]

    assert isinstance(column.arrowtable, pa.Table)
    with pytest.raises(IndexError) as e_info:
        table = table["banana"]
        assert (
            e_info.value.args[0] == "Column: 'banana' does not exist try again"
        )


def test_aggregated_summary_pandas(explorer: Explorer):
    """Test usage of Aggregated class with item_type=pandas"""
    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table["FOPT"].dataframe, pd.DataFrame)


def test_get_fmu_iteration_parameters(explorer: Explorer):
    """Test getting the metadata of of an object"""
    case = explorer.cases.filter(name="drogon_ahm-2023-02-22")[0]
    table = AggregatedTable(case, "summary", "eclipse", "iter-0")
    assert isinstance(table.parameters, dict)