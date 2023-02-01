from sumo.wrapper import SumoClient
from fmu.sumo.explorer.objects.child_collection import ChildCollection
from fmu.sumo.explorer.objects.polygons import Polygons
from typing import Union, List, Dict


class PolygonsCollection(ChildCollection):
    """Class for representing a collection of polygons objects in Sumo"""

    def __init__(self, sumo: SumoClient, case_id: str, filter: List[Dict] = None):
        super().__init__("polygons", sumo, case_id, filter)

    def __getitem__(self, index) -> Polygons:
        doc = super().__getitem__(index)
        return Polygons(self._sumo, doc)

    def filter(
        self,
        name: Union[str, List[str]] = None,
        tagname: Union[str, List[str]] = None,
        iteration: Union[int, List[int]] = None,
        realization: Union[int, List[int]] = None,
    ) -> "PolygonsCollection":
        filter = super()._add_filter(name, tagname, iteration, realization)
        return PolygonsCollection(self._sumo, self._case_id, filter)
