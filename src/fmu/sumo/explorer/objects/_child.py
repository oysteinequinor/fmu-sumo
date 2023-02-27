from fmu.sumo.explorer.objects._document import Document
from sumo.wrapper import SumoClient
from io import BytesIO
from typing import Dict


class Child(Document):
    """Class representing a child object in Sumo"""

    def __init__(self, sumo: SumoClient, metadata: Dict) -> None:
        """
        Args:
            sumo (SumoClient): connection to Sumo
            metadata: (dict): child object metadata
        """
        super().__init__(metadata)
        self._sumo = sumo
        self._blob = None

    @property
    def name(self) -> str:
        """Object name"""
        return self._get_property(["data", "name"])

    @property
    def tagname(self) -> str:
        """Object tagname"""
        return self._get_property(["data", "tagname"])

    @property
    def context(self) -> str:
        """Object context"""
        return self._get_property(["fmu", "context", "stage"])

    @property
    def iteration(self) -> int:
        """Object iteration"""
        return self._get_property(["fmu", "iteration", "name"])

    @property
    def realization(self) -> int:
        """Object realization"""
        return self._get_property(["fmu", "realization", "id"])

    @property
    def operation(self) -> str:
        """Object operation"""
        return self._get_property(["fmu", "aggregation", "operation"])

    @property
    def stage(self) -> str:
        """Object stage"""
        return self._get_property(["fmu", "context", "stage"])

    @property
    def format(self) -> str:
        """Object file format"""
        return self._get_property(["data", "format"])

    @property
    def blob(self) -> BytesIO:
        """Object blob"""
        if self._blob is None:
            res = self._sumo.get(f"/objects('{self.uuid}')/blob")
            self._blob = BytesIO(res)

        return self._blob