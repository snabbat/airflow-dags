from abc import ABC, abstractmethod

from context_manager.JsonAccessorManager import JsonAccessorManager
from dag_manager.CompositeNode import CompositeNode
from exception_manager.ExceptionManager import ExceptionManager


class PhaseStrategy(ABC):

    def __init__(self, accessor_manager: JsonAccessorManager, exception_manager: ExceptionManager, name, recovery_mode="NONE"):
        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager
        self.name = name
        self.root = CompositeNode(name)
        self.recovery_mode = recovery_mode  # Mode de reprise

    @abstractmethod
    # ALTERED: added target parameter to build_tree() signature
    def build_tree(self, config, target=None, system_target=None, flows=None):
        pass

    @abstractmethod
    # ALTERED: added target parameter to execute() signature
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        pass

    # ALTERED: added target parameter to run_task() and all sub-methods
    def run_task(self, country: str, vertical: str, target: str, system_target: str, leaf_name):
        """Diriger vers la bonne sous-méthode selon le mode de reprise"""
        if self.recovery_mode == "FULL":
            return self.run_task_full(country=country, vertical=vertical, target=target, system_target=system_target, leaf_name=leaf_name)
        elif self.recovery_mode == "EXECUTION_PLAN_BASED":
            return self.run_task_execution_plan_based(country=country, vertical=vertical, target=target, system_target=system_target, leaf_name=leaf_name)
        else:
            return self.run_task_default(country=country, vertical=vertical, target=target, system_target=system_target, leaf_name=leaf_name)

    @abstractmethod
    def run_task_full(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode FULL : Réexécution complète"""
        pass

    @abstractmethod
    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode EXECUTION_PLAN_BASED : Suivi d'un plan d'exécution"""
        pass

    @abstractmethod
    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Exécution par défaut (NONE)"""
        pass