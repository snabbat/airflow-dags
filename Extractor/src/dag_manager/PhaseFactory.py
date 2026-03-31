from context_manager.JsonAccessorManager import JsonAccessorManager
from dag_manager.CoreStrategy import CorePhase
# ALTERED: IngestionPhase renamed to ExtractionPhase
from dag_manager.ExtractionPhase import ExtractionPhase
from dag_manager.MartStrategy import MartsPhase
from dag_manager.PhaseStrategy import PhaseStrategy
from exception_manager.ExceptionManager import ExceptionManager


class PhaseFactory:
    @staticmethod
    def get_phase(accessor_manager: JsonAccessorManager, exception_manager: ExceptionManager, name, recovery_mode="NONE"):
        # ALTERED: "ingestion" renamed to "extraction", IngestionPhase → ExtractionPhase
        if name == "extraction":
            return ExtractionPhase(accessor_manager, exception_manager, name, recovery_mode)
        elif name == "core":
            # ALTERED: added accessor_manager and exception_manager (was missing — bug fix)
            return CorePhase(accessor_manager, exception_manager, name, recovery_mode)
        elif name == "marts":
            # ALTERED: added accessor_manager and exception_manager (was missing — bug fix)
            return MartsPhase(accessor_manager, exception_manager, name, recovery_mode)
        else:
            raise ValueError(f"Unknown phase: {name}")