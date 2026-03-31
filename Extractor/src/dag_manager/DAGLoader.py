from context_manager.JsonAccessorManager import JsonAccessorManager
from dag_manager.PhaseFactory import PhaseFactory
from exception_manager.ExceptionManager import ExceptionManager


class DAGLoader:

    def __init__(self, accessor_manager: JsonAccessorManager, country: str, vertical: str, exception_manager: ExceptionManager):


        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager

        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager
        # ALTERED: renamed dag to extractor_dag
        self.key = country + "-" + vertical + "-extractor_dag"
        # print(self.key)
        self.config = accessor_manager.get(self.key).get_metadata()


    # ALTERED: added target parameter, renamed ingestion references to extraction
    def execute(self, country, phase=None, vertical=None, target=None, system_target=None, flows=None, recovery_mode="NONE"):
        self.country = country
        self.vertical = vertical
        # ALTERED: added self.target
        self.target = target
        self.system_target = system_target

        try:
            if self.config.get("country") != country:
                raise ValueError(f"Le pays '{country}' ne correspond pas a la configuration du DAG")

            verticals = self.config.get("verticals", {})
            if vertical and vertical not in verticals:
                raise ValueError(f"La verticale '{vertical}' est introuvable dans la configuration du DAG")

            target_verticals = [vertical] if vertical else verticals.keys()

            for vert in target_verticals:
                vertical_data = verticals[vert]
                phases = vertical_data.get("phases", {})

                if phase:
                    if phase in phases and not phases[phase].get("disable", False):
                        phase_instance = PhaseFactory.get_phase(self.accessor_manager, self.exception_manager, phase, recovery_mode)
                        # ALTERED: added target parameter to build_tree() and execute()
                        phase_instance.build_tree(phases[phase], target, system_target, flows)
                        if not phase_instance.execute(self.country, self.vertical, self.target, self.system_target):
                            return False
                else:
                    for phase_name, phase_data in phases.items():
                        if not phase_data.get("disable", False):
                            phase_instance = PhaseFactory.get_phase(self.accessor_manager, self.exception_manager, phase_name, recovery_mode)
                            # ALTERED: added target parameter to build_tree() and execute()
                            phase_instance.build_tree(phase_data, target, system_target, flows)
                            if not phase_instance.execute(self.country, self.vertical, self.target, self.system_target):
                                return False

            return True

        except Exception as e:
            self.exception_manager.handle(e, {"classe": __name__, "country": country, "vertical": vertical, "phase": phase})
            return False