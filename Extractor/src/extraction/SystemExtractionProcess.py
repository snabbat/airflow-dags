import os
import sys
from context_manager.JsonAccessorManager import JsonAccessorManager
from exception_manager.ExceptionManager import ExceptionManager
from logger.LogHandler import LogHandler


# ALTERED: renamed from SystemIngestionProcess, moved from ingestion/ to extraction/
class SystemExtractionProcess:
    def __init__(self, accessor_manager: JsonAccessorManager, country: str, vertical: str, system_target: str, exception_manager: ExceptionManager):
        """
        Initialise le processus d'exécution des flow d'extraction.
        :param config: Configuration du système
        :param country: Pays concerné
        :param vertical: Verticale métier
        :param system_target: Système source des données
        """

        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager
        self.country = country
        self.vertical = vertical
        self.system_target = system_target
        # ALTERED: renamed dag to extractor_dag
        self.key = country + "-" + vertical + "-" + "extractor_dag"

    def run(self):
        """
        Exécute les flow d'extraction définis dans la configuration DAG en prenant en compte la notion de "disable".
        """

        try:
            dag_config = self.accessor_manager.get(self.key).get_metadata()
            phases = dag_config.get("verticals", {}).get(self.vertical, {}).get("phases", {})
            # ALTERED: navigates new hierarchy — extraction → target → system → flow
            # TODO: target is hardcoded to "bronze" — should be passed as parameter
            extraction = phases.get("extraction", {}).get("target", {}).get("bronze", {}).get("system", {}).get(self.system_target, {})

            if "flow" not in extraction:
                print(f"Aucun flow défini pour {self.system_target} dans l'extraction.")
                return

            for flow in extraction["flow"]:
                flow_name = flow["name"]
                mandatory = flow["mandatory"]
                disable = flow.get("disable", False)

                if disable:
                    print(f"Flow {flow_name} est désactivé. Il ne sera pas exécuté.")
                    continue

                print(f"Execution du flow: {flow_name}")
                accessor_manager = JsonAccessorManager()
                project_root = os.path.normpath(os.path.join(sys.path[0], "..", ".."))
                accessor_manager.add_from_directory(os.path.join(project_root, "metadata", "extractor"))
                accessor_manager.add_from_directory(os.path.join(project_root, "config"))

                # TODO: FlowExtractionProcess is not yet wired here
                # process = FlowExtractionProcess(accessor_manager, self.country, self.vertical, self.system_target, flow_name, self.exception_manager)
                # success = process.run()
                # ALTERED: placeholder value until FlowExtractionProcess is wired
                success = False

                if not success and mandatory:
                    print(
                        f"Erreur critique: le flow obligatoire {flow_name} a échoué. Arrêt du traitement avec code d'erreur 1.")
                    return False
                elif not success:
                    print(f"Flow {flow_name} a échoué mais est optionnel. Poursuite du traitement.")
                    continue

        except Exception as e:
            self.exception_manager.handle(e, {"classe": __name__, "system": self.system_target})
            return False
