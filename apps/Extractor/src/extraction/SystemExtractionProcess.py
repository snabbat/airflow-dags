import os
import sys
from context_manager.JsonAccessorManager import JsonAccessorManager
from exception_manager.ExceptionManager import ExceptionManager
from logger.LogHandler import LogHandler


class SystemExtractionProcess:
    def __init__(self, accessor_manager: JsonAccessorManager, country: str, vertical: str, system_target: str, exception_manager: ExceptionManager):
        """
        Initialise le processus d'execution des flows d'extraction.
        :param country: Pays concerne
        :param vertical: Verticale metier
        :param system_target: Systeme source des donnees
        """

        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager
        self.country = country
        self.vertical = vertical
        self.system_target = system_target
        self.key = country + "-" + vertical + "-" + "extractor_dag"

    def run(self):
        """
        Execute les flows d'extraction definis dans la configuration DAG
        en prenant en compte la notion de "disable".
        """

        try:
            dag_config = self.accessor_manager.get(self.key).get_metadata()
            phases = dag_config.get("verticals", {}).get(self.vertical, {}).get("phases", {})
            extraction = phases.get("extraction", {}).get("target", {}).get("bronze", {}).get("system", {}).get(self.system_target, {})

            if "flow" not in extraction:
                print(f"Aucun flow defini pour {self.system_target} dans l'extraction.")
                return

            for flow in extraction["flow"]:
                flow_name = flow["name"]
                mandatory = flow["mandatory"]
                disable = flow.get("disable", False)

                if disable:
                    print(f"Flow {flow_name} est desactive. Il ne sera pas execute.")
                    continue

                print(f"Execution du flow: {flow_name}")
                accessor_manager = JsonAccessorManager()
                project_root = os.path.normpath(os.path.join(sys.path[0], "..", "..", ".."))
                accessor_manager.add_from_directory(os.path.join(project_root, "metadata", "extractor"))
                accessor_manager.add_from_directory(os.path.join(project_root, "config"))

                # FlowExtractionProcess non connecte ici — valeur temporaire
                success = False

                if not success and mandatory:
                    print(f"Erreur critique: le flow obligatoire {flow_name} a echoue. Arret du traitement avec code d'erreur 1.")
                    return False
                elif not success:
                    print(f"Flow {flow_name} a echoue mais est optionnel. Poursuite du traitement.")
                    continue

        except Exception as e:
            self.exception_manager.handle(e, {"classe": __name__, "system": self.system_target})
            return False
