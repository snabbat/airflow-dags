from dag_manager.CompositeNode import CompositeNode
from dag_manager.Leaf import Leaf
from dag_manager.PhaseStrategy import PhaseStrategy
# ALTERED: renamed from ingestion.FlowIngestionProcessCible to extraction.FlowExtractionProcess
from extraction.FlowExtractionProcess import FlowExtractionProcess


# ALTERED: Renamed from IngestionPhase to ExtractionPhase
# ALTERED: build_tree() updated to navigate new hierarchy: target → system → flow (was: systeme_source → flow)
class ExtractionPhase(PhaseStrategy):
    def build_tree(self, config, target=None, system_target=None, flows=None):
        # ALTERED: added target level — was directly accessing 'systeme_source'
        if 'target' in config:
            for target_name, target_data in config['target'].items():
                if target_data.get("disable", False):
                    continue
                if target is not None and target_name != target:
                    continue
                target_node = CompositeNode(target_name)
                self.root.add(target_node)
                # ALTERED: 'systeme_source' renamed to 'system'
                if 'system' in target_data:
                    for system_name, system_data in target_data['system'].items():
                        if system_data.get("disable", False):
                            continue
                        if system_target is not None and system_name not in system_target:
                            continue
                        system_node = CompositeNode(system_name)
                        target_node.add(system_node)
                        # ALTERED: renamed variable 'flow' to 'flow'
                        for flow in system_data.get("flow", []):
                            if not flow.get("disable", False) and (flows is None or flow["name"] in flows):
                                # ALTERED: pass target_name and system_name so Leaf knows its tree context
                                system_node.add(Leaf(flow["name"], self, mandatory=flow.get("mandatory", False), target_name=target_name, system_name=system_name))

    # ALTERED: added target parameter to execute()
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        return self.root.execute(country, vertical, target, system_target)

    def run_task_full(self, country, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode FULL : réexécuter entièrement tous les flows"""
        return True  # Simulation de succès

    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode EXECUTION_PLAN_BASED : réexécuter selon un plan d'exécution"""
        return leaf_name != "bkhis"  # Simule un échec sur "bkhis"

    # ALTERED: renamed from ingestion terminology, added target parameter
    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Exécution par défaut"""
        # ALTERED: added target parameter — needed to build correct metadata path (country/vertical/target/system/flow.json)
        # ALTERED: renamed from FlowIngestionProcessCible to FlowExtractionProcess
        process = FlowExtractionProcess(accessor_manager=self.accessor_manager, country=country, vertical=vertical, target=target, system_target=system_target, flow_name=leaf_name, exception_manager=self.exception_manager)

        # ALTERED: retourne le resultat de process.run() au lieu de toujours retourner False
        return process.run()
