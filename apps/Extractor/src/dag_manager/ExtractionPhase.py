from dag_manager.CompositeNode import CompositeNode
from dag_manager.Leaf import Leaf
from dag_manager.PhaseStrategy import PhaseStrategy
from extraction.FlowExtractionProcess import FlowExtractionProcess


class ExtractionPhase(PhaseStrategy):
    def build_tree(self, config, target=None, system_target=None, flows=None):
        """Construit l'arbre DAG a partir de la configuration : target → system → flow."""
        if 'target' in config:
            for target_name, target_data in config['target'].items():
                if target_data.get("disable", False):
                    continue
                if target is not None and target_name != target:
                    continue
                target_node = CompositeNode(target_name)
                self.root.add(target_node)
                if 'system' in target_data:
                    for system_name, system_data in target_data['system'].items():
                        if system_data.get("disable", False):
                            continue
                        if system_target is not None and system_name not in system_target:
                            continue
                        system_node = CompositeNode(system_name)
                        target_node.add(system_node)
                        for flow in system_data.get("flow", []):
                            if not flow.get("disable", False) and (flows is None or flow["name"] in flows):
                                system_node.add(Leaf(flow["name"], self, mandatory=flow.get("mandatory", False), target_name=target_name, system_name=system_name))

    def execute(self, country: str, vertical: str, target: str, system_target: str):
        return self.root.execute(country, vertical, target, system_target)

    def run_task_full(self, country, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode FULL : reexecuter entierement tous les flows."""
        return True

    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Mode EXECUTION_PLAN_BASED : reexecuter selon un plan d'execution."""
        return leaf_name != "bkhis"

    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        """Execution par defaut — instancie et lance le FlowExtractionProcess."""
        process = FlowExtractionProcess(accessor_manager=self.accessor_manager, country=country, vertical=vertical, target=target, system_target=system_target, flow_name=leaf_name, exception_manager=self.exception_manager)
        return process.run()
