from dag_manager.PhaseStrategy import PhaseStrategy


class MartsPhase(PhaseStrategy):
    def build_tree(self, config, target=None, system_target=None, flows=None):
        pass  # A completer

    def execute(self, country: str, vertical: str, target: str, system_target: str):
        print(f"Execution de la phase Marts en mode {self.recovery_mode}")
        return self.root.execute(country, vertical, target, system_target)

    def run_task_full(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Marts FULL] Reexecution de {leaf_name} en mode complet.")
        return True

    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Marts PLAN-BASED] Execution de {leaf_name} selon le plan d'execution.")
        return leaf_name != "flux1"

    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Marts Defaut] Traitement de {leaf_name}...")
        return leaf_name != "flux1"
