from dag_manager.PhaseStrategy import PhaseStrategy


class CorePhase(PhaseStrategy):
    def build_tree(self, config, target=None, system_target=None, flows=None):
        pass  # A completer

    def execute(self, country: str, vertical: str, target: str, system_target: str):
        print(f"Execution de la phase Core en mode {self.recovery_mode}")
        return self.root.execute(country, vertical, target, system_target)

    def run_task_full(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core FULL] Reexecution de {leaf_name} en mode complet.")
        return True

    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core PLAN-BASED] Execution de {leaf_name} selon le plan d'execution.")
        return True

    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core Defaut] Traitement de {leaf_name}...")
        return True
