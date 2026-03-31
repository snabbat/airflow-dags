from dag_manager.PhaseStrategy import PhaseStrategy


class CorePhase(PhaseStrategy):
    # ALTERED: added target parameter to match PhaseStrategy signature
    def build_tree(self, config, target=None, system_target=None, flows=None):
        pass  # Peut être complété

    # ALTERED: added full signature to match PhaseStrategy
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        print(f"Executing Core Phase in {self.recovery_mode} mode")
        return self.root.execute(country, vertical, target, system_target)

    # ALTERED: added full signature to match PhaseStrategy
    def run_task_full(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core FULL] Re-executing {leaf_name} fully.")
        return True

    # ALTERED: added full signature to match PhaseStrategy
    def run_task_execution_plan_based(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core PLAN-BASED] Executing {leaf_name} based on execution plan.")
        return True

    # ALTERED: added full signature to match PhaseStrategy
    def run_task_default(self, country: str, vertical: str, target: str, system_target: str, leaf_name: str):
        print(f"[Core Default] Processing {leaf_name}...")
        return True