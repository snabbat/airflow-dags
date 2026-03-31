from dag_manager.Node import Node


class Leaf(Node):
    # ALTERED: added target_name and system_name to store tree context on each leaf
    def __init__(self, name, phase_strategy, mandatory=False, target_name=None, system_name=None):
        super().__init__(name)
        self.phase_strategy = phase_strategy  # Stocker la phase associée
        self.mandatory = mandatory
        self.target_name = target_name
        self.system_name = system_name

    # ALTERED: Leaf now uses its own target_name/system_name from tree context instead of relying on params that may be None
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        success = self.phase_strategy.run_task(
            country=country, vertical=vertical,
            target=self.target_name or target,
            system_target=self.system_name or system_target,
            leaf_name=self.name
        )
        if not success and self.mandatory:
            return False  # Stopper si échec obligatoire
        return True  # Continuer sinon