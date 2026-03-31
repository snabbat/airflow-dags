from dag_manager.Node import Node


class Leaf(Node):
    def __init__(self, name, phase_strategy, mandatory=False, target_name=None, system_name=None):
        super().__init__(name)
        self.phase_strategy = phase_strategy  # Phase associee a cette feuille
        self.mandatory = mandatory
        self.target_name = target_name
        self.system_name = system_name

    def execute(self, country: str, vertical: str, target: str, system_target: str):
        """Execute la tache associee a cette feuille via la strategie de phase."""
        success = self.phase_strategy.run_task(
            country=country, vertical=vertical,
            target=self.target_name or target,
            system_target=self.system_name or system_target,
            leaf_name=self.name
        )
        if not success and self.mandatory:
            return False  # Stopper si echec obligatoire
        return True  # Continuer sinon
