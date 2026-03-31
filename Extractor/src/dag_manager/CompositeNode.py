from dag_manager.Node import Node


class CompositeNode(Node):
    # ALTERED: added target parameter to execute() signature
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        #print(f"Executing {self.name}")
        for child in self.children:
            # ALTERED: added target to child.execute() call
            if not child.execute(country, vertical, target, system_target):
                return False  # Stop execution if a mandatory node fails
        return True