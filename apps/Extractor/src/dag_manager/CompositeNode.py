from dag_manager.Node import Node


class CompositeNode(Node):
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        """Execute tous les enfants du noeud. Arrete si un noeud obligatoire echoue."""
        for child in self.children:
            if not child.execute(country, vertical, target, system_target):
                return False
        return True
