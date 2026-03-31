from abc import ABC, abstractmethod


class Node(ABC):
    def __init__(self, name):
        self.name = name
        self.children = []

    def add(self, child):
        self.children.append(child)

    @abstractmethod
    # ALTERED: added target parameter to execute() signature
    def execute(self, country: str, vertical: str, target: str, system_target: str):
        pass