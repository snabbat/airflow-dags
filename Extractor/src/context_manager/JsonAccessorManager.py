import os
from typing import Dict

from context_manager.DynamicJsonAccessor import DynamicJsonAccessor


class JsonAccessorManager:
    def __init__(self):
        """
        Initialise un gestionnaire d'accessors JSON vide.
        """
        self.accessors = {}

    def add_from_directory(self, directory: str, root_key: str = ""):
        """
        Charge tous les fichiers JSON d'un répertoire et crée des DynamicJsonAccessor avec des clés basées sur l'arborescence.

        :param directory: Chemin du répertoire contenant les fichiers JSON.
        """
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    key = f"{root_key}-" + os.path.relpath(file_path, directory).replace(os.sep, "-").replace(".json",
                                                                                                              "") if root_key else os.path.relpath(
                        file_path, directory).replace(os.sep, "-").replace(".json", "")
                    self.accessors[key] = DynamicJsonAccessor(file_path)

    def add(self, key: str, accessor: DynamicJsonAccessor):
        """
        Ajoute un DynamicJsonAccessor à la liste sous une clé unique.
        """
        self.accessors[key] = accessor

    def remove(self, key: str):
        """
        Supprime un DynamicJsonAccessor de la liste.
        """
        if key in self.accessors:
            del self.accessors[key]

    def get(self, key: str) -> DynamicJsonAccessor:
        """
        Récupère un DynamicJsonAccessor par son nom clé.
        """
        return self.accessors.get(key, None)

    def get_accessors(self) -> Dict[str, DynamicJsonAccessor]:
        """
        Récupère l'ensemble des accessors sous forme de dictionnaire.
        """
        return self.accessors

    def update_accessor(self, key: str, accessor: DynamicJsonAccessor):
        """
        Met à jour un DynamicJsonAccessor existant dans la liste.
        """
        if key in self.accessors:
            self.accessors[key] = accessor