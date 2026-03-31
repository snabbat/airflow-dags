import os
from typing import Dict

from context_manager.DynamicJsonAccessor import DynamicJsonAccessor


class JsonAccessorManager:
    """Gestionnaire centralise des accesseurs JSON. Charge et organise les fichiers de configuration et metadonnees."""

    def __init__(self):
        self.accessors = {}

    def add_from_directory(self, directory: str, root_key: str = ""):
        """Charge tous les fichiers JSON d'un repertoire et cree des accesseurs avec des cles basees sur l'arborescence."""
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    key = f"{root_key}-" + os.path.relpath(file_path, directory).replace(os.sep, "-").replace(".json",
                                                                                                              "") if root_key else os.path.relpath(
                        file_path, directory).replace(os.sep, "-").replace(".json", "")
                    self.accessors[key] = DynamicJsonAccessor(file_path)

    def add(self, key: str, accessor: DynamicJsonAccessor):
        """Ajoute un accesseur JSON sous une cle unique."""
        self.accessors[key] = accessor

    def remove(self, key: str):
        """Supprime un accesseur JSON de la liste."""
        if key in self.accessors:
            del self.accessors[key]

    def get(self, key: str) -> DynamicJsonAccessor:
        """Recupere un accesseur JSON par sa cle."""
        return self.accessors.get(key, None)

    def get_accessors(self) -> Dict[str, DynamicJsonAccessor]:
        """Retourne l'ensemble des accesseurs sous forme de dictionnaire."""
        return self.accessors

    def update_accessor(self, key: str, accessor: DynamicJsonAccessor):
        """Met a jour un accesseur JSON existant."""
        if key in self.accessors:
            self.accessors[key] = accessor
