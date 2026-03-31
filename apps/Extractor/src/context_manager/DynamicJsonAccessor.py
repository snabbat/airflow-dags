import json


class DynamicJsonAccessor:
    """Charge un fichier JSON et genere dynamiquement des methodes d'acces pour chaque cle, quel que soit le niveau d'imbrication."""

    def __init__(self, json_file):
        with open(json_file, 'r', encoding='utf-8') as f:
            self.metadata = json.load(f)

        # Generation dynamique des methodes d'acces
        self._generate_methods(self.metadata, prefix="")

    def _generate_methods(self, metadata, prefix):
        """Genere recursivement des methodes d'acces pour chaque cle de la hierarchie JSON."""
        if isinstance(metadata, dict):
            for key, value in metadata.items():
                new_prefix = f"{prefix}{key}_" if prefix else f"{key}_"
                method_name = f"get_{new_prefix[:-1]}"
                setattr(self, method_name, self._create_accessor(value))
                self._generate_methods(value, prefix=new_prefix)
        elif isinstance(metadata, list):
            for i, item in enumerate(metadata):
                self._generate_methods(item, prefix=f"{prefix}{i}_")

    def _create_accessor(self, value):
        """Cree une fonction lambda pour retourner la valeur (evite les problemes de fermeture de portee)."""
        return lambda: value

    def get_metadata(self):
        """Retourne l'integralite des donnees chargees depuis le JSON."""
        return self.metadata

    def set(self, key, value):
        """Ajoute ou modifie une paire cle-valeur dans la structure JSON."""
        self.metadata[key] = value

    def get(self, key):
        """Recupere la valeur associee a une cle donnee."""
        return self.metadata.get(key, None)
