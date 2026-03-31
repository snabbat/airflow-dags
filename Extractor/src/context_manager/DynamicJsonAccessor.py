



import json

class DynamicJsonAccessor:
    def __init__(self, json_file):
        """
        Charge un fichier JSON et génère dynamiquement des méthodes pour accéder aux valeurs des clés,
        quel que soit le niveau d'imbrication.
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            self.metadata = json.load(f)

        # Générer dynamiquement des méthodes
        self._generate_methods(self.metadata, prefix="")

    def _generate_methods(self, metadata, prefix):
        """
        Génère dynamiquement des méthodes pour accéder aux clés JSON hiérarchiques récursivement.
        """
        if isinstance(metadata, dict):
            for key, value in metadata.items():
                new_prefix = f"{prefix}{key}_" if prefix else f"{key}_"
                method_name = f"get_{new_prefix[:-1]}"  # Retirer le dernier "_"
                setattr(self, method_name, self._create_accessor(value))
                self._generate_methods(value, prefix=new_prefix)
        elif isinstance(metadata, list):
            for i, item in enumerate(metadata):
                self._generate_methods(item, prefix=f"{prefix}{i}_")

    def _create_accessor(self, value):
        """
        Crée une fonction qui retourne une valeur pour éviter les problèmes de fermeture de portée.
        """
        return lambda: value

    def get_metadata(self):
        """
        Retourne l'intégralité des données chargées depuis le JSON.
        """
        return self.metadata

    def set(self, key, value):
        """
        Ajoute une nouvelle paire clé-valeur à la structure JSON.
        """
        self.metadata[key] = value

    def get(self, key):
        """
        Récupère une valeur associée à une clé donnée.
        """
        return self.metadata.get(key, None)



