import pandas as pd
from typing import Dict


class DataContext:
    """Encapsule les donnees, metadonnees et configuration d'un jeu de donnees."""

    def __init__(self, data: pd.DataFrame, metadata: Dict, config: Dict):
        self.data = data
        self.metadata = metadata
        self.config = config

    def get_data(self) -> pd.DataFrame:
        """Retourne les donnees (DataFrame)."""
        return self.data

    def get_metadata(self) -> Dict:
        """Retourne le dictionnaire de metadonnees."""
        return self.metadata

    def get_config(self) -> Dict:
        """Retourne le dictionnaire de configuration."""
        return self.config
