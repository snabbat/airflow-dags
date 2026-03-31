import pandas as pd
from typing import Dict


class DataContext:
    """
    Encapsulates data, metadata, and configuration for a dataset.
    """

    def __init__(self, data: pd.DataFrame, metadata: Dict, config: Dict):
        """
        Constructor for DataContext.

        :param data: The dataset as a Pandas DataFrame.
        :param metadata: The metadata dictionary associated with the data.
        :param config: The configuration dictionary for the data context.
        """

        self.data = data
        self.metadata = metadata
        self.config = config

        

    def get_data(self) -> pd.DataFrame:
        """
        Returns the data (DataFrame).

        :return: Pandas DataFrame containing the dataset.
        """
        return self.data

    def get_metadata(self) -> Dict:
        """
        Returns the metadata dictionary.

        :return: Metadata dictionary.
        """
        return self.metadata

    def get_config(self) -> Dict:
        """
        Returns the configuration dictionary.

        :return: Configuration dictionary.
        """
        return self.config
