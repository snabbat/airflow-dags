import json
from exception_manager.ExceptionHandler import ExceptionHandler


class ExceptionManager:
    """Gestionnaire central pour enregistrer les exceptions et charger la configuration des exceptions."""

    def __init__(self, config_path, file_logger, print_logger):
        self.chain = None
        self.config = self._load_config(config_path)
        self.file_logger = file_logger
        self.print_logger = print_logger
        # Configurer automatiquement les handlers a la creation
        self.setup_handlers()

    def _load_config(self, config_path):
        """Charge la configuration des exceptions a partir d'un fichier JSON."""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            raise RuntimeError(f"Erreur lors du chargement du fichier exceptions_config.json: {e}")

    def setup_handlers(self):
        """Configure les gestionnaires d'exceptions en fonction de la configuration chargee."""
        handler = ExceptionHandler(self.config)
        self.register_handler(handler)

    def register_handler(self, handler):
        """Enregistre un gestionnaire dans la chaine de responsabilite."""
        if self.chain:
            current = self.chain
            while current.next_handler:
                current = current.next_handler
            current.set_next(handler)
        else:
            self.chain = handler

    def handle(self, exception, context):
        """Passe l'exception dans la chaine de gestionnaires et enregistre l'exception."""
        if self.chain:
            error_info = self.chain.handle_exception(exception, context)
            if isinstance(error_info, dict):
                # Formater et enregistrer dans le fichier
                formatted_exception_file = self.file_logger.format_exception(error_info)
                self.file_logger.log_exception(formatted_exception_file)

                # Formater et afficher sur la console
                formatted_exception_console = self.print_logger.format_exception(error_info)
                self.print_logger.log_exception(formatted_exception_console)
            else:
                print("Erreur lors du traitement de l'exception : error_info n'est pas un dictionnaire.")
        else:
            print("Aucun gestionnaire enregistre.")

    def get_error_info(self, exception):
        """Recupere les informations de l'erreur via le handler."""
        if self.chain:
            return self.chain.get_error_info(exception)
        return {"message": str(exception)}
