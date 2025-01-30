import logging
from datetime import datetime
import json
import os


class LoggerManager:
    """Clase dedicada al manejo de logs"""

    def __init__(self, nombre_proceso="busqueda_partnumber"):
        self.nombre_proceso = nombre_proceso
        self.logger = self._setup_logging()

    def _setup_logging(self):
        """Configura y retorna el logger"""
        # Crear directorio de logs
        log_dir = "./logs"
        os.makedirs(log_dir, exist_ok=True)

        # Configurar logger
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"./logs/{self.nombre_proceso}_{timestamp}.log"

        logger = logging.getLogger(self.nombre_proceso)
        logger.setLevel(logging.INFO)

        # Formato del log
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Handler para archivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def log_inicio_proceso(self, total_registros, config):
        """Registra información de inicio del proceso"""
        self.logger.info("=== INICIO DE PROCESO DE BÚSQUEDA ===")
        self.logger.info(f"Fecha y hora de inicio: {datetime.now()}")
        self.logger.info(f"Total registros a procesar: {total_registros}")
        self.logger.info(f"Configuración: {json.dumps(config, indent=2)}")

    def log_proceso_lote(self, num_lote, total_lotes, coincidencias):
        """Registra información de procesamiento de lote"""
        self.logger.info(f"Procesando lote {num_lote}/{total_lotes}")
        self.logger.info(f"Coincidencias encontradas en lote: {coincidencias}")

    def log_error_registro(self, index, error):
        """Registra errores en el procesamiento de registros"""
        self.logger.error(f"Error procesando registro {index}: {str(error)}")

    def log_resultados_finales(self, stats):
        """Registra estadísticas finales del proceso"""
        self.logger.info("=== RESULTADOS FINALES ===")
        self.logger.info(f"Estadísticas: {json.dumps(stats, indent=2)}")
        self.logger.info("=== FIN DEL PROCESO ===")
