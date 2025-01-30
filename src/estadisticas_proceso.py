class EstadisticasProceso:
    """Clase responsable de las estadísticas del proceso"""

    def __init__(self):
        self.inicio = time.time()
        self.stats = {"total_procesados": 0, "coincidencias": 0, "errores": 0}

    def registrar_resultado(self, resultado):
        """Registra un resultado individual"""
        self.stats["total_procesados"] += 1
        if resultado is not None:
            self.stats["coincidencias"] += 1

    def registrar_error(self):
        """Registra un error"""
        self.stats["errores"] += 1

    def obtener_estadisticas_finales(self):
        """Calcula y retorna estadísticas finales"""
        tiempo_total = time.time() - self.inicio
        return {
            **self.stats,
            "tiempo_total_segundos": round(tiempo_total, 2),
            "tiempo_promedio_por_registro": round(
                tiempo_total / self.stats["total_procesados"], 4
            ),
            "fecha_hora_fin": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
