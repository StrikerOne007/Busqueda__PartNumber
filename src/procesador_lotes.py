class ProcesadorLotes:
    """Clase responsable del procesamiento por lotes"""

    def __init__(self, batch_size=3000):
        self.batch_size = batch_size
        self.logger = LoggerManager()

    def procesar_lotes(self, dataframe, funcion_proceso):
        """Procesa el dataframe en lotes"""
        total_registros = len(dataframe)
        resultados = []

        for i in range(0, total_registros, self.batch_size):
            batch = dataframe.iloc[i : i + batch_size]
            resultados_batch = self._procesar_lote(batch, funcion_proceso)
            resultados.extend(resultados_batch)

        return resultados

    def _procesar_lote(self, batch, funcion_proceso):
        """Procesa un lote individual"""
        resultados_batch = []
        for _, registro in batch.iterrows():
            try:
                resultado = funcion_proceso(registro["DS_DESC_COM"])
                resultados_batch.append(resultado)
            except Exception as e:
                self.logger.log_error_registro(registro.name, e)
                resultados_batch.append(None)
        return resultados_batch
