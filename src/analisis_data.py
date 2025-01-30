import matplotlib.pyplot as plt
import seaborn as sns
import ipywidgets as widgets
from ipywidgets import interact, interact_manual
import pandas as pd
import time


class AnalisisDatos:
    def __init__(self, dataframe=None):
        self.dataframe_analisis = dataframe
        self.lista_resumen_tipo_producto = None

    def tipo_producto_segmentacion(
        self,
        lista_filtro_sementacion,
        filtro_porcentaje_segmentacion,
        columna_probabilidad_segm,
    ):
        inicio = time.time()
        df_temp = self.dataframe_analisis.copy()
        lista_resumen_tipo_producto = list()
        for i in range(len(lista_filtro_sementacion)):
            lista_filtro_sementacion[i] = df_temp[
                (
                    df_temp[columna_probabilidad_segm]
                    >= filtro_porcentaje_segmentacion[i]
                )
                & (
                    df_temp[columna_probabilidad_segm]
                    < filtro_porcentaje_segmentacion[i + 1]
                )
            ]
            lista_resumen_tipo_producto.append(lista_filtro_sementacion[i])
        print(
            f"Segmentacion por tipo producto: Tiempo de ejecución: {time.time() - inicio}"
        )
        return lista_resumen_tipo_producto

    def resumen_segmetacion(
        self,
        lista_nombre_grupos,
        campo_fila,
        lista_filtro_sementacion,
        filtro_porcentaje_segmentacion,
        columna_probabilidad_segm,
        impr=None,
    ):
        try:
            inicio = time.time()
            lista_resumen_tipo_producto = self.tipo_producto_segmentacion(
                lista_filtro_sementacion,
                filtro_porcentaje_segmentacion,
                columna_probabilidad_segm,
            )
            for nombre, filtro in zip(lista_nombre_grupos, lista_resumen_tipo_producto):
                if impr == 1:
                    print(
                        "TIPOS DE PRODUCTOS NO ENCONTRADOS DE IMPORTACION SISTEMAS EN TABLAS GD\n"
                    )
                elif impr == 2:
                    print(
                        "TIPOS DE PRODUCTOS NO ENCONTRADOS DE IMPORTACION IA EN TABLAS GD\n"
                    )
                print(
                    f"Son {len(filtro)} descripciones con 0 posibilidad a ser encontrados en las tablas GD.\n-Entre los 10 mas presentativos: \n"
                )
                print(
                    filtro.value_counts(
                        campo_fila, normalize=False, sort=campo_fila
                    ).head(10)
                )  # .to_frame().style.background_gradient(cmap='Blues', axis=0))
                print("=" * 30)
                print(
                    filtro.value_counts(
                        "TIPO_PRODUCTO", normalize=True, sort=campo_fila
                    ).head(10)
                )  # .to_frame().style.background_gradient(cmap='Blues', axis=0))
                print("\n")
                print("=" * 70)
                print("\n")
            print(
                f"Resumen de segmentación: Tiempo de ejecución: {time.time() - inicio}"
            )
        except AttributeError as e:
            print(f"Objeto no posee atributo - resumen_segmetacion {e}")
        except Exception as e:
            print(f"Problema presentado en {e} - resumen_segmetacion")

    def tabla_interactiva(
        self, segmentador0, segmentador1, segmentador2, campo_fila, contar_por_campo
    ):
        try:
            inicio = time.time()
            df_temp = self.dataframe_analisis.copy()

            @interact
            def tabla_probab_tipProd_marca(
                segmentador_A=["Todos"]
                + list(df_temp[segmentador0].sort_values().unique()),
                segmentador_B=["Todos"]
                + list(df_temp[segmentador1].sort_values().unique()),
                segmentador_C=["Todos"]
                + list(df_temp[segmentador2].sort_values().unique()),
            ):
                # Filtrar los datos
                if segmentador_A != "Todos":
                    df = df_temp[df_temp[segmentador0] == segmentador_A]
                else:
                    df = df_temp

                if segmentador_B != "Todos":
                    df = df[df[segmentador1] == segmentador_B]

                if segmentador_C != "Todos":
                    df = df[df[segmentador2] == segmentador_C]

                # Crear la tabla dinámica
                tabla_pivot = pd.pivot_table(
                    df, index=campo_fila, values=contar_por_campo, aggfunc="count"
                ).style.background_gradient(cmap="Blues", axis=0)

                return tabla_pivot

            print(f"Tabla interactiva: Tiempo de ejecución: {time.time() - inicio}")
            return tabla_probab_tipProd_marca
        except AttributeError as e:
            print(f"Objeto no posee atributo - tabla_interactiva {e}")
