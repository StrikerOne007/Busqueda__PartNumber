import pandas as pd
import re
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv #leer archivo env
from src.conexionbd import Conexion_BD, DAO
from src.operaciones_dataframe import OperacionesDataframe
from src.analisis_data import AnalisisDatos

import sys
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

pd.options.display.max_rows = None  #vista de mayor cantidad de filas


class AppManager:
    data_deltron = None
    data_importacion = None
    data_modelo = None
    data_final_procesada = None
    grouped = None  
    def __init__(self):
        self.dao = DAO()
        self.operaciones = OperacionesDataframe()
        self.analisis = AnalisisDatos()

    def ejecutar_flujo_importacion(self, consulta, columnas, archivo_csv):
        # Cargar datos desde la base de datos
        print("\n==============================================================")
        print("\t\t DATA SUNAT")
        print("==============================================================")
        print("1.-Cargando datos desde SQL...")
        df_sql = self.dao.carga_data_sql(consulta, columnas)
        print(df_sql.head())
        if df_sql is not None:
            AppManager.data_importacion = df_sql
            print("Datos cargados desde SQL con éxito.\n")



    def ejecutar_flujo_data_deltron(self, consulta, columnas):
        # Cargar datos desde la base de datos
        print("==============================================================")
        print("\t\t TABLA PART NUMBERT GD")
        print("==============================================================")
        print("\n1.-Cargando datos desde SQL...")
        df_sql = self.dao.carga_data_sql(consulta, columnas)
        print(df_sql.head())
        if df_sql is not None:
            AppManager.data_deltron = df_sql
            pass #print("Datos cargados desde SQL con éxito.")



    def ejecutar_flujo_data_modelo(self,consulta,columnas):
        # Cargar datos desde la base de datos
        print("\n==============================================================")
        print("\t\t TABLA MODELO CRUCE PART NUMBERT GD")
        print("==============================================================")
        print("\n1.-Cargando datos desde SQL...")
        df_sql = self.dao.carga_data_sql(consulta, columnas)
        print(df_sql.head())
        if df_sql is not None:
            AppManager.data_modelo = df_sql
            pass #print("Datos cargados desde SQL con éxito.")



    def ejecutar_operaciones_dataframe(self):

        print("\n==============================================================")
        print("CRUCES PARA LA COINCIDENCIA DE PART NUMBER ENTRE TABLA IMPORTACION Y GD")
        print("==============================================================\n")
        self.operaciones.dataframe_importacion = AppManager.data_importacion
        self.operaciones.dataframe_deltron = AppManager.data_deltron
        self.operaciones.dataframe_modelo = AppManager.data_modelo
        # self.operaciones.dataframe_importacion.to_excel("df_importacion_revision.xlsx")


        print("1.-Aplicando operaciones para busqueda de datos por Sistemas...")
        df_busqueda_partnumber_importacion = self.operaciones.buscar_part_number(
            campo_data_deltron = 'PART_NUMBER',
            campo_iterable = 'value',
            nueva_columna = 'resultado_busqueda2',
            campo_descripcion = 'DS_DESC_COM',
            valores_nulos = {'value':'0','SA_Value':'0'}
        )
        if df_busqueda_partnumber_importacion is not None:
            print(df_busqueda_partnumber_importacion.head())


        print("\n CARGA DE DATA A BASE DE DATOS\n")
        df_to_sql = df_busqueda_partnumber_importacion[['resultado_busqueda2','ID']]
        print(df_to_sql.head(10))
        df_to_sql.to_excel("df_to_sql.xlsx")
        self.dao.actualiza_data_sql(df_to_sql, 'resultado_busqueda2', 'ID')




    #     print("-------------------------------------------------------------")
    #     print("2.-Conteo resultados de busqueda por Sistemas...")
    #     conteo_resultado_importacion = self.operaciones.conteo_resultados(
    #         indice_importacion = 'indice',
    #         resultado_busqueda = 'resultado_busqueda2'
    #     )
    #     if conteo_resultado_importacion is not None:
    #         print(f"-Se encontraron: {conteo_resultado_importacion['encontrado']}, de un total {conteo_resultado_importacion['encontrado']+conteo_resultado_importacion['no encontrado']}")


    #     print("-------------------------------------------------------------")
    #     print("\n3.-Aplicando operaciones para busqueda de datos sistema por modelo...")
    #     df_busqueda_partnumber_mod_importacion = self.operaciones.buscar_part_number_modelo(
    #         campo_importacion = 'value',
    #         campo_data_deltron = 'PART_NUMBER',
    #         campo_iterable = 'value',
    #         nueva_columna = 'resultado_busqueda',
    #         campo_descripcion = 'DS_DESC_COM',
    #         valores_nulos = {'MODELO':'0'},
    #         lista_resultado_busqueda = 'resultado_busqueda2'
    #     )
    #     if df_busqueda_partnumber_mod_importacion is not None:
    #         pass #print(df_busqueda_partnumber_mod_importacion.head())


    #     print("-------------------------------------------------------------")
    #     print("4.-Conteo resultados de busqueda por Sistemas - Modelo...")
    #     conteo_resultado_importacion = self.operaciones.conteo_resultados(
    #         indice_importacion = 'indice',
    #         resultado_busqueda = 'resultado_busqueda'
    #     )
    #     if conteo_resultado_importacion is not None:
    #         print(f"-Se encontraron: {conteo_resultado_importacion['encontrado']}, de un total {conteo_resultado_importacion['encontrado']+conteo_resultado_importacion['no encontrado']}")


    #     print("-------------------------------------------------------------")
    #     print("\n5.-Aplicando operaciones para busqueda de datos por IA...")
    #     df_busqueda_partnumber_importacion_IA = self.operaciones.buscar_part_number(
    #         campo_data_deltron = 'PART_NUMBER',
    #         campo_iterable = 'SA_Value',
    #         campo_descripcion = 'DS_DESC_COM',
    #         nueva_columna = 'resultado_busqueda_AI',
    #     )
    #     if df_busqueda_partnumber_importacion_IA is not None:
    #         pass #df_busqueda_partnumber_importacion_IA


    #     print("-------------------------------------------------------------")
    #     print("6.-Conteo resultados de busqueda por IA...")
    #     conteo_resultado_importacion = self.operaciones.conteo_resultados(
    #         indice_importacion = 'indice',
    #         resultado_busqueda = 'resultado_busqueda_AI'
    #     )
    #     if conteo_resultado_importacion is not None:
    #         print(f"-Se encontraron: {conteo_resultado_importacion['encontrado']}, de un total {conteo_resultado_importacion['encontrado']+conteo_resultado_importacion['no encontrado']}")

        
    #     print("-------------------------------------------------------------")
    #     print("\n7.-Aplicando operaciones para busqueda de modelos IA...")
    #     df_busqueda_partnumber_mod_importacion_IA = self.operaciones.buscar_part_number_modelo(
    #         campo_importacion = 'SA_Value',
    #         campo_data_deltron = 'PART_NUMBER',
    #         campo_iterable = 'SA_Value',
    #         nueva_columna = 'resultado_busqueda_AI_model',
    #         campo_descripcion = 'DS_DESC_COM',
    #         valores_nulos = {'MODELO':'0'},
    #         lista_resultado_busqueda = 'resultado_busqueda_AI'
    #     )
    #     if df_busqueda_partnumber_mod_importacion_IA is not None:
    #         pass #print(df_busqueda_partnumber_mod_importacion.head())

        
    #     print("-------------------------------------------------------------")
    #     print("8.-Conteo resultados de busqueda por IA - Modelo...")
    #     conteo_resultado_importacion = self.operaciones.conteo_resultados(
    #         indice_importacion = 'indice',
    #         resultado_busqueda = 'resultado_busqueda_AI_model'
    #     )
    #     if conteo_resultado_importacion is not None:
    #         print(f"-Se encontraron: {conteo_resultado_importacion['encontrado']}, de un total {conteo_resultado_importacion['encontrado']+conteo_resultado_importacion['no encontrado']}")

    #     print("-------------------------------------------------------------")
    #     print("8.1 limitando resultados de busqueda a 1")
    #     df_limitacion_resultados = self.operaciones.limitando_lista_resultados_a_1(
    #         campo_resultado_importacion = 'resultado_busqueda',
    #         campo_partnumber_deltron = 'PART_NUMBER',
    #         nuevo_campo_resultado_1="resultado_busqueda_1"
    #     )
    #     if df_limitacion_resultados is not None:
    #         pass #print(df_limitacion_resultados.head())

    #     print("-------------------------------------------------------------")
    #     print("9.-Creando columnas de largo de part number...")
    #     df_conteo_campo_largo = self.operaciones.conteo_caracteres_alfanumerico(
    #         alfanumerico_sistm = 'value',
    #         alfanumerico_ia = 'SA_Value',
    #         largo_alfanumerico_sistemas = 'size_value',
    #         largo_alfanumerico_ia = 'size_SA_Value',
    #         campo_ajuste_resultado_sist = 'resultado_busqueda3',
    #         eliminar_campos = ['variable','resultado_busqueda2', 'resultado_busqueda', ],
    #         resultado_busqueda_sist = 'resultado_busqueda2',
    #         resultado_busqueda_sist_modl = 'resultado_busqueda'
    #     )
    #     if df_conteo_campo_largo is not None:
    #         pass #print(df_conteo_campo_largo['value'].head())


    #     print("-------------------------------------------------------------")
    #     print("10.-Generando porcentajes de coincidencia...")
    #     df_porcentajes_coincidencia = self.operaciones.porcentajes_coicidencia(
    #         indice = 'indice',
    #         tam_value = 'size_value',
    #         tam_SA_value = 'size_SA_Value',
    #         resultado_busqueda = 'resultado_busqueda3'
    #     )
    #     if df_porcentajes_coincidencia is not None:
    #         df_porcentajes_coincidencia.to_excel("df_porcentajes_coincidencia.xlsx")
    #         pass #print(df_porcentajes_coincidencia.head())

        
    #     print("-------------------------------------------------------------")
    #     print("11.-Procesando data final...")
    #     df_final_procesada = self.operaciones.procesar_data_final(
    #         seleccion_col_datafinal =    ['ID', 'indice','MARCA','TIPO_PRODUCTO','DS_IMPORTADOR','DS_DESC_COM','resultado_busqueda_1'],
    #         seleccion_col_datafinal_ia = ['ID', 'indice','SA_Value','resultado_busqueda_AI_model',
    #                                       'size_resultado_IA','porcentaje_coincidencia_IA',],
    #         seleccion_col_datafinal_log = ['ID', 'indice','value','resultado_busqueda3','size_resultado',
    #                                        'porcentaje_coincidencia',],
    #         eliminar_filas_vacias = 'resultado_busqueda3',
    #         columna_duplicada = 'indice',
    #         merge_importacion_log = ['indice', 'value','resultado_busqueda3','size_resultado',
    #                                  'porcentaje_coincidencia',],
    #         merge_importacion_ia = ['indice','SA_Value','resultado_busqueda_AI_model','size_resultado_IA',
    #                                 'porcentaje_coincidencia_IA',],
    #         ordenar_campos = ['ID', 'indice', 'MARCA', 'TIPO_PRODUCTO','DS_IMPORTADOR','DS_DESC_COM',
    #                           'PART_NUMBER_IMPORTACION_SISTEMAS', 'PART_NUMBER_IMPORTACION_IA',
    #                           'part_number_sistemas_GD','resultado_busqueda_1', 'resultado_busqueda_AI_model',
    #                           'Probalidad_sistemas_GD', 'Probalidad_IA',],
    #         campos_renombrados = {'porcentaje_coincidencia':'Probalidad_sistemas_GD',
    #                               'porcentaje_coincidencia_IA':'Probalidad_IA',
    #                               'SA_Value':'PART_NUMBER_IMPORTACION_IA',
    #                               'value':'PART_NUMBER_IMPORTACION_SISTEMAS',
    #                               'resultado_busqueda3':'part_number_sistemas_GD',
    #                               'size_value':'TAMAÑO_PART_NUMBER_IMPORTACION_SISTEMAS'}
    #     )
    #     if df_final_procesada is not None:
    #         AppManager.data_final_procesada = df_final_procesada





    # def ejecutar_analisis_datos(self):
    #     try:
    #         self.analisis.dataframe_analisis = AppManager.data_final_procesada

    #         print("==============================================================")
    #         print("="*80, sep="\n")
    #         print("\nCONTEO DE RESULTADOS POR COINCIDENCIA DE TABLA IMPORTACION DELTRON Y PART NUMBER DE LA TABLA GD \n")
    #         print(80*"=")
    #         resumen_tipo_producto = self.analisis.resumen_segmetacion(
    #             lista_nombre_grupos = ['grup_tipos_productos_segm_sist_0','grup_tipos_productos_segm_sist_10_40',
    #                                 'grup_tipos_productos_segm_sist_50_80','grup_tipos_productos_segm_sist_90_100'],
    #             campo_fila = 'TIPO_PRODUCTO',
    #             lista_filtro_sementacion = ['tipos_productos_segm_sist_0','tipos_productos_segm_sist_10_40',
    #                                     'tipos_productos_segm_sist_50_80','tipos_productos_segm_sist_90_100'],
    #             filtro_porcentaje_segmentacion = [0,10,50,90,100],
    #             columna_probabilidad_segm = 'Segm_prob_sistemas_GD',
    #             impr=1
    #         )
    #         if resumen_tipo_producto is not None:
    #             print(resumen_tipo_producto)

    #         print("="*80, sep="\n")
    #         print("\nCONTEO DE RESULTADOS POR COINCIDENCIA DE TABLA IMPORTACION DELTRON Y PART NUMBER DE LA TABLA GD \n")
    #         print(80*"=")
    #         resumen_tipo_producto = self.analisis.resumen_segmetacion(
    #             lista_nombre_grupos = ['grup_tipos_productos_segm_sist_0','grup_tipos_productos_segm_sist_10_40',
    #                                 'grup_tipos_productos_segm_sist_50_80','grup_tipos_productos_segm_sist_90_100'],
    #             campo_fila = 'TIPO_PRODUCTO',
    #             lista_filtro_sementacion = ['tipos_productos_segm_sist_0','tipos_productos_segm_sist_10_40',
    #                                     'tipos_productos_segm_sist_50_80','tipos_productos_segm_sist_90_100'],
    #             filtro_porcentaje_segmentacion = [0,10,50,90,100],
    #             columna_probabilidad_segm = 'Segm_prob_IA',
    #             impr=2
    #         )
    #         if resumen_tipo_producto is not None:
    #             print(resumen_tipo_producto)


    #         print("\n TABLA RESUMEN DE PROBABILIDAD POR PART NUMBER SISTEMAS\n")
    #         resumen_tipo_producto = self.analisis.tabla_interactiva(
    #             segmentador0='DS_IMPORTADOR',
    #             segmentador1='MARCA',
    #             segmentador2='TIPO_PRODUCTO',
    #             campo_fila= ['Segm_prob_sistemas_GD', 'MARCA'],
    #             contar_por_campo = 'ID'
    #         )
    #         if resumen_tipo_producto is not None:
    #             print(resumen_tipo_producto)


    #         print("\n TABLA RESUMEN DE PROBABILIDAD POR PART NUMBER IA\n")
    #         resumen_tipo_producto = self.analisis.tabla_interactiva(
    #             segmentador0='DS_IMPORTADOR',
    #             segmentador1='MARCA',
    #             segmentador2='TIPO_PRODUCTO',
    #             campo_fila= ['Segm_prob_IA', 'MARCA'],
    #             contar_por_campo = 'ID'
    #         )
    #         if resumen_tipo_producto is not None:
    #             print(resumen_tipo_producto)
    

    #         print("\n CARGA DE DATA A BASE DE DATOS\n")
    #         df_to_sql = AppManager.data_final_procesada[['resultado_busqueda_1','Segm_prob_sistemas_GD','ID']]
    #         df_to_sql['Segm_prob_sistemas_GD'] = df_to_sql.apply(
    #             lambda x: 'no registra part number con este registro' if x['Segm_prob_sistemas_GD'] == 0 else x['Segm_prob_sistemas_GD'], axis=1
    #         )
    #         df_to_sql['Segm_prob_sistemas_GD'] = df_to_sql.apply(
    #                 lambda x: 'Menor al 0.6 coincidencia parcial o sin coincidencia' if (not isinstance(x['Segm_prob_sistemas_GD'], str) and x['Segm_prob_sistemas_GD'] < 60)  else x['Segm_prob_sistemas_GD'], axis=1
    #             )
    #         df_to_sql['resultado_busqueda_1'] = df_to_sql.apply(
    #             lambda x: '' if x['Segm_prob_sistemas_GD'] == 'Menor al 0.6 coincidencia parcial o sin coincidencia' else x['resultado_busqueda_1'], axis=1
    #         )
    #         print(df_to_sql.head(10))
    #         #df_to_sql.to_excel("df_to_sql.xlsx")
    #         self.dao.actualiza_data_sql(df_to_sql, 'resultado_busqueda_1', 'Segm_prob_sistemas_GD', 'ID')
    #     except TypeError as e:
    #         print("Objeto no puede ser iterado - ejecutar_analisis_datos", e)
    #     except Exception as e:
    #         print(f"Problema presentado en {e} - ejecutar_analisis_datos")

