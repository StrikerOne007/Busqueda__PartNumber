from src.conexionbd import DAO

import operator
import time
import pandas as pd
import re
from rapidfuzz import fuzz
import numpy as np
import sys
sys.path.append("C:\\Users\\gustavo.grillo\\1.ANALISIS_DATA\\SERVIDOR\\busqueda_partnumber\\src")
import fuzzrapid



class OperacionesDataframe():
    def __init__(self,dataframe_importacion=None,dataframe_deltron=None,dataframe_modelo=None,
                data_final=None, data_final_sist=None, data_final_ia=None):
        self.dataframe_importacion = dataframe_importacion
        self.dataframe_deltron = dataframe_deltron
        self.dataframe_modelo = dataframe_modelo
        self.conexion_dao = DAO()



    def buscar_part_number(self,campo_data_deltron,campo_iterable,nueva_columna,campo_descripcion,valores_nulos=None):
        """ 
        """

        # try:
        inicio = time.time()
        self.dataframe_importacion.drop_duplicates(subset=['DS_DESC_COM'], keep='first', inplace=True)
        df_importacion = self.dataframe_importacion.copy()
        df_deltron = self.dataframe_deltron.copy()
        df_modelo = self.dataframe_modelo.copy()
        df_modelo = df_modelo[df_modelo['PART_NUMBER'].str.len() > 4]
        df_deltron = df_deltron[df_deltron['PART_NUMBER'].str.len() > 4]
        df_importacion['DS_DESC_COM'] = df_importacion['DS_DESC_COM'].str.upper().str.strip()
        df_deltron['PART_NUMBER'] = df_deltron['PART_NUMBER'].str.upper().str.strip()
        df_modelo['PART_NUMBER'] = df_modelo['PART_NUMBER'].str.upper().str.strip()

        if valores_nulos:
            df_importacion = df_importacion.fillna(valores_nulos)


        # @profile
        def busqueda_por_igualdad(campo_descripcion, conjunto_part_numbers, conjunto_pnumber_modelo,partnumber_found_desc):

            if partnumber_found_desc is None:
                partnumber_found_desc = []

            

            lista_descripcion = sorted(re.findall(r'\b[\w./#()-]+\b',campo_descripcion), key=len, reverse=True)

            palabras_filtradas = [
                palabra for palabra in lista_descripcion 
                    if len(palabra) > 4 and palabra[:5] != "RYZEN" and (
                        ( any(c.isalpha() for c in palabra) and any(c.isdigit() for c in palabra) ) or
                        (palabra.replace('.', '').isdigit() and '.' in palabra) or
                        (palabra.replace('-', '').isdigit() and '-' in palabra) or
                        (palabra.isdigit())
                    )
                ]
            

            print(palabras_filtradas)
        # HACER BUSQUEDA POR IGUALDAD DE PARTNUMBER
        

            lista_modelo, lista_parnum_mod, lista_codproducto = map(list, zip(*conjunto_pnumber_modelo))

            
            if campo_descripcion.startswith("TARJETA MADRE, TEROS") or campo_descripcion.startswith("COMPUTADORA, MSI")\
                or campo_descripcion.startswith("COMPUTADORA, LENOVO") or campo_descripcion.startswith("COMPUTADORA,LENOVO")\
                    or campo_descripcion[:19] == "COMPUTADORA,ADVANCE" or campo_descripcion.startswith("COMPUTADORA, ADVANCE,")\
                        or campo_descripcion.startswith("COMPUTADORA,HEWLETT") or campo_descripcion.startswith("COMPUTADORA,DELL")\
                            or 'ASUS' in campo_descripcion:
                for palabra in palabras_filtradas:

                    if len(palabra) > 5 and palabra in lista_modelo:                     
                        indice = lista_modelo.index(palabra)
                        indice_partnumber = lista_parnum_mod[indice]
                        partnumber_found_desc.append(indice_partnumber)
                        return "@modelo-" + indice_partnumber
                    
                    if len(palabra) > 5 and palabra in lista_codproducto:
                        indice = lista_codproducto.index(palabra)
                        indice_partnumber = lista_parnum_mod[indice]
                        partnumber_found_desc.append(indice_partnumber)
                        return "@codprodc-" + indice_partnumber
                    
                    if campo_descripcion.startswith("COMPUTADORA, LENOVO") or campo_descripcion.startswith("COMPUTADORA,LENOVO"):
                        for modelo in lista_modelo:
                            # Primero eliminamos todo lo que no necesitamos
                            patron = re.compile(
                                r'^.*?(?:ideapad|IDEAPAD)'  # todo hasta ideapad/IDEAPAD
                                r'(?:\s+(?:gaming|slim|flex))?\s*'  # palabras opcionales como gaming/slim/flex
                                r'(?:\d+\s+)?'  # números opcionales
                                r'(?=\w+$)',    # hasta encontrar la última palabra
                                re.IGNORECASE)
                            
                            modelo_sin_patron = re.sub(patron, '', modelo)
                            modelo_limpio = modelo_sin_patron.strip()
                            
                            
                            if modelo_limpio:  # Solo si queda algo después de limpiar
                                palabras_clave = re.findall(modelo_limpio if len(modelo_limpio) > 3 else modelo, campo_descripcion)
                            
                            for palabra in palabras_clave:
                            
                                if isinstance(palabra, str):
                                    indice_modelo = lista_modelo.index(modelo)
                                    indice_partnumber = lista_parnum_mod[indice_modelo]
                                    partnumber_found_desc.append(indice_partnumber)
                                    return "@modelo2-" + indice_partnumber
                                
                    if campo_descripcion.startswith("COMPUTADORA, ASUS") or campo_descripcion.startswith("COMPUTADORA,ASUS"):
                        for modelo in lista_modelo:
                            # Eliminar todo después del / (incluyendo espacios opcionales antes)
                            modelo_preview = modelo
                            modelo = re.sub(r'\s*/.*$', '', modelo)
                            palabras_clave = re.findall(modelo, campo_descripcion)
                            for palabra in palabras_clave:

                                if isinstance(palabra, str):
                                    indice_modelo = lista_modelo.index(modelo_preview)
                                    indice_partnumber = lista_parnum_mod[indice_modelo]
                                    partnumber_found_desc.append(indice_partnumber)
                                    return "@modelo2-" + indice_partnumber
                                
                                
                    if campo_descripcion.startswith("COMPUTADORA,HEWLETT"):
                        for modelo in lista_modelo:
                            palabras_clave = re.findall(modelo, campo_descripcion)
                            for palabra in palabras_clave:
                                if isinstance(palabra, str):
                                    indice_modelo = lista_modelo.index(modelo)
                                    indice_partnumber = lista_parnum_mod[indice_modelo]
                                    partnumber_found_desc.append(indice_partnumber)
                                    return "@modelo2-" + indice_partnumber
                                
                    if campo_descripcion.startswith("COMPUTADORA,ADVANCE") or campo_descripcion.startswith("COMPUTADORA, ADVANCE,"):
                        conjunto_part_numbers = np.array(conjunto_part_numbers)
                        conjunto_part_numbers = conjunto_part_numbers[::-1]
                        for partnumber in conjunto_part_numbers:
                            if partnumber.startswith("ADV-"):
                                if partnumber[:10] in palabras_filtradas: 
                                    partnumber_found_desc.append(partnumber)
                                    return "@ADV_inicio-" + partnumber
                                elif partnumber[4:10] in palabras_filtradas: 
                                    partnumber_found_desc.append(partnumber)
                                    return "@ADV_inicio-" + partnumber
                            


            
            if campo_descripcion.startswith("MOTHERBOARD,GIGABYTE") or campo_descripcion.startswith("MOTHERBOARD, GIGABYTE"):
                for partnumber in conjunto_part_numbers:
                    palabras_clave = re.findall(re.escape(partnumber), campo_descripcion)
                    for palabra in palabras_clave:

                        if isinstance(palabra, str):
                            partnumber_found_desc.append(partnumber)
                            return "@partnumber_inicia_GIGABYTE-" + partnumber


            if campo_descripcion.startswith("COMPUTADORA,LENOVO"):
                for palabra in palabras_filtradas:
                    palabra = palabra.replace('-', '')
                    if palabra in conjunto_part_numbers:
                        partnumber_found_desc.append(palabra) 
                        return "@partnumber_inicia_LENOVO-" + palabra



            if campo_descripcion[:13] == "TARJETA MADRE" or campo_descripcion[:15] == "TARJETA GRAFICA" \
                or campo_descripcion.startswith("MOUSE., TEROS"):
                for partnumber in conjunto_part_numbers:

                    if partnumber in campo_descripcion:
                        partnumber_found_desc.append(partnumber)
                        return "@tarjeta-" + partnumber            



            if campo_descripcion.startswith("COMPUTADORA, LENOVO"):
                palabras_filtradas = [palabra.replace('-', '') for palabra in palabras_filtradas]
                for partnumber in conjunto_part_numbers:

                    if partnumber in palabras_filtradas:
                        partnumber_found_desc.append(partnumber)
                        return "@partnumber_inicia_LENOVO-" + partnumber

                                
            
        
            conjunto_partnumber_hashtag = [pn[:pn.find("#")] for pn in conjunto_part_numbers if "#" in pn]
            if len(palabras_filtradas) > 0:
                for palabra in palabras_filtradas:

                    if palabra in conjunto_partnumber_hashtag:
                        partnumber_found_desc.append(palabra)
                        return palabra
                             


            if len(palabras_filtradas) > 0:
                for palabra in palabras_filtradas:

                    if palabra in conjunto_part_numbers:
                        partnumber_found_desc.append(palabra)
                        return palabra



         # HACER BUSQUEDA POR SIMILITUD DE PARTNUMBER

            # conjunto_part_numbers = np.unique(conjunto_part_numbers)
            # partnumber_found_desc = np.unique(partnumber_found_desc)
            # saldo_part_num = conjunto_part_numbers[~np.isin(conjunto_part_numbers, partnumber_found_desc)]
            saldo_part_num_chunks = sorted(conjunto_part_numbers, key=len, reverse=True)
            mask = np.array([len(x) > 8 for x in saldo_part_num_chunks])
            saldo_part_num_chunks_lenmay_8 = np.array(saldo_part_num_chunks)[mask]
            chunks = np.array_split(saldo_part_num_chunks_lenmay_8, max(1, len(saldo_part_num_chunks_lenmay_8) // 1000))
            

            if "HEWLETT PACKARD" in campo_descripcion:
                hp_chunks = [pn for pn in saldo_part_num_chunks_lenmay_8 if "#" in pn]
                chunks = np.array_split(hp_chunks, max(1, len(hp_chunks) // 1000))
                
                mejor_similitud = 0
                mejor_partnumber = None
                umbral = 60
                
                for chunk in chunks:
                    for partnumb_del in chunk:
                        similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)
                        # print(f"Comparando: {partnumb_del} -> Similarity: {similarity}")
                        if similarity > mejor_similitud and similarity > umbral:
                            mejor_similitud = similarity
                            mejor_partnumber = partnumb_del
                return f"@simil_prox-{mejor_partnumber}" if mejor_partnumber else None
            
            elif campo_descripcion.startswith("COMPUTADORA, LENOVO") or campo_descripcion.startswith("COMPUTADORA,LENOVO")\
                or campo_descripcion.startswith("MEMORIA RAM, KINGSTON") or 'TEROS' in campo_descripcion\
                    or 'AORUS' in campo_descripcion or 'ASUS' in campo_descripcion\
                        or 'SEAGATE' in campo_descripcion or 'ADVANCE' in campo_descripcion:
                mejor_similitud = 0
                mejor_partnumber = None
                umbral = 75

                for chunk in chunks:
                    for partnumb_del in chunk:
                        similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)
                        # print(f"Comparando: {partnumb_del} -> Similarity: {similarity}")
                        if similarity > mejor_similitud and similarity > umbral:
                            mejor_similitud = similarity
                            mejor_partnumber = partnumb_del
                return f"@simil_prox-{mejor_partnumber}" if mejor_partnumber else None
            
            elif campo_descripcion.startswith("TARJETA MADRE") or 'GIGABYTE' in campo_descripcion\
                    or 'MSI' in campo_descripcion or 'WESTER' in campo_descripcion:
                mejor_similitud = 0
                mejor_partnumber = None
                umbral = 60
                
                for chunk in chunks:
                    for partnumb_del in chunk:
                        similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)

                        if similarity > mejor_similitud and similarity > umbral:
                            mejor_similitud = similarity
                            mejor_partnumber = partnumb_del

                return f"@simil_prox-{mejor_partnumber}" if mejor_partnumber else None
            
            elif campo_descripcion.startswith("TARJETA GRAFICA"):
                mask = [len(pn) > 10  for pn in saldo_part_num_chunks_lenmay_8]
                hp_chunks = np.array(saldo_part_num_chunks_lenmay_8)[mask]
                chunks = np.array_split(hp_chunks, max(1, len(hp_chunks) // 1000))
                mejor_similitud = 0
                mejor_partnumber = None
                umbral_95 = 95
                
                # Primera pasada con umbral alto
                for chunk in chunks:
                    for partnumb_del in chunk:
                        similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)
                        if similarity > mejor_similitud and similarity > umbral_95:
                            mejor_similitud = similarity  
                            mejor_partnumber = partnumb_del

                return f"@simil_prox-{mejor_partnumber}" if mejor_partnumber else None
            
            elif 'RX' in campo_descripcion or 'RTX' in campo_descripcion or 'GTX' in campo_descripcion\
                or 'LANDBYTE' in campo_descripcion:              
                mejor_similitud = 0
                mejor_partnumber = None
                umbral = 77.5

                for chunk in chunks:
                    for partnumb_del in chunk:
                        similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)
                        # print(f"Comparando: {partnumb_del} -> Similarity: {similarity}")
                        if similarity > mejor_similitud and similarity > umbral:
                            mejor_similitud = similarity
                            mejor_partnumber = partnumb_del
                return f"@simil_prox-{mejor_partnumber}" if mejor_partnumber else None
                

        

            



        conjunto_part_numbers = df_deltron['PART_NUMBER'].values
        conjunto_part_numbers = np.array(sorted(conjunto_part_numbers, key=len, reverse=True))


        conjunto_pnumber_modelo = df_modelo[['MODELO','PART_NUMBER','PRODUCTO']].values
        conjunto_pnumber_modelo = np.array(sorted(conjunto_pnumber_modelo, key=operator.itemgetter(1), reverse=True))


        # Aplicar la función optimizada
        list_partnumber = []
        resultado = df_importacion['DS_DESC_COM'].apply(busqueda_por_igualdad, args=(conjunto_part_numbers,conjunto_pnumber_modelo,list_partnumber))
   
   
   

        df_importacion[nueva_columna] = resultado




        # df_importacion.to_excel("./return_data/df_importacion_21.xlsx")
        



        self.dataframe_importacion = df_importacion.copy()
        
        print("***")
        print(f"\n\nBusqueda part numbers. Tiempo de ejecución: {time.time()-inicio} segundos")
        print("\n")
        return self.dataframe_importacion
        # except Exception as e:
        #     print(f"Problema presentado en {e} - buscar_part_number")




    def conteo_resultados(self,indice_importacion,resultado_busqueda):
        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()
            indice = df_importacion[indice_importacion].tolist()
            resultado_busqueda = df_importacion[resultado_busqueda].tolist()
            conteo_unico = []
            unicos_importacion = {}
            unicos_importacion['encontrado'], unicos_importacion['no encontrado'] = 0, 0
            for i in range(0,len(indice)):
                if indice[i] not in conteo_unico and resultado_busqueda[i] != "0":
                    unicos_importacion['encontrado'] += 1
                    conteo_unico.append(indice[i])
            for i in range(0,len(indice)):
                if indice[i] not in conteo_unico:
                    if resultado_busqueda[i] == "0":
                        unicos_importacion['no encontrado'] += 1
                        conteo_unico.append(indice[i])
            print("\n ---  conteo de resultados --- \n")
            print(f"Conteo de resultados: tiempo de ejecucion {time.time()-inicio} segundos")
            return unicos_importacion
        except KeyError:
            print("Archivo o campo no encontrado")   
        except Exception as e:
            print(f"Problema presentado en {e} - conteo_resultados")


    def conteo_caracteres_alfanumerico(self,alfanumerico_sistm,alfanumerico_ia,largo_alfanumerico_sistemas,
                                      largo_alfanumerico_ia,campo_ajuste_resultado_sist,eliminar_campos,
                                      resultado_busqueda_sist,resultado_busqueda_sist_modl):
        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()

            df_importacion.loc[:,largo_alfanumerico_sistemas] = df_importacion[alfanumerico_sistm].apply(
                lambda x: len(x) if x is not None else 0
            )

            df_importacion.loc[:,largo_alfanumerico_ia] = df_importacion[alfanumerico_ia].apply(
                lambda x: len(x) if x is not None else 0
            )
            df_importacion.loc[:,campo_ajuste_resultado_sist] = df_importacion.apply(
                lambda x: str(0) 
                if x[largo_alfanumerico_sistemas] < 4 and (x[resultado_busqueda_sist] != 'no encontrado' or x[resultado_busqueda_sist_modl] != 'no encontrado') 
                else x[resultado_busqueda_sist_modl], axis=1
            )

            df_importacion = df_importacion.drop(
                eliminar_campos, axis=1
            )
            self.dataframe_importacion = df_importacion.copy()
            print(f"Conteo de caracteres alfanumericos: tiempo de ejecucion {time.time()-inicio} segundos")
            return self.dataframe_importacion
        except KeyError:
            print("Archivo o campo no encontrado")   
        except Exception as e:
            print(f"Se presento un problema en {e} - conteo_caracteres_alfanumerico")


    def porcentajes_coicidencia(self,indice,tam_value,tam_SA_value,resultado_busqueda):
        try:
            inicio = time.time()
            def contar_elementos_lista(lista):

                    if isinstance(lista, list):
                        contador = 0
                        nro_elementos = 0
                        for elemento in lista:
                            nro_elementos += 1
                            for letra in elemento:
                                contador += 1
                        if nro_elementos > 0:
                            return contador / nro_elementos
                        else:
                            return 0
                    elif isinstance(lista, str) and lista != '0':
                        return len(lista)
                    else:
                        return 0

            # try:
            df_importacion = self.dataframe_importacion.copy()

            df_importacion['size_resultado'] = df_importacion[resultado_busqueda].apply(
                lambda x: contar_elementos_lista(x) if x is not None else 0
            )

            df_importacion['porcentaje_coincidencia'] = df_importacion.apply(
                lambda x: x[tam_value]/x['size_resultado']  if (x[tam_value] <= x['size_resultado'] and x['size_resultado'] != 1) and x['size_resultado'] !=0 else 0, axis=1
            )
        
            df_importacion['size_resultado_IA'] = df_importacion['resultado_busqueda_AI_model'].apply(
                lambda x: contar_elementos_lista(x) if x is not None else 0
            )            
            df_importacion['porcentaje_coincidencia_IA'] = df_importacion.apply(
                lambda x: x[tam_SA_value]/x['size_resultado_IA']  if (x[tam_SA_value] <= x['size_resultado_IA'] and x['size_resultado_IA'] != 1) and  x['size_resultado']!=0 else 0, axis=1
            )
            print(df_importacion)
            df_importacion = df_importacion.sort_values(
                by=[indice,'porcentaje_coincidencia', 'size_resultado', tam_value], ascending=[True, False, False, False]
            )
            print(df_importacion['resultado_busqueda_1'])
            self.dataframe_importacion = df_importacion.copy()

            print(f"Porcentajes de coincidencia: tiempo de ejecucion {time.time()-inicio} segundos")
            return self.dataframe_importacion
            # except ZeroDivisionError as e:
            #     print(f"Se presento un problema en {e} - porcentajes_coicidencia")
        except KeyError:
            print("Archivo o campo no encontrado")  
        except Exception as e:
            print(f"Se presento un problema en {e} - porcentajes_coicidencia") 

    def procesar_data_final(self,seleccion_col_datafinal,seleccion_col_datafinal_ia,seleccion_col_datafinal_log,
                           eliminar_filas_vacias,columna_duplicada,merge_importacion_log,merge_importacion_ia,
                           ordenar_campos,campos_renombrados):

        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()
            df_importacion.to_excel("data_final.xlsx")
            importacion_total = df_importacion[seleccion_col_datafinal]
            importacion_res_ai = df_importacion[seleccion_col_datafinal_ia]
            importacion_res_log = df_importacion[seleccion_col_datafinal_log]

            # importacion_res_ai = importacion_res_ai.sort_values(by = 'resultado_busqueda_AI_model'\
            #                                                     , ascending=False)
            importacion_res_log = importacion_res_log.replace(
                '0',pd.NA).dropna(
                how='any',axis=0, subset=[eliminar_filas_vacias]
            )
            importacion_res_log = importacion_res_log.drop_duplicates(
                subset=columna_duplicada, keep='first'
            )
            importacion_res_ai = importacion_res_ai.drop_duplicates(
                subset=columna_duplicada, keep='first'
            )
            importacion_total = importacion_total.drop_duplicates(
                subset=columna_duplicada, keep='first'
            )
            importacion_total = importacion_total.merge(
                importacion_res_log[merge_importacion_log], how='left',on=columna_duplicada
            )
            importacion_total = importacion_total.merge(
                importacion_res_ai[merge_importacion_ia], how='left',on=columna_duplicada
            )
            importacion_total = importacion_total.fillna(
                "0")
            importacion_total = importacion_total.rename(
                columns=campos_renombrados
            )
            importacion_total = importacion_total[ordenar_campos]
            importacion_total['Segm_prob_sistemas_GD'] = pd.to_numeric(
                importacion_total['Probalidad_sistemas_GD']*100
            ).round(-1)
            importacion_total['Segm_prob_IA'] = pd.to_numeric(
                importacion_total['Probalidad_IA']*100
            ).round(-1)
            importacion_total.drop(
                ['Probalidad_sistemas_GD','Probalidad_IA'],axis=1
            )
            # En lugar de usar style.background_gradient
            writer = pd.ExcelWriter(
                f"part_number_{importacion_total['DS_IMPORTADOR'][0]}.xlsx",
                engine='openpyxl'
            )
            importacion_total.to_excel(writer, index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            # Aplicar formato básico
            for col in ['Probalidad_sistemas_GD', 'Probalidad_IA', 'Segm_prob_sistemas_GD', 'Segm_prob_IA']:
                col_idx = importacion_total.columns.get_loc(col)
                for row in range(len(importacion_total)):
                    cell = worksheet.cell(row=row+2, column=col_idx+1)
                    cell.number_format = '0.00%'
            
            writer.save()
            self.dataframe_importacion = importacion_total.copy()
            print("Data procesada!!!\n")
            print(f"Procesar data final: tiempo de ejecucion {time.time()-inicio} segundos")
            # print(self.dataframe_importacion['resultado_busqueda_1'])
            return self.dataframe_importacion
        except Exception as e:
            print(f"Se presento un problema con {e} - procesar_data_final")



    



