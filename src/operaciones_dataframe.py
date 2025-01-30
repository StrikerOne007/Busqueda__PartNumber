from src.conexionbd import DAO

import operator
import time
import pandas as pd
import re
from rapidfuzz import fuzz
import numpy as np
import sys

sys.path.append(
    "C:\\Users\\gustavo.grillo\\1.ANALISIS_DATA\\SERVIDOR\\busqueda_partnumber\\src"
)
# import fuzzrapid
# import pdb
import logging

logging.basicConfig(level=logging.DEBUG)
import functools
import datetime
from tqdm import tqdm


class OperacionesDataframe:
    def __init__(
        self,
        dataframe_importacion=None,
        dataframe_deltron=None,
        dataframe_modelo=None,
        data_final=None,
        data_final_sist=None,
        data_final_ia=None,
    ):
        self.conexion_dao = DAO()
        self.dataframe_importacion = dataframe_importacion
        self.dataframe_deltron = dataframe_deltron
        self.dataframe_modelo = dataframe_modelo
        self.nombre_partnumber = None
        self.LISTA_PARTNUMBERS_NO_VALIDOS = [
            "AORUS H5",
            "AORUS M5",
            "500GB",
            "BATERIA2",
            "SCREEN 2",
            "ROCKET M5",
            "INTEL 651",
            "ADVANCE28",
            "ROCKETM2",
            "ZZZ001",
            "LENOVO 110",
            "I7-10700",
            "P6POWERSUPPLY",
            "RYZEN5",
            "SEAGATE",
            "MON-32",
            "MON-27",
            "MON-215",
            "RYZEN7",
            "RYZEN9",
            "RYZEN3",
            "RYZEN1",
        ]
        self.DICCIONARIO_TIPO_PRODUCTO_MARCA = {
            "DISCO": ["WD", "WESTERN", "HELLETT", "HP", "SEAGATE"],
            "PROCESADOR": ["INTEL", "AMD"],
            "PANTALLA": ["HCK"],
            "MOTHERBOARD": ["ASUS"],
            "ACCESORIO": ["ASUS"],
            "CARGADO": ["ASUS"],
            "TARJETA GRAFICA": ["GIGABYTE"],
            "OTROS": [
                "SANDISK",
                "APC",
                "LANDBYTE",
                "ADVANCE",
                "SAMSUNG",
                "EPSON",
                "LOGITECH",
                "TEROS",
                "KINGSTON",
            ],
        }

        # Patrones de expresiones regulares simplificados y seguros
        self.PATRON_LIMPIEZA = re.compile(r"[^a-zA-Z0-9]")
        self.PATRON_PALABRAS = re.compile(r"\b[\w./#()-]+\b")

        # Patrón de Lenovo simplificado y corregido
        self.PATRON_LIMPIEZA_LENOVO = re.compile(
            r"(?:ideapad|IDEAPAD)"  # Palabra clave
            r"(?:\s+(?:gaming|slim|flex))?"  # Palabras opcionales
            r"(?:\s+\d+)?"  # Números opcionales
            r"\s*(\w+)",  # Captura la palabra final
            re.IGNORECASE,
        )

    def buscar_part_number(self, nueva_columna, valores_nulos=None):
        """Metodo que realiza la busqueda por igualdad de partnumber, hashtag, palabras filtradas y producto"""

        # Log de busqueda de partnumber
        logging.basicConfig(
            filename="busqueda_partnumber.log",
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
        )

        # Iniciar tiempo total
        tiempo_inicio_total = time.time()
        total_registros = len(self.dataframe_importacion)
        registros_procesados = 0

        inicio = time.time()

        # ----------------------------   COPIAS DE LOS DATAFRAMES     ------------------------

        df_importacion = self.dataframe_importacion.copy()
        df_deltron = self.dataframe_deltron.copy()
        df_modelo = self.dataframe_modelo.copy()

        if valores_nulos:
            df_importacion = df_importacion.fillna(valores_nulos)

        # ----------------------------   LIMPIEZA DE LOS DATAFRAMES     ------------------------

        df_modelo = df_modelo[df_modelo["PART_NUMBER"].str.len() > 4]
        df_deltron = df_deltron[df_deltron["PART_NUMBER"].str.len() > 4]
        df_importacion["DS_DESC_COM"] = (
            df_importacion["DS_DESC_COM"].str.upper().str.strip()
        )
        df_deltron["PART_NUMBER"] = df_deltron["PART_NUMBER"].str.upper().str.strip()
        df_deltron["PRODUCTO"] = df_deltron["PRODUCTO"].str.upper().str.strip()
        df_modelo["PART_NUMBER"] = df_modelo["PART_NUMBER"].str.upper().str.strip()

        # ----------------------------   PARTNUMBER SOLO LETRAS Y NO VALIDOS     ------------------------

        lista_completa_partnumber = df_deltron["PART_NUMBER"].values
        partnumber_solo_letras = [
            pn
            for pn in df_deltron["PART_NUMBER"].values
            if not any(c.isdigit() for c in pn)
        ]

        # ----------------------------   QUITAR PARTNUMBER SOLO LETRAS Y NO VALIDOS     ------------------------

        # quitar tambien partnumber_solo_letras y lista_partnumber_no_validos
        df_deltron = df_deltron[~df_deltron["PART_NUMBER"].isin(partnumber_solo_letras)]
        df_deltron = df_deltron[
            ~df_deltron["PART_NUMBER"].isin(self.LISTA_PARTNUMBERS_NO_VALIDOS)
        ]

        conjunto_part_numbers_hashtag = [
            pn[: pn.find("#")] for pn in df_deltron["PART_NUMBER"].values if "#" in pn
        ]
        self.nombre_partnumber = df_deltron["NOMBRE"].values

        # -------- PATRONES DE LIMPIEZA PARA CRUCE DE DESCRIPCION Y PARTNUMBER

        # Patrón para  campo_descripcion_sin_espacios_sin_caracteres_especiales
        PATRON_LIMPIEZA_DESCRIPCION = re.compile(r"[^a-zA-Z0-9]")
        # Patrón para campo_partnumber_sin_espacios_sin_caracteres_especiales
        PATRON_LIMPIEZA_PARTNUMBER = re.compile(r"[^a-zA-Z0-9]")

        def limpiar_descripcion(descripcion):
            return PATRON_LIMPIEZA_DESCRIPCION.sub("", descripcion).upper()

        def limpiar_partnumber(partnumber):
            return PATRON_LIMPIEZA_PARTNUMBER.sub("", partnumber).upper()

        conjunto_part_numbers = df_deltron["PART_NUMBER"].values
        conjunto_part_numbers = np.array(
            sorted(conjunto_part_numbers, key=len, reverse=True)
        )

        conjunto_pnumber_modelo = df_modelo[
            ["MODELO", "PART_NUMBER", "PRODUCTO"]
        ].values
        conjunto_pnumber_modelo = np.array(
            sorted(conjunto_pnumber_modelo, key=operator.itemgetter(1), reverse=True)
        )

        set_conjunto_part_numbers = set(conjunto_part_numbers)
        set_lista_partnumber_no_validos = set(self.LISTA_PARTNUMBERS_NO_VALIDOS)

        def filtrar_partnumbers(set_conjunto_part_numbers):
            """Filtra partnumbers según reglas de negocio"""
            partnumbers_filtrados = {
                pn
                for pn in set_conjunto_part_numbers
                if not pn.isdigit() or (pn.isdigit() and len(pn) > 5)
            }
            return partnumbers_filtrados - set_lista_partnumber_no_validos

        # Filtrar partnumbers según reglas de negocio
        set_conjunto_part_numbers_total_validos = filtrar_partnumbers(
            set_conjunto_part_numbers
        )

        lista_modelo, lista_parnum_mod, lista_codproducto = map(
            list, zip(*conjunto_pnumber_modelo)
        )

        # PROBAR LA FUNCION QUE REEMPLAZA A IGUALDAD_PARTNUMBER
        partnumber_limpio_igualdad_partnumber = {
            partnumber.strip().upper(): partnumber
            for partnumber in set_conjunto_part_numbers_total_validos
            if len(partnumber) > 6
        }

        # print(set_conjunto_part_numbers_total_validos   )

        # PROBAR LAS 3 NUEVAS FUNCIONES QUE REEMPLAZAN A LA FUNCION GENERAL partnumber_sin_espacios_sin_caracteres_especiales

        partnumber_sin_espacios_sin_caracteres_especiales_1 = {
            limpiar_partnumber(partnumber)
            for partnumber in set_conjunto_part_numbers_total_validos
            if len(partnumber) > 6
        }

        partnumber_sin_espacios_sin_caracteres_especiales_2 = {
            partnumber[:-3]: partnumber  # clave: valor (sin 'LHR' : con 'LHR')
            for partnumber in partnumber_sin_espacios_sin_caracteres_especiales_1
            if partnumber[-3:] == "LHR"
        }

        partnumber_sin_espacios_sin_caracteres_especiales_3 = {
            partnumber.replace("GP", ""): partnumber
            for partnumber in partnumber_sin_espacios_sin_caracteres_especiales_1
            if partnumber.find("GP") and "RTX" in partnumber
        }

        # ----------------------------   BUSQUEDA POR IGUALDAD     ------------------------

        # Crear barra de progreso
        with tqdm(total=total_registros, desc="Procesando registros") as pbar:

            def actualizar_progreso():
                nonlocal registros_procesados
                registros_procesados += 1
                if registros_procesados % 100 == 0:  # Loguear cada 100 registros
                    tiempo_actual = time.time()
                    tiempo_transcurrido = tiempo_actual - tiempo_inicio_total
                    velocidad = registros_procesados / tiempo_transcurrido
                    tiempo_estimado = (
                        total_registros - registros_procesados
                    ) / velocidad

                    logging.info(
                        f"Progreso: {registros_procesados}/{total_registros} "
                        f"({(registros_procesados / total_registros) * 100:.2f}%) - "
                        f"Velocidad: {velocidad:.2f} reg/s - "
                        f"Tiempo estimado restante: {datetime.timedelta(seconds=int(tiempo_estimado))}"
                    )
                pbar.update(1)

            # @profile
            def busqueda_por_igualdad(campo_descripcion):
                campo_descripcion = campo_descripcion.upper()

                # Usar el patrón compilado
                lista_descripcion = sorted(
                    self.PATRON_PALABRAS.findall(campo_descripcion),
                    key=len,
                    reverse=True,
                )

                palabras_filtradas = [
                    palabra
                    for palabra in lista_descripcion
                    if len(palabra) > 4
                    and palabra[:5] != "RYZEN"
                    and (
                        (
                            any(c.isalpha() for c in palabra)
                            and any(c.isdigit() for c in palabra)
                        )
                        or (palabra.replace(".", "").isdigit() and "." in palabra)
                        or (palabra.replace("-", "").isdigit() and "-" in palabra)
                        or (palabra.isdigit())
                    )
                ]

                nonlocal \
                    set_conjunto_part_numbers_total_validos, \
                    conjunto_part_numbers_hashtag, \
                    lista_modelo, \
                    lista_parnum_mod, \
                    lista_codproducto, \
                    partnumber_limpio_igualdad_partnumber
                nonlocal partnumber_sin_espacios_sin_caracteres_especiales_1, partnumber_sin_espacios_sin_caracteres_especiales_2, partnumber_sin_espacios_sin_caracteres_especiales_3
                # ----------------------------   BUSQUEDA POR IGUALDAD DE PARTNUMBER     ------------------------

                campo_descripcion_sin_espacios_sin_caracteres_especiales = (
                    limpiar_descripcion(campo_descripcion)
                )

                def igualdad_palabras_filtradas_partnumber(palabras_filtradas):
                    if len(palabras_filtradas) > 0:
                        for palabra in palabras_filtradas:
                            if palabra in set_conjunto_part_numbers_total_validos:
                                return "@IGUALDAD_PARTNUMBER-" + palabra

                def igualdad_hashtag_palabras_filtradas_partnumber(palabras_filtradas):
                    if len(palabras_filtradas) > 0:
                        for palabra in palabras_filtradas:
                            if palabra in conjunto_part_numbers_hashtag:
                                return "@IGUALDAD_HASHTAG-" + palabra

                def igualdad_partnumber():
                    for key, valor in partnumber_limpio_igualdad_partnumber.items():
                        # print(partnumber)
                        if key in campo_descripcion:
                            return "@IGUALDAD_PARTNUMBER-" + valor

                # def igualdad_partnumber_sin_espacios_sin_caracteres_especiales():
                #     for partnum in set_conjunto_part_numbers_total_validos:

                #         partnum_limpio = limpiar_partnumber(partnum)
                #         # casos para tarjeta grafica (video)
                #         if len(partnum_limpio) >= 5:
                #             if partnum_limpio in campo_descripcion_sin_espacios_sin_caracteres_especiales:
                #                 return 'IGUALDAD_PARTNUMBER_SIN_LIMPIO-' + partnum
                #             elif partnum_limpio[-3:] == 'LHR' and partnum_limpio[:-3] in campo_descripcion_sin_espacios_sin_caracteres_especiales:
                #                 return 'IGUALDAD_PARTNUMBER-SIN_LHR-' + partnum
                #             elif partnum_limpio.find('GP') and 'RTX' in partnum_limpio and partnum_limpio.replace('GP', '') in campo_descripcion_sin_espacios_sin_caracteres_especiales:
                #                 return 'IGUALDAD_PARTNUMBER_SIN_GP-' + partnum

                def igualdad_partnumber_sin_espacios_sin_caracteres_1():
                    for (
                        partnumber
                    ) in partnumber_sin_espacios_sin_caracteres_especiales_1:
                        if (
                            partnumber
                            in campo_descripcion_sin_espacios_sin_caracteres_especiales
                        ):
                            return "@IGUALDAD_PARTNUMBER_SIN_LIMPIO-" + partnumber

                def igualdad_partnumber_sin_espacios_sin_caracteres_2():
                    for (
                        key,
                        valor,
                    ) in partnumber_sin_espacios_sin_caracteres_especiales_2.items():
                        if (
                            key
                            in campo_descripcion_sin_espacios_sin_caracteres_especiales
                        ):
                            return "@IGUALDAD_PARTNUMBER-" + valor

                def igualdad_partnumber_sin_espacios_sin_caracteres_3():
                    for (
                        key,
                        valor,
                    ) in partnumber_sin_espacios_sin_caracteres_especiales_3.items():
                        if (
                            key
                            in campo_descripcion_sin_espacios_sin_caracteres_especiales
                        ):
                            return "@IGUALDAD_PARTNUMBER_SIN_GP-" + valor

                resultado = igualdad_palabras_filtradas_partnumber(palabras_filtradas)
                if resultado:
                    return resultado

                resultado = igualdad_hashtag_palabras_filtradas_partnumber(
                    palabras_filtradas
                )
                if resultado:
                    return resultado

                resultado = igualdad_partnumber()
                if resultado:
                    return resultado

                # NO USAR ESTA FUNCION
                # resultado = igualdad_partnumber_sin_espacios_sin_caracteres_especiales()
                # if resultado:
                #     return resultado

                resultado = igualdad_partnumber_sin_espacios_sin_caracteres_1()
                if resultado:
                    return resultado

                resultado = igualdad_partnumber_sin_espacios_sin_caracteres_2()
                if resultado:
                    return resultado

                resultado = igualdad_partnumber_sin_espacios_sin_caracteres_3()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR IGUADAL EMPLEANDO EL NUMERO DE PRODUCTO     ------------------------

                def parametro_busqueda_por_producto(tipo_producto, marca):
                    if marca in campo_descripcion and (
                        tipo_producto in campo_descripcion or tipo_producto == "OTROS"
                    ):
                        for id_producto in df_deltron["PRODUCTO"].values:
                            if id_producto in campo_descripcion:
                                partnumber = df_deltron["PART_NUMBER"][
                                    df_deltron["PRODUCTO"] == id_producto
                                ].values[0]
                                #    print(f"partnumber: {partnumber}")
                                return f"@ADVANCE_OTROS_-{partnumber}"  # Retornamos inmediatamente después de encontrar el partnumber

                for (
                    tipos_producto,
                    marcas,
                ) in self.DICCIONARIO_TIPO_PRODUCTO_MARCA.items():
                    for marca in marcas:
                        resultado = parametro_busqueda_por_producto(
                            tipos_producto, marca
                        )
                        if resultado:
                            return resultado

                def busqueda_por_igualdad_por_modelo(lista_modelo, lista_codproducto):
                    for palabra in palabras_filtradas:
                        if len(palabra) > 5 and palabra in lista_modelo:
                            indice = lista_modelo.index(palabra)
                            indice_partnumber = lista_parnum_mod[indice]
                            return "@modelo-" + indice_partnumber

                        if len(palabra) > 5 and palabra in lista_codproducto:
                            indice = lista_codproducto.index(palabra)
                            indice_partnumber = lista_parnum_mod[indice]
                            return "@codprodc-" + indice_partnumber

                resultado = busqueda_por_igualdad_por_modelo(
                    lista_modelo, lista_codproducto
                )
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICACION DE MODELO     ------------------------

                def busqueda_igualdad_por_especifica_asus(lista_modelo):
                    if (
                        "COMPUTADORA" in campo_descripcion
                        and "ASUS" in campo_descripcion
                    ):
                        for modelo in lista_modelo:
                            # Eliminar todo después del / (incluyendo espacios opcionales antes)
                            modelo_sin_barra_inversa = modelo.find("/")
                            if modelo_sin_barra_inversa != -1:
                                modelo_preview = modelo[
                                    :modelo_sin_barra_inversa
                                ].strip()
                                if modelo_preview in campo_descripcion:
                                    partnumber = df_modelo["PART_NUMBER"][
                                        df_modelo["MODELO"].str.contains(
                                            f"{modelo_preview}"
                                        )
                                    ].values[0]
                                    return "@modelo_preview-" + partnumber

                    elif "ASUS" in campo_descripcion:
                        for partnumber in conjunto_part_numbers_hashtag:
                            for palabra in palabras_filtradas:
                                if palabra in partnumber:
                                    return "@partnumber_inicia_ASUS-" + partnumber

                def busqueda_igualdad_por_especifica_lenovo(lista_modelo):
                    if (
                        "COMPUTADORA" in campo_descripcion
                        and "LENOVO" in campo_descripcion
                    ):
                        for modelo in lista_modelo:
                            try:
                                match = self.PATRON_LIMPIEZA_LENOVO.search(modelo)
                                if match:
                                    modelo_limpio = match.group(1)
                                    if modelo_limpio and len(modelo_limpio) > 3:
                                        if modelo_limpio in campo_descripcion:
                                            indice_modelo = lista_modelo.index(modelo)
                                            indice_partnumber = lista_parnum_mod[
                                                indice_modelo
                                            ]
                                            return "@modelo2-" + indice_partnumber
                            except (re.error, IndexError) as e:
                                logging.debug(
                                    f"Error procesando modelo Lenovo: {modelo}, Error: {str(e)}"
                                )
                                continue

                        # Si no se encontró por modelo, buscar por palabras filtradas
                        for palabra in palabras_filtradas:
                            palabra = palabra.replace("-", "")
                            if palabra in set_conjunto_part_numbers_total_validos:
                                return "@partnumber_inicia_LENOVO-" + palabra

                def busqueda_igualdad_por_especifica_hewlett_modelo(lista_modelo):
                    if campo_descripcion.startswith("COMPUTADORA,HEWLETT"):
                        for modelo in lista_modelo:
                            palabras_clave = re.findall(modelo, campo_descripcion)
                            for palabra in palabras_clave:
                                if isinstance(palabra, str):
                                    indice_modelo = lista_modelo.index(modelo)
                                    indice_partnumber = lista_parnum_mod[indice_modelo]
                                    return "@modelo2-" + indice_partnumber

                def busqueda_igualdad_por_especifica_advance():
                    if campo_descripcion.startswith(
                        "COMPUTADORA,ADVANCE"
                    ) or campo_descripcion.startswith("COMPUTADORA, ADVANCE,"):
                        conjunto_part_numbers = np.array(
                            set_conjunto_part_numbers_total_validos
                        )
                        conjunto_part_numbers = conjunto_part_numbers[::-1]
                        for partnumber in conjunto_part_numbers:
                            if partnumber.startswith("ADV-"):
                                if partnumber[:10] in palabras_filtradas:
                                    return "@ADV_inicio-" + partnumber
                                elif partnumber[4:10] in palabras_filtradas:
                                    return "@ADV_inicio-" + partnumber

                # *************************************************************************************
                def eliminar_parentesis(palabra):
                    """Funcion toma un string y retorna el mismo string sin parentesis"""
                    return palabra.replace("(", "").replace(")", "")

                # *************************************************************************************

                def busqueda_igualdad_por_especifica_hewlett_partnumber():
                    if (
                        "HEWLETT" in campo_descripcion
                        or "HP" in campo_descripcion
                        or "BIWIN" in campo_descripcion
                    ):
                        retirar_hashtag_palabras_filtradas = [
                            palabra[: palabra.find("#")]
                            for palabra in palabras_filtradas
                            if "#" in palabra
                        ]
                        # print(retirar_hashtag_palabras_filtradas)
                        if retirar_hashtag_palabras_filtradas:
                            for partnumber in set_conjunto_part_numbers_total_validos:
                                partnumber = eliminar_parentesis(partnumber)
                                if partnumber in retirar_hashtag_palabras_filtradas:
                                    return "@partnumber_inicia_HEWLETT-" + partnumber

                                else:
                                    palabras_clave = re.findall(
                                        retirar_hashtag_palabras_filtradas[0],
                                        partnumber,
                                    )
                                    if palabras_clave:
                                        return (
                                            "@partnumber_inicia_HEWLETT-" + partnumber
                                        )

                def buscar_coincidencia_partnumber(
                    partnumber, texto_busqueda, palabras_buscar=None
                ):
                    """
                    Función auxiliar que maneja el re.escape y la búsqueda de coincidencias
                    partnumber: número de parte a buscar
                    texto_busqueda: texto donde buscar (campo_descripcion o partnumber)
                    palabras_buscar: lista opcional de palabras específicas para buscar
                    """
                    partnumber_escaped = re.escape(partnumber)

                    # Caso Western: busca palabras específicas en el partnumber
                    if palabras_buscar is not None:
                        for palabra in palabras_buscar:
                            if re.findall(palabra, partnumber_escaped):
                                return True
                    # Caso Gigabyte/Tripplite: busca el partnumber en la descripción
                    elif texto_busqueda is not None:
                        coincidencias = re.findall(partnumber_escaped, texto_busqueda)
                        if coincidencias and isinstance(coincidencias[0], str):
                            return True
                    return False

                def busqueda_igualdad_por_especifica_western():
                    if "WESTERN" in campo_descripcion:
                        for partnumber in set_conjunto_part_numbers_total_validos:
                            if partnumber.startswith("WDB"):
                                # Aquí buscamos en el partnumber, no en texto_busqueda
                                if buscar_coincidencia_partnumber(
                                    partnumber,
                                    texto_busqueda=None,
                                    palabras_buscar=palabras_filtradas,
                                ):
                                    return "@partnumber_inicia_WESTERN-" + partnumber

                def busqueda_igualdad_por_especifica_gigabyte():
                    if (
                        "MOTHERBOARD" in campo_descripcion
                        and "GIGABYTE" in campo_descripcion
                    ):
                        for partnumber in set_conjunto_part_numbers_total_validos:
                            # Aquí buscamos en campo_descripcion
                            if buscar_coincidencia_partnumber(
                                partnumber, texto_busqueda=campo_descripcion
                            ):
                                return "@partnumber_inicia_GIGABYTE-" + partnumber

                def busqueda_igualdad_por_especifica_tripplite():
                    if "TRIPPLITE" in campo_descripcion:
                        for partnumber in set_conjunto_part_numbers_total_validos:
                            if buscar_coincidencia_partnumber(
                                partnumber, campo_descripcion
                            ):
                                return "@partnumber_inicia_TRIPPLITE-" + partnumber

                def busqueda_igualdad_por_especifica_teros():
                    if "MOUSE" in campo_descripcion and "TEROS" in campo_descripcion:
                        for partnumber in set_conjunto_part_numbers_total_validos:
                            if partnumber in campo_descripcion:
                                return "@tarjeta-" + partnumber

                    elif "TEROS" in campo_descripcion:
                        for palabra in palabras_filtradas:
                            palabra = palabra.replace("-", "")
                            if palabra in set_conjunto_part_numbers_total_validos:
                                return "@partnumber_inicia_TEROS-" + palabra

                def busqueda_igualdad_por_especifica_tarjeta():
                    if (
                        campo_descripcion[:13] == "TARJETA MADRE"
                        or campo_descripcion[:15] == "TARJETA GRAFICA"
                    ):
                        for partnumber in set_conjunto_part_numbers_total_validos:
                            if partnumber in campo_descripcion:
                                return "@tarjeta-" + partnumber

                #   LLAMADA A LAS FUNCIONES DE BUSQUEDA POR ESPECIFICA

                # ----------------------------   BUSQUEDA POR ESPECIFICA ASUS     ------------------------
                resultado = busqueda_igualdad_por_especifica_asus(lista_modelo)
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA LENOVO     ------------------------

                resultado = busqueda_igualdad_por_especifica_lenovo(lista_modelo)
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA HEWLETT     ------------------------

                resultado = busqueda_igualdad_por_especifica_hewlett_modelo(
                    lista_modelo
                )
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA ADVANCE     ------------------------

                resultado = busqueda_igualdad_por_especifica_advance()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA HEWLETT PARTNUMBER     ------------------------

                resultado = busqueda_igualdad_por_especifica_hewlett_partnumber()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA WESTERN     ------------------------

                resultado = busqueda_igualdad_por_especifica_western()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA GIGABYTE     ------------------------

                resultado = busqueda_igualdad_por_especifica_gigabyte()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA TRIPPLITE     ------------------------

                resultado = busqueda_igualdad_por_especifica_tripplite()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA TEROS     ------------------------

                resultado = busqueda_igualdad_por_especifica_teros()
                if resultado:
                    return resultado

                # ----------------------------   BUSQUEDA POR ESPECIFICA TARJETA     ------------------------

                resultado = busqueda_igualdad_por_especifica_tarjeta()
                if resultado:
                    return resultado

                #  COMENTADO HASTA AQUI

                #  # HACER BUSQUEDA POR SIMILITUD DE PARTNUMBER

                #     saldo_part_num_chunks = sorted(conjunto_part_numbers, key=len, reverse=True)
                #     mask = np.array([len(x) > 6 for x in saldo_part_num_chunks])
                #     saldo_part_num_chunks_lenmay_6 = np.array(saldo_part_num_chunks)[mask]
                #     chunks = np.array_split(saldo_part_num_chunks_lenmay_6, max(1, len(saldo_part_num_chunks_lenmay_6) // 1000))

                #     def funcion_similitud(campo_descripcion, chunks, umbral):
                #         """ Funcion de similitud, recibe campo descripcion y chunks de partnumbers, y umbral de similitud
                #         retorna el partnumber que tiene mayor similitud con el campo descripcion
                #         """
                #         mejor_similitud = 0
                #         mejor_partnumber = None
                #         umbral = umbral

                #         for chunk in chunks:
                #             for partnumb_del in chunk:
                #                 similarity = fuzzrapid.funcion_fuzz(partnumb_del, campo_descripcion)

                #                 if similarity > mejor_similitud and similarity > umbral:
                #                     mejor_similitud = similarity
                #                     mejor_partnumber = partnumb_del

                #         return f"{mejor_partnumber}" if mejor_partnumber else None

                #     if campo_descripcion.startswith("COMPUTADORA, LENOVO") or campo_descripcion.startswith("COMPUTADORA,LENOVO")\
                #         or "HEWLETT PACKARD" in campo_descripcion and not campo_descripcion.startswith("MEMORIA SSD"):
                #         hp_chunks = [pn for pn in saldo_part_num_chunks_lenmay_6 if "#" in pn]
                #         chunks = np.array_split(hp_chunks, max(1, len(hp_chunks) // 1000))

                #         resultado = funcion_similitud(campo_descripcion, chunks, 88)
                #         return f"@simil_88_hewpack-{resultado}" if resultado else None

                #     elif (campo_descripcion.startswith("MEMORIA RAM, KINGSTON") or 'SEAGATE' in campo_descripcion\
                #            or ('TEROS' in campo_descripcion and (not campo_descripcion.startswith("MOUSE") or not campo_descripcion.startswith("TECLADO") or not campo_descripcion.startswith("CASE"))))\
                #                  and 'NO NECESITA PERMISO' not in campo_descripcion:

                #         resultado = funcion_similitud(campo_descripcion, chunks, 90)
                #         return f"@simil_90_adv_te-{resultado}" if resultado else None

                #     elif campo_descripcion.startswith("TARJETA MADRE") and 'GIGABYTE' not in campo_descripcion \
                #         or campo_descripcion.startswith("MOUSE,DELL"):

                #         resultado = funcion_similitud(campo_descripcion, chunks, 95)
                #         return f"@simil_95_te_wes_giga_msi-{resultado}" if resultado else None

                #     elif campo_descripcion.startswith("TARJETA GRAFICA") or 'ADVANCE' in campo_descripcion:
                #         mask = [len(pn) > 10  for pn in saldo_part_num_chunks_lenmay_6]
                #         hp_chunks = np.array(saldo_part_num_chunks_lenmay_6)[mask]
                #         chunks = np.array_split(hp_chunks, max(1, len(hp_chunks) // 1000))

                #         resultado = funcion_similitud(campo_descripcion, chunks, 95)
                #         return f"@simil_95_tarj_gra-{resultado}" if resultado else None

                #     elif 'LANDBYTE' in campo_descripcion or campo_descripcion.startswith("PROCESADOR,INTEL")\
                #             or campo_descripcion.startswith("PROCESADOR,AMD"):    \

                #         resultado = funcion_similitud(campo_descripcion, chunks, 77.5)
                #         return f"@simil_77_5_rx_rtx_gtx_land-{resultado}" if resultado else None

                #     elif 'GIGABYTE' in campo_descripcion.upper():

                #         resultado = funcion_similitud(campo_descripcion, chunks, 93)
                #         return f"@simil_93_gigabyte-{resultado}" if resultado else None

                #     elif campo_descripcion.startswith("DISCO DURO") and 'WESTER' in campo_descripcion:

                #         resultado = funcion_similitud(campo_descripcion, chunks, 85)
                #         return f"@simil_85_disco_wester-{resultado}" if resultado else None

                #     elif ('CHASIS' in campo_descripcion and 'ASUS' in campo_descripcion):

                #         resultado = funcion_similitud(campo_descripcion, chunks, 87)
                #         return f"@simil_87_chasis_asus-{resultado}" if resultado else None

                #     elif 'GTX' in campo_descripcion or 'RTX' in campo_descripcion:
                #         resultado = funcion_similitud(campo_descripcion, chunks, 97)
                #         return f"@simil_97_gtx-{resultado}" if resultado else None

                #     elif 'RX' in campo_descripcion:
                #         resultado = funcion_similitud(campo_descripcion, chunks, 89)
                #         return f"@simil_89_rx-{resultado}" if resultado else None

                #     elif 'ASUS' in campo_descripcion\
                #            and (not campo_descripcion.startswith("TARJETA") or not campo_descripcion.startswith("CHASSIS")\
                #             or not campo_descripcion.startswith("COMPUTADORA")):
                #         resultado = funcion_similitud(campo_descripcion, chunks, 97)
                #         return f"@simil_97_asus-{resultado}" if resultado else None

                actualizar_progreso()

        tiempo_total = time.time() - tiempo_inicio_total
        logging.info(
            f"Proceso completado en {datetime.timedelta(seconds=int(tiempo_total))}"
        )
        print(
            f"\nTiempo total de ejecución: {datetime.timedelta(seconds=int(tiempo_total))}"
        )

        resultados = []
        batch_size = 3000
        for i in range(0, total_registros, batch_size):
            batch = df_importacion.iloc[i : i + batch_size]
            for index, row in batch.iterrows():
                campo_descripcion = row["DS_DESC_COM"]
                resultado = busqueda_por_igualdad(campo_descripcion)
                resultados.append(resultado)  # Agregamos cada resultado a la lista

        # Asignamos toda la lista de resultados a la nueva columna
        df_importacion[nueva_columna] = resultados

        # Guardar resultados
        df_importacion.to_excel("./return_data/df_importacion_29.xlsx")

        self.dataframe_importacion = df_importacion.copy()
        print("***")
        print(
            f"\n\nBusqueda part numbers. Tiempo de ejecución: {time.time() - inicio} segundos"
        )
        print("\n")
        return self.dataframe_importacion
        # except Exception as e:
        #     print(f"Problema presentado en {e} - buscar_part_number")

    def conteo_resultados(self, indice_importacion, resultado_busqueda):
        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()
            indice = df_importacion[indice_importacion].tolist()
            resultado_busqueda = df_importacion[resultado_busqueda].tolist()
            conteo_unico = []
            unicos_importacion = {}
            unicos_importacion["encontrado"], unicos_importacion["no encontrado"] = 0, 0
            for i in range(0, len(indice)):
                if indice[i] not in conteo_unico and resultado_busqueda[i] != "0":
                    unicos_importacion["encontrado"] += 1
                    conteo_unico.append(indice[i])
            for i in range(0, len(indice)):
                if indice[i] not in conteo_unico:
                    if resultado_busqueda[i] == "0":
                        unicos_importacion["no encontrado"] += 1
                        conteo_unico.append(indice[i])
            print("\n ---  conteo de resultados --- \n")
            print(
                f"Conteo de resultados: tiempo de ejecucion {time.time() - inicio} segundos"
            )
            return unicos_importacion
        except KeyError:
            print("Archivo o campo no encontrado")
        except Exception as e:
            print(f"Problema presentado en {e} - conteo_resultados")

    def conteo_caracteres_alfanumerico(
        self,
        alfanumerico_sistm,
        alfanumerico_ia,
        largo_alfanumerico_sistemas,
        largo_alfanumerico_ia,
        campo_ajuste_resultado_sist,
        eliminar_campos,
        resultado_busqueda_sist,
        resultado_busqueda_sist_modl,
    ):
        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()

            df_importacion.loc[:, largo_alfanumerico_sistemas] = df_importacion[
                alfanumerico_sistm
            ].apply(lambda x: len(x) if x is not None else 0)

            df_importacion.loc[:, largo_alfanumerico_ia] = df_importacion[
                alfanumerico_ia
            ].apply(lambda x: len(x) if x is not None else 0)
            df_importacion.loc[:, campo_ajuste_resultado_sist] = df_importacion.apply(
                lambda x: str(0)
                if x[largo_alfanumerico_sistemas] < 4
                and (
                    x[resultado_busqueda_sist] != "no encontrado"
                    or x[resultado_busqueda_sist_modl] != "no encontrado"
                )
                else x[resultado_busqueda_sist_modl],
                axis=1,
            )

            df_importacion = df_importacion.drop(eliminar_campos, axis=1)
            self.dataframe_importacion = df_importacion.copy()
            print(
                f"Conteo de caracteres alfanumericos: tiempo de ejecucion {time.time() - inicio} segundos"
            )
            return self.dataframe_importacion
        except KeyError:
            print("Archivo o campo no encontrado")
        except Exception as e:
            print(f"Se presento un problema en {e} - conteo_caracteres_alfanumerico")

    def porcentajes_coicidencia(
        self, indice, tam_value, tam_SA_value, resultado_busqueda
    ):
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
                elif isinstance(lista, str) and lista != "0":
                    return len(lista)
                else:
                    return 0

            # try:
            df_importacion = self.dataframe_importacion.copy()

            df_importacion["size_resultado"] = df_importacion[resultado_busqueda].apply(
                lambda x: contar_elementos_lista(x) if x is not None else 0
            )

            df_importacion["porcentaje_coincidencia"] = df_importacion.apply(
                lambda x: x[tam_value] / x["size_resultado"]
                if (x[tam_value] <= x["size_resultado"] and x["size_resultado"] != 1)
                and x["size_resultado"] != 0
                else 0,
                axis=1,
            )

            df_importacion["size_resultado_IA"] = df_importacion[
                "resultado_busqueda_AI_model"
            ].apply(lambda x: contar_elementos_lista(x) if x is not None else 0)
            df_importacion["porcentaje_coincidencia_IA"] = df_importacion.apply(
                lambda x: x[tam_SA_value] / x["size_resultado_IA"]
                if (
                    x[tam_SA_value] <= x["size_resultado_IA"]
                    and x["size_resultado_IA"] != 1
                )
                and x["size_resultado"] != 0
                else 0,
                axis=1,
            )
            print(df_importacion)
            df_importacion = df_importacion.sort_values(
                by=[indice, "porcentaje_coincidencia", "size_resultado", tam_value],
                ascending=[True, False, False, False],
            )
            print(df_importacion["resultado_busqueda_1"])
            self.dataframe_importacion = df_importacion.copy()

            print(
                f"Porcentajes de coincidencia: tiempo de ejecucion {time.time() - inicio} segundos"
            )
            return self.dataframe_importacion
            # except ZeroDivisionError as e:
            #     print(f"Se presento un problema en {e} - porcentajes_coicidencia")
        except KeyError:
            print("Archivo o campo no encontrado")
        except Exception as e:
            print(f"Se presento un problema en {e} - porcentajes_coicidencia")

    def procesar_data_final(
        self,
        seleccion_col_datafinal,
        seleccion_col_datafinal_ia,
        seleccion_col_datafinal_log,
        eliminar_filas_vacias,
        columna_duplicada,
        merge_importacion_log,
        merge_importacion_ia,
        ordenar_campos,
        campos_renombrados,
    ):
        try:
            inicio = time.time()
            df_importacion = self.dataframe_importacion.copy()
            df_importacion.to_excel("data_final.xlsx")
            importacion_total = df_importacion[seleccion_col_datafinal]
            importacion_res_ai = df_importacion[seleccion_col_datafinal_ia]
            importacion_res_log = df_importacion[seleccion_col_datafinal_log]

            # importacion_res_ai = importacion_res_ai.sort_values(by = 'resultado_busqueda_AI_model'\
            #                                                     , ascending=False)
            importacion_res_log = importacion_res_log.replace("0", pd.NA).dropna(
                how="any", axis=0, subset=[eliminar_filas_vacias]
            )
            importacion_res_log = importacion_res_log.drop_duplicates(
                subset=columna_duplicada, keep="first"
            )
            importacion_res_ai = importacion_res_ai.drop_duplicates(
                subset=columna_duplicada, keep="first"
            )
            importacion_total = importacion_total.drop_duplicates(
                subset=columna_duplicada, keep="first"
            )
            importacion_total = importacion_total.merge(
                importacion_res_log[merge_importacion_log],
                how="left",
                on=columna_duplicada,
            )
            importacion_total = importacion_total.merge(
                importacion_res_ai[merge_importacion_ia],
                how="left",
                on=columna_duplicada,
            )
            importacion_total = importacion_total.fillna("0")
            importacion_total = importacion_total.rename(columns=campos_renombrados)
            importacion_total = importacion_total[ordenar_campos]
            importacion_total["Segm_prob_sistemas_GD"] = pd.to_numeric(
                importacion_total["Probalidad_sistemas_GD"] * 100
            ).round(-1)
            importacion_total["Segm_prob_IA"] = pd.to_numeric(
                importacion_total["Probalidad_IA"] * 100
            ).round(-1)
            importacion_total.drop(["Probalidad_sistemas_GD", "Probalidad_IA"], axis=1)
            # En lugar de usar style.background_gradient
            writer = pd.ExcelWriter(
                f"part_number_{importacion_total['DS_IMPORTADOR'][0]}.xlsx",
                engine="openpyxl",
            )
            importacion_total.to_excel(writer, index=False)
            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]

            # Aplicar formato básico
            for col in [
                "Probalidad_sistemas_GD",
                "Probalidad_IA",
                "Segm_prob_sistemas_GD",
                "Segm_prob_IA",
            ]:
                col_idx = importacion_total.columns.get_loc(col)
                for row in range(len(importacion_total)):
                    cell = worksheet.cell(row=row + 2, column=col_idx + 1)
                    cell.number_format = "0.00%"

            writer.save()
            self.dataframe_importacion = importacion_total.copy()
            print("Data procesada!!!\n")
            print(
                f"Procesar data final: tiempo de ejecucion {time.time() - inicio} segundos"
            )
            # print(self.dataframe_importacion['resultado_busqueda_1'])
            return self.dataframe_importacion
        except Exception as e:
            print(f"Se presento un problema con {e} - procesar_data_final")
