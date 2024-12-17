def buscar_part_number(self,campo_data_deltron,campo_iterable,nueva_columna,campo_descripcion,valores_nulos=None):
        """ 
        """
        from fuzzywuzzy import fuzz, process
        import pandas as pd
        import re
        import time
        # try:
        inicio = time.time()
        backup_data_importacion = self.dataframe_importacion.copy()
        self.dataframe_importacion.drop_duplicates(subset=['DS_DESC_COM'], keep='first', inplace=True)
        df_importacion = self.dataframe_importacion.copy()
        df_deltron = self.dataframe_deltron.copy()
        df_modelo = self.dataframe_modelo.copy()
        df_modelo = df_modelo[df_modelo['PART_NUMBER'].str.len() > 5]
        df_deltron = df_deltron[df_deltron['PART_NUMBER'].str.len() > 5]
        df_importacion['DS_DESC_COM'] = df_importacion['DS_DESC_COM'].str.upper().str.strip()
        df_deltron['PART_NUMBER'] = df_deltron['PART_NUMBER'].str.upper().str.strip()
        df_modelo['PART_NUMBER'] = df_modelo['PART_NUMBER'].str.upper().str.strip()
        if valores_nulos:
            df_importacion = df_importacion.fillna(valores_nulos)


        def busqueda_por_igualdad(campo_descripcion, conjunto_part_numbers, conjunto_pnumber_modelo,partnumber_found_desc):

            if partnumber_found_desc is None:
                partnumber_found_desc = []

            

            lista_descripcion = sorted(re.findall(r'\b[\w./#()-]+\b',campo_descripcion), key=len, reverse=True)
            # print(lista_descripcion)
            palabras_filtradas = [
                palabra for palabra in lista_descripcion 
                    if len(palabra) > 4 and (
                        ( any(c.isalpha() for c in palabra) and any(c.isdigit() for c in palabra) ) or
                        (palabra.replace('.', '').isdigit() and '.' in palabra) or
                        (palabra.replace('-', '').isdigit() and '-' in palabra) 
                    )
                ]
            

            # print(palabras_filtradas)
            lista_modelo, lista_parnum_mod = map(list, zip(*conjunto_pnumber_modelo))

            if campo_descripcion.startswith("COMPUTADORA, ASUS"):
                for palabra in palabras_filtradas:
                    if len(palabra) > 5 and palabra in lista_modelo:
                        # print(f"palabra: {palabra}")
                        indice = lista_modelo.index(palabra)
                        indice_partnumber = lista_parnum_mod[indice]
                        # print(f"indice: {indice}")
                        # print(f"indice_partnumber: {indice_partnumber}")
                        partnumber_found_desc.append(indice_partnumber)
                        return "@modelo-" + indice_partnumber



            if campo_descripcion[:13] == "TARJETA MADRE" or campo_descripcion[:15] == "TARJETA GRAFICA" \
                or campo_descripcion.startswith("MOUSE., TEROS"):
                # print(f"campo_descripcion: {campo_descripcion[:13]}")
                for partnumber in conjunto_part_numbers:
                    if partnumber in campo_descripcion:
                        # print(f"partnumber_mouse: {'TE-5076N'}")
                        partnumber_found_desc.append(partnumber)
                        return "@tarjeta-" + partnumber



            if len(palabras_filtradas) > 0:
                for palabra in palabras_filtradas:
                    if palabra in conjunto_part_numbers:
                        partnumber_found_desc.append(palabra)
                        return palabra


    
            saldo_part_num = [partnumber for partnumber in conjunto_part_numbers if partnumber not in partnumber_found_desc]

            matches = {}

            for partnumb_del in saldo_part_num:
                if campo_descripcion.startswith("MOUSE., TEROS"):
                    similarity = process.extract(partnumb_del, palabras_filtradas, scorer=fuzz.token_sort_ratio, limit=1)
                    # print(f"similarity: {similarity}")
                    if similarity[0][1] > 75:
                        # print(f"{partnumb_del} - {similarity[0][0]} - Similaridad: {similarity[0][1]}")
                        matches[partnumb_del] = similarity[0][1]
                    else:
                        None
                else:
                    # print(f"campo_descripcion_otro_producto: {campo_descripcion}")
                    similarity = process.extract(partnumb_del, palabras_filtradas, scorer=fuzz.token_sort_ratio, limit=1)
                    # print(similarity)
                    if not similarity :
                        None
                    elif similarity[0][1] > 75:
                        matches[partnumb_del] = similarity[0][1]

                if len(matches) == 0:
                    return None
                else:
                    return "@similitud" + max(matches, key=matches.get)