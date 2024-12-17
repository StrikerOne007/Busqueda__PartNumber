import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv 
load_dotenv()
import os
import pandas as pd
import time


class Conexion_BD:
    def __init__(self):
        try:
            inicio = time.time()
            self.conexion = mysql.connector.connect(
                host= "192.168.66.76",
                port= "3306",
                user= "gtrillo",
                password = os.getenv("DB_PASSWORD"),
            )
            print(f"Conexión exitosa a la base de datos en {time.time()-inicio} segundos")
        except Error as e:
            print(f"Se presento un problema en {e}")

class DAO:
    def __init__(self):
        self.db = Conexion_BD()

    def listar_data(self,consulta):

        if self.db.conexion.is_connected():
            try:
                data = self.db.conexion.cursor()
                data.execute(consulta)
                resultados = data.fetchall()
                return resultados
            except Error as ex:
                print(f"Se obtuvo el siguiente error la listar datos en base de datos: {ex}")
                return None


    def carga_data_sql(self,consulta,columns):
        try:
            inicio = time.time()
            data = self.listar_data(consulta)
            resultado = pd.DataFrame(data, columns=columns)
            print(f"Se obtuvieron {resultado.shape[0]} datos en {time.time()-inicio} segundos")
            return resultado
        except ValueError as e:
            print(f'El numero de columnas recibidos no coincide con los campos asignados {e} - carga_data_sql')
        except Exception as e:
            print(f'Error en la carga de datos {e} - carga_data_sql')
            return None
        else:
            self.db.conexion.close()


    def actualiza_data_sql(self,df, DS_PartNumber, ID):
        try:    
            inicio = time.time()
            if self.db.conexion.is_connected():
                cursor = self.db.conexion.cursor()
                sql = '''UPDATE dw.SN_DataSunat SET DS_PartNumber = %s WHERE ID = %s'''
                for i, row in df.iterrows():
                    params = (row[DS_PartNumber], row[ID])
                    print(params)
                    cursor.execute(sql, params)
                self.db.conexion.commit()
                cursor.close()
                print(f"Se actualizo la tabla en {time.time()-inicio} segundos")
            else:
                print("La conexión a la base de datos no está activa.")
            return None
        except Error as e:
            print(f"Error en la actualización de datos {e} - actualiza_data_sql")
            return None


