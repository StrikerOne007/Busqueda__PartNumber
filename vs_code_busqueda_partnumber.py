from src.administra_app import AppManager
import sys
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

if __name__ == "__main__":
    app = AppManager()

    # Datos de tabla importacion SUNAT
    consulta_sql_importacion = """
                                SELECT
                                    SN.ID
                                    ,SNN.SA_Value AS PRODUCTO
                                    ,SNV.SA_Value AS PART_NUMBER
                                    ,SNA.SA_Value AS TIPO_PRODUCTO
                                    ,SN.DS_IMPORTADOR AS IMPORTADOR
                                    ,SN.DS_DESC_COM AS DESCRIPCION
                                FROM
                                    dw.SN_DataSunat SN
                                LEFT JOIN
                                    dw.SN_SunatAttribute SNV
                                ON
                                    SN.id = SNV.SN_Id
                                AND
                                    SNV.MCA_Attribute_id = '1010'
                                LEFT JOIN
                                    dw.SN_SunatAttribute SNA
                                ON
                                    SN.id = SNA.SN_Id
                                AND
                                    SNA.MCA_Attribute_id = '1008'
                                LEFT JOIN
                                    dw.SN_SunatAttribute SNN
                                ON
                                    SN.id = SNN.SN_Id
                                AND 
                                    SNN.MCA_Attribute_id = '1009'
                                WHERE
                                    SN.DS_LIBR_TRIBU IN (
                                                    SELECT RUC
                                                    FROM
                                                    (
                                                    SELECT DS_LIBR_TRIBU RUC,
                                                            COUNT(*) IMPORTACIONES
                                                    FROM dw.SN_DataSunat
                                                    WHERE DS_LIBR_TRIBU <> 'No Disponi.'
                                                    GROUP BY DS_IMPORTADOR
                                                    ORDER BY 2 DESC
                                                    LIMIT 30
                                                    ) IMPORTADORES
                                    )
                                AND SN.DS_LIBR_TRIBU = '20212331377'
                                AND SN.ID IN (
                                        '303018'
                                )
                                # AND SNA.SA_Value IN ('MOTHERBOARDS', 'LAPTOP', 'DESKTOP PC', 'ALL IN ONE', 'SERVIDOR')
                                AND DS_FECHA >= '2019-01-01' AND  DS_FECHA < '2024-12-01'
                                AND DS_PartNumber IS NULL OR DS_PartNumber = ''
                                ORDER BY
                                    SN.ID
                                # LIMIT 1000;
        """
    columnas_sql_importacion = ['ID','MARCA','SA_Value','TIPO_PRODUCTO', 'DS_IMPORTADOR','DS_DESC_COM']
    archivo = None

    # Datos de tablas GD
    consulta_sql_data_deltron = """
                                SELECT
                                DI.IMI_Id AS PRODUCTO,
                                DI.IMI_Name AS NOMBRE,
                            -- 	REPLACE(IT.IMIS_TechInfo, '[@@@]', ' ') AS DESCRIPCION,
                            --	ISL.IMSS_Price AS PRECIO,
                                MRP.IMM_PartNumber AS PART_NUMBER
                            FROM
                                d7erpdev.IM_Item DI
                                    LEFT JOIN
                                d7erpdev.IM_ItemSpecs IT ON IT.IMI_Id = DI.IMI_Id
                            --             LEFT JOIN
                            --         d7erpdev.IM_ItemSales ISL ON ISL.IMI_Id = DI.IMI_Id
                                    LEFT JOIN
                                d7erpdev.IM_ItemMRP MRP ON MRP.IMI_Id = DI.IMI_Id
                                            AND MRP.IMM_PartNumber IS NOT NULL AND TRIM(MRP.IMM_PartNumber) != ''
                            WHERE
                                1
                                        AND DI.IMI_Id NOT LIKE 'Z%'
                                                AND MRP.IMM_PartNumber is NOT NULL
                                                AND LENGTH(MRP.IMM_PartNumber) > 4
                            GROUP BY PART_NUMBER
                            ORDER BY
                                3
                            -- LIMIT 10000
    """
    columnas_sql_data_deltron = ['PRODUCTO', 'NOMBRE', 'PART_NUMBER' ]

    # Datos de tabla modelos GD para extraccion part number
    consulta_sql_data_modelos = """
                            SELECT
                                            UPPER(ITSP.IMI_Id)        AS PRODUCTO
                            -- 				,TSP.MCIS_Id			  AS MCIS_Id
                            -- 				,CATSP.MCIT_Id            AS GRUPO
                            -- 			 	,CATSP.CS_Rpt_Repo_Order AS SECUENCIA
                            -- 				,TSP.MCIS_Description     AS DESCRIPCION
                                            ,ITSP.IS_Value            AS MODELO
                                            ,MRP.IMM_PartNumber       AS PART_NUMBER
                                            FROM d7erpdev.IM_Item I

                                            LEFT JOIN d7erpdev.IM_ItemTecSpecs ITSP ON ITSP.IMI_Id = I.IMI_Id
                                            LEFT JOIN d7erpdev.MC_TecSpecs TSP ON TSP.MCIS_Id=ITSP.MCIS_Id
                                            LEFT JOIN d7erpdev.IM_CategorySpecs CATSP ON CATSP.MCIS_Id=TSP.MCIS_Id AND CATSP.MCIT_Id=I.MCIT_Id
                                            LEFT JOIN d7erpdev.IM_ItemMRP MRP ON MRP.IMI_Id=ITSP.IMI_Id
                                            WHERE 1

                            -- 				AND I.MCIS_Id = 'F'
                            -- 				AND I.IMI_Id not like 'ZZ%'
                            -- 				AND I.IMI_Id not like 'REP%'
                            -- 				AND I.MCIT_Id NOT IN ('GE','RST','0000','RP','SRV','RMT','MSLV','RTA','RTV')
                            --
                            -- 				AND ITSP.IS_Status='A'
                            -- 				AND TSP.MCIS_Status='A'

                                        -- 	AND CATSP.CS_Rpt_Repo='Y'
                                        -- AND MRP.IMM_PartNumber like '%G733PYV%'
                                        -- AND ITSP.IS_Value like '%X1504VA-NJ944%'
                                        -- AND MRP.IMM_PartNumber like '%G733PYV%'
                                            --  ".$s_Where_Spec."
                            AND CATSP.MCIT_Id  ='NBK'
                            AND TSP.MCIS_Description ='Descripcion Modelo'
                            -- 				order by 1
                            -- LIMIT 5000
    """

    columnas_sql_modelo = ['PRODUCTO', 'MODELO', 'PART_NUMBER' ]

    consulta_carga_tabla = "UPDATE dw.SN_DataSunat SET DS_PartNumber = %s, DS_ItemNumber = %s WHERE ID = %s"

    app.ejecutar_flujo_importacion(consulta_sql_importacion, columnas_sql_importacion, archivo)
    app.ejecutar_flujo_data_deltron(consulta_sql_data_deltron, columnas_sql_data_deltron)
    app.ejecutar_flujo_data_modelo(consulta_sql_data_modelos, columnas_sql_modelo)
    app.ejecutar_operaciones_dataframe()
    # app.ejecutar_analisis_datos()

