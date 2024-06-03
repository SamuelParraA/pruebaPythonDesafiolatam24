#############################################################################################
#############################################################################################
                                    #FUNCIONES#
#############################################################################################
#############################################################################################


#Función para filtrar entre fechas

def filtro_fecha_orden(df, fecha_inicio, fecha_fin):
    import pandas
    
    df['orderDate'] = pandas.to_datetime(df['orderDate']) #Convierte orderDate en datetime 
    df2 = df[(df['orderDate'] >= fecha_inicio) & (df['orderDate'] <= fecha_fin)] # Filtra la data
    
    return df2



# Función para sumar valores para una variable agrupada

def suma_por_var(df, var_agg, suma1):
    
    df2 = df.pivot_table(values=[suma1], index=var_agg, aggfunc=sum,  fill_value=0).reset_index()
    
    return df2

#Funcion para escribir en la base de datos desde dataframe



def dataframeToPostgresql(df, table_name, conn_string, esquema="public"):
    import pandas as pd
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.exc import SQLAlchemyError
    # crear conexión
    engine = create_engine(conn_string)
    
    try:
        # Comprobar si la tabla ya existe
        if not inspect(engine).has_table(table_name, schema=esquema):
            # Si la tabla no existe, guardar el DataFrame
            df.to_sql(table_name, engine, index=False)
            print(f"Tabla '{table_name}' creada y datos insertados.")
        else:
            print(f"La tabla '{table_name}' ya existe. No se realizaron cambios.")
    except SQLAlchemyError as e:
        print(f"Error al interactuar con la base de datos: {e}")
    finally:
        # Cerrar la conexión
        engine.dispose()


    


