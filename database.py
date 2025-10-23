import pymysql as sql
from datetime import datetime

class Database:
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.host = host or '192.168.252.35'
        self.user = user or 'LABTI'
        self.password = password or 'ScadaEMEMSA40' # Asegúrate de que esta contraseña sea correcta
        self.database = database or 'paradas_produccion'
        self.port = port or 3306
        self.connection = None
        self.cursor = None
        self.connect() # Llama a connect directamente en el __init__

    def connect(self):
        """Establece la conexión a la base de datos si no está ya abierta."""
        if self.connection is None or not self.is_connected():
            try:
                self.connection = sql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    port=self.port
                )
                self.cursor = self.connection.cursor() # Asigna el cursor aquí también
                print("Conexión a la base de datos establecida con éxito.")
            except sql.MySQLError as e:
                print(f"Error al conectar a la base de datos: {e}")
                self.connection = None # Asegura que la conexión sea None si falla
                self.cursor = None
        else:
            print("La conexión a la base de datos ya está activa.") # Para depuración

    def is_connected(self):
        """Verifica si la conexión a la base de datos está activa."""
        try:
            if self.connection:
                # Intenta una operación simple para verificar si la conexión es válida
                self.connection.ping(reconnect=True) 
                return True
            return False
        except sql.MySQLError:
            return False

    def viewTable(self, tablename):
        if not self.is_connected(): 
            print("No hay conexión a la base de datos para ver la tabla.")
            return []
        try:
            self.cursor.execute(f"SELECT * FROM {tablename}")
            # No se necesita commit para SELECT, solo para DML/DDL
            return self.cursor.fetchall()
        except sql.MySQLError as e:
            print(f"Error al ver la tabla {tablename}: {e}")
            return []

    def get_query(self, query):
        if not self.is_connected(): 
            print("No hay conexión a la base de datos para ejecutar la consulta GET.")
            return []
        try:
            self.cursor.execute(query)
            # No se necesita commit para SELECT, solo para DML/DDL
            return self.cursor.fetchall()
        except sql.MySQLError as e:
            print(f"Error al ejecutar la consulta GET: {e}")
            return []

    def set_query(self, query, params=None):
        """Ejecuta una consulta INSERT, UPDATE o DELETE."""
        if not self.is_connected(): 
            print("No hay conexión a la base de datos para ejecutar la consulta SET.")
            return False
        try:
            # Usa el cursor existente si ya está disponible, o crea uno temporal
            cursor = self.connection.cursor() if self.cursor else self.connection.cursor()
            cursor.execute(query, params or ())
            self.connection.commit()
            if cursor is not self.cursor: # Si se creó un cursor temporal, ciérralo
                cursor.close()
            return True
        except sql.MySQLError as e:
            print(f"Error al ejecutar la consulta: {e}")
            self.connection.rollback() # Siempre rollback en caso de error
            return False
        except Exception as e:
            print(f"Error inesperado al ejecutar la consulta SET: {e}")
            self.connection.rollback()
            return False
    
    # Renombrado disconnect a close para ser más estándar
    def close(self):
        """Cierra la conexión a la base de datos."""
        if self.connection and self.is_connected():
            self.connection.close()
            print("Conexión a la base de datos cerrada.")
        else:
            print("No hay conexión activa que cerrar.")
    
    def process_data_paradas(self, codMaquina, inicioParada_alex, desParada, operario, idNumOrd, inicioParada, finParada, horaParada, fecha, horaInicio, horaFin, turno, desOrden):
        if not self.is_connected():
            print("No hay conexión a la base de datos para insertar reporte de agua.")
            return False

        # Tabla dinámica del mes/año actual
        table_name = datetime.now().strftime("parada_planta_%m_%Y")

        try:
            query = f"""
            INSERT INTO {self.database}.{table_name} (
                codMaquina, inicioParada_alex, desParada, operario, idNumOrd, 
                inicioParada, finParada, horaParada, fecha, horaInicio, 
                horaFin, turno, desOrden
            )
            VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s
            );
            """
            data_to_insert = (
                codMaquina, inicioParada_alex, desParada, operario, idNumOrd, 
                inicioParada, finParada, horaParada, fecha, horaInicio, 
                horaFin, turno, desOrden
            )
            
            self.cursor.execute(query, data_to_insert)
            self.connection.commit()
            print(f"✅ Insertado en '{table_name}' la máquina: {codMaquina}")
            return True
        except Exception as e:
            self.connection.rollback()
            print(f"❌ Error al insertar en {table_name}: {e}")
            return False
