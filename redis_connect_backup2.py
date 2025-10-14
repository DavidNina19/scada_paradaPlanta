# mqtt_with_redis.py
import redis
import json
import re
import pandas as pd
from datetime import datetime, time, timedelta
from collections import defaultdict
import json
import re

class MQTTWithRedis:
    def __init__(self, redis_host="192.168.252.99", redis_port=6379, redis_db=0):
        # Conexión a Redis
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )

    def guardar_dato(self, nivel_agua, id_redis, rango_tiempo):
        """Guarda el dato recibido de MQTT en Redis"""
        t_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Generar clave por día
        fecha_clave = datetime.now().strftime(f"{id_redis}_%Y_%m_%d")

        # Datos a guardar
        data = {
            "valor": nivel_agua,
            "t_stamp": t_stamp
        }

        # Guardar en Redis (como lista de JSONs)
        self.redis_client.rpush(fecha_clave, json.dumps(data))

        # Configurar expiración de 2 días
        if self.redis_client.ttl(fecha_clave) == -1:
            self.redis_client.expire(fecha_clave, rango_tiempo)

        print(f"💾 Guardado en Redis -> {fecha_clave} : {data}")

    def procesar_datos_y_guardar(self):
        """Procesa datos de Redis y guarda min y max en MySQL"""
        fecha_clave = datetime.now().strftime("tanqueagua_%Y_%m_%d")
        datos = self.redis_client.lrange(fecha_clave, 0, -1)

        if not datos:
            print("⚠️ No hay datos en Redis para procesar")
            return

        valores_filtrados = []
        for item in datos:
            d = json.loads(item)
            try:
                valor = float(d["valor"])
                t_stamp = datetime.strptime(d["t_stamp"], "%Y-%m-%d %H:%M:%S")
                hora = t_stamp.time()
                if time(7, 0, 0) <= hora <= time(20, 0, 0):
                    valores_filtrados.append({"valor": valor, "t_stamp": t_stamp})
            except Exception as e:
                print("⚠️ Error al convertir valor:", e)

        if not valores_filtrados:
            print("⚠️ No hay datos entre 7am y 8pm")
            return

        # Encontrar mínimo y máximo con su timestamp
        valor_min = min(valores_filtrados, key=lambda x: x["valor"])
        valor_max = max(valores_filtrados, key=lambda x: x["valor"])

        if valor_min["valor"] > 0 and valor_max["valor"] <= 30:
            if (valor_max["valor"] - valor_min["valor"]) > 15:
                # Usamos el t_stamp del valor máximo
                fecha_registro = valor_max["t_stamp"].strftime("%Y-%m-%d %H:%M:%S")
                self.db_mysql.insert_reporte_agua(
                    valor_min["valor"], valor_max["valor"], fecha_registro
                )
                abastecido = valor_max["valor"] - valor_min["valor"]
                self.guardar_dato(abastecido, 'reporte_reaba', 31536000)
                print(f"📊 Mínimo={valor_min['valor']}, Máximo={valor_max['valor']}, Fecha={fecha_registro}")

    def leer_datos_redis(self, fecha_clave):
        """
        Función para leer los datos desde Redis usando la fecha clave proporcionada.
        """
        datos = self.redis_client.lrange(fecha_clave, 0, -1)
        # Convertir cada elemento de la lista en un diccionario
        datos_json = [json.loads(item) for item in datos]
        return datos_json

    def read_produccion(self):
        fecha_clave = datetime.now().strftime("produccion_%Y_%m_%d")
        datos = self.redis_client.lrange(fecha_clave, 0, -1)
        if not datos:
            print("⚠️ No hay datos en Redis para procesar")
            return
        valores = [json.loads(item) for item in datos]
        df = pd.DataFrame(valores)
        df['t_stamp'] = pd.to_datetime(df['t_stamp'])
        df.set_index('t_stamp', inplace=True)
        df.sort_index(inplace=True)
        return df

    def limpiar_codmaq(self, cod_maquina):
        patron_busqueda = r"MicroWin\.PLC\d+"
        sin_micro = re.sub(patron_busqueda, "", cod_maquina)
        sin_cto = sin_micro.replace(".CTO", "")
        format_codmaq = sin_cto.strip()
        return format_codmaq
    
    def filtrar_conteo_produccion(self, df):
        # Asegurarse de trabajar con strings y filtrar por 'CTO' (insensible a mayúsculas)
        mask_cto = df['codmaq'].astype(str).str.contains(r'CTO', case=False, na=False)
        df_cto = df[mask_cto].copy()
        return df_cto

    def paradas_produccion(self):
        df = self.read_produccion()
        if df is None or df.empty:
            print("⚠️ No hay datos para analizar paradas de producción")
            return pd.DataFrame()
        df_filtrado = self.filtrar_conteo_produccion(df) 
        if df_filtrado is None or df_filtrado.empty:
            print("⚠️ No hay datos para analizar paradas de producción")
            return pd.DataFrame()
        df_filtrado['codmaq'] = df_filtrado['codmaq'].apply(self.limpiar_codmaq) 
        maquinas = defaultdict(list)
        df_filtrado_final = df_filtrado.reset_index(names='t_stamp') 
        for fila in df_filtrado_final.itertuples(index=False):
            registro_actual = {
                't_stamp': fila.t_stamp,
                'codmaq': fila.codmaq,
                'valor': fila.valor
            }
            maquinas[fila.codmaq].append(registro_actual) 
        resultados_paradas = []

        umbral_parada = timedelta(minutes=10)
        for maquina, registros in maquinas.items():
            df_conteo = pd.DataFrame(registros).sort_values(by='t_stamp')
            df_conteo['df_tiempo'] = df_conteo['t_stamp'].diff().fillna(pd.Timedelta(seconds=0))

            df_paradas = df_conteo[df_conteo['df_tiempo'] > umbral_parada].copy()
            
            if df_paradas.empty:
                continue # Pasa a la siguiente máquina si no hay paradas

            df_paradas['finParada'] = df_paradas['t_stamp']
            t_stamps_anteriores = df_conteo['t_stamp'].shift(1)

            df_paradas['inicioParada'] = t_stamps_anteriores[df_paradas.index]            
            df_paradas['MAQUINA'] = maquina
            df_paradas['HORA PARADA'] = df_paradas['df_tiempo']
            
            resultados_paradas.append(df_paradas[['MAQUINA', 'inicioParada', 'finParada', 'HORA PARADA']])
            
        if not resultados_paradas:
            print("✅ No se identificaron paradas de producción mayores a 10 minutos en la jornada.")
            return pd.DataFrame(columns=['MAQUINA', 'inicioParada', 'finParada', 'HORA PARADA'])

        df_resultado_final = pd.concat(resultados_paradas, ignore_index=True)
        df_resultado_final['FECHA'] = df_resultado_final['inicioParada'].dt.strftime('%Y-%m-%d')
        df_resultado_final['HORA INICIO'] = df_resultado_final['inicioParada'].dt.strftime('%H:%M:%S')
        df_resultado_final['HORA FIN'] = df_resultado_final['finParada'].dt.strftime('%H:%M:%S')
        
        df_resultado_final['HORA PARADA'] = (
            df_resultado_final['HORA PARADA'].dt.total_seconds() / 3600
        ).round(2)

        print(f"🛑 Se identificaron {len(df_resultado_final)} periodos de parada mayores a 10 minutos.")
        return df_resultado_final
            
    
if __name__ == "__main__":
    read_redis = MQTTWithRedis()
    db = read_redis.paradas_produccion()
    db.to_excel("registro_paradas.xlsx", index=False)
    print(db)
    
