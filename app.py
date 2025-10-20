from generate_data_parada import matches_producto_alex_scada
from database import Database
import time
from datetime import datetime
import pandas as pd

def monitor_and_insert_db(interval_seconds: int = 5, db_instance: Database = None):
    """
    Cada interval_seconds ejecuta matches_producto_alex_scada() y
    para cada fila nueva llama a db_instance.process_data_paradas(...) con:
    (codMaquina, inicioParada_alex, desParada, operario, idNumOrd,
     inicioParada, finParada, horaParada, fecha, horaInicio, horaFin, turno, desOrden)
    Evita duplicados en memoria.
    """
    if db_instance is None:
        raise ValueError("Se requiere una instancia Database en db_instance")

    seen = set()
    try:
        while True:
            df = matches_producto_alex_scada()
            if df is None or df.empty:
                time.sleep(interval_seconds)
                continue

            # Normalizar nombres de columna mínimos (si existen variantes)
            df = df.copy()
            # asegurar columnas que usaremos, si faltan rellenar con None
            cols_needed = [
                'codMaquina', 'inicioParada_alex', 'desParada', 'operario', 'idNumOrd',
                'inicioParada', 'finParada', 'horaParada', 'fecha', 'horaInicio', 'horaFin', 'turno', 'desOrden'
            ]
            for c in cols_needed:
                if c not in df.columns:
                    df[c] = pd.NA

            # convertir tiempos a datetime (si corresponde)
            for tcol in ['inicioParada_alex', 'inicioParada', 'finParada']:
                df[tcol] = pd.to_datetime(df[tcol], errors='coerce')

            # iterar filas y enviar a BD solo si no están en seen
            for row in df.itertuples(index=False, name=None):
                # crear tupla identificadora (puedes ajustar campos)
                # uso (codMaquina, inicioParada_alex, idNumOrd) como identificador
                codMaquina = row[df.columns.get_loc('codMaquina')]
                inicioParada_alex = row[df.columns.get_loc('inicioParada_alex')]
                idNumOrd = row[df.columns.get_loc('idNumOrd')]

                key = (str(codMaquina), 
                       pd.NaT if pd.isna(inicioParada_alex) else pd.Timestamp(inicioParada_alex).to_pydatetime(),
                       str(idNumOrd) if not pd.isna(idNumOrd) else '')

                if key in seen:
                    continue

                # preparar valores en el orden esperado (con fallback a None)
                def val(col):
                    v = row[df.columns.get_loc(col)]
                    return None if pd.isna(v) else v

                inicio_db = val('inicioParada_alex')
                desParada_db = val('desParada')
                operario_db = val('operario')
                idNumOrd_db = val('idNumOrd')
                inicioParada_db = val('inicioParada')
                finParada_db = val('finParada')
                horaParada_db = val('horaParada')
                fecha_db = val('fecha')
                horaInicio_db = val('horaInicio')
                horaFin_db = val('horaFin')
                turno_db = val('turno')
                desOrden_db = val('desOrden')

                # Llamar al método de inserción
                try:
                    ok = db_instance.process_data_paradas(
                        codMaquina, inicio_db, desParada_db, operario_db, idNumOrd_db,
                        inicioParada_db, finParada_db, horaParada_db, fecha_db, horaInicio_db,
                        horaFin_db, turno_db, desOrden_db
                    )
                    if ok:
                        seen.add(key)
                except Exception as e:
                    print(f"Error insertando fila {key}: {e}")

            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Monitor detenido por usuario.")
    except Exception as e:
        print(f"Monitor finalizado por error: {e}")

if __name__ == "__main__":
    db = Database()  # usa tu constructor ya configurado
    monitor_and_insert_db(interval_seconds=5, db_instance=db)
