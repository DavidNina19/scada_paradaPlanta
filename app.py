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
                # construir clave estable basada en campos SCADA (no incluir idNumOrd/operario
                # porque estos se rellenan posteriormente y harían que filas antiguas parezcan nuevas)
                try:
                    cod_val = row[df.columns.get_loc('codMaquina')]
                except Exception:
                    cod_val = ''
                try:
                    scada_inicio = row[df.columns.get_loc('inicioParada')]
                except Exception:
                    scada_inicio = None
                try:
                    scada_fin = row[df.columns.get_loc('finParada')]
                except Exception:
                    scada_fin = None

                def fmt(dt):
                    try:
                        if pd.isna(dt):
                            return ''
                        if hasattr(dt, 'strftime'):
                            return dt.strftime('%Y-%m-%d %H:%M:%S')
                        return str(dt)
                    except Exception:
                        return str(dt)

                key = (str(cod_val), fmt(scada_inicio), fmt(scada_fin))

                if key in seen:
                    # debug: indicar por qué se omite
                    try:
                        print(f"Skipping already-seen row (key): {key}")
                    except Exception:
                        print("Skipping already-seen row (key) - (could not stringify key)")
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

                # extraer codMaquina (se usa en el mensaje y para la clave de seen)
                codMaquina = val('codMaquina')

                # Llamar al método de inserción
                try:
                    # debug: mostrar intento de inserción
                    print(f"Attempting insert for machine={codMaquina}, id={idNumOrd_db}, inicio_alex={inicio_db}")
                    ok = db_instance.process_data_paradas(
                        codMaquina, inicio_db, desParada_db, operario_db, idNumOrd_db,
                        inicioParada_db, finParada_db, horaParada_db, fecha_db, horaInicio_db,
                        horaFin_db, turno_db, desOrden_db
                    )
                    if ok:
                        seen.add(key)
                    else:
                        print(f"Insert returned False for key={key}")
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
