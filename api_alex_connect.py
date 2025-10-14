from datetime import datetime
import requests
import json  # Importa la librería json para formatear la salida

class Api:
    def __init__(self, urlLogin = 'http://192.168.252.6/serviciowebaccesonet/api/authentication/logintercero', urlData=''):
        self.url = urlLogin
        self.urlData = urlData
        self.data = {"User": "SCADAEME", "Password": "SCADAEME2024"}
        self.headers = {"Accept": "application/json"}
        self.all_data = None  # Cambiado a all_data para reflejar que contiene todos los datos

    def get_all_data(self):  # Método para obtener todos los datos
        try:
            response = requests.post(self.url, headers=self.headers, json=self.data)
            response.raise_for_status() # Lanza una excepción si el código de estado no es 200
            data = response.json()
            token = data.get("accessToken") # Usamos .get() para evitar errores si no existe la clave
            if token:
                newUrl = self.urlData
                
                headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
                newResponse = requests.get(newUrl, headers=headers)
                newResponse.raise_for_status()
                self.all_data = newResponse.json() # Almacena todos los datos

                # Imprime los datos formateados en JSON para inspeccionarlos
                #print(json.dumps(self.all_data, indent=4)) # Formateado con indentación

                return self.all_data  # Devuelve los datos para que puedas usarlos fuera de la clase

            else:
                print("No se encontró el token en la respuesta de autenticación.")
                print(data) # Imprime la respuesta para depurar
                return None

        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud: {e}")
            return None
        except (KeyError, TypeError) as e: # Captura errores si 'accessToken' no está presente o la estructura es inesperada
            print(f"Error al procesar la respuesta JSON: {e}")
            print(data) # Imprime la respuesta para depurar
            return None
