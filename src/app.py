import io
import sqlite3
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Configuración de constantes
URL_RESOURCE = "https://en.wikipedia.org/wiki/List_of_most-streamed_songs_on_Spotify"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def run():
    """Función principal para ejecutar el proceso de ETL."""
    try:
        # 1. Descarga del HTML
        response = requests.get(URL_RESOURCE, headers=HEADERS, timeout=10)
        response.raise_for_status()
        
        # 2. Transformación y Limpieza
        html_data = io.StringIO(response.text)
        df = pd.read_html(html_data, match="Artist")[0]
        
        # Renombrar y limpiar columnas
        df.columns = ['Rank', 'Song', 'Artist', 'Streams_Billions', 'Release_Date', 'Ref']
        df = df.drop(columns=['Ref'])
        df['Song'] = df['Song'].str.replace(r'\[.*\]', '', regex=True)
        df['Artist'] = df['Artist'].str.replace(r'\[.*\]', '', regex=True)
        
        # Preparar tipos de datos
        df['Streams_Billions'] = pd.to_numeric(df['Streams_Billions'], errors='coerce')
        df['Release_Date'] = pd.to_datetime(df['Release_Date'], errors='coerce')
        df['Year'] = df['Release_Date'].dt.year
        df = df.dropna(subset=['Year', 'Streams_Billions'])

        print("¡DATOS LISTOS!")
        print(df.head())

        # 3. Almacenamiento en SQLite
        with sqlite3.connect('spotify.db') as conn:
            df.to_sql('top_songs', conn, if_exists='replace', index=False)
        print("Base de datos 'spotify.db' actualizada.")

        # 4. Generación de las 3 Gráficas
        sns.set_theme(style="whitegrid")

        # Gráfica 1: Artistas
        plt.figure(figsize=(10, 6))
        top_artistas = df['Artist'].value_counts().head(10)
        sns.barplot(x=top_artistas.values, y=top_artistas.index, palette='viridis', hue=top_artistas.index, legend=False)
        plt.title('Top 10 Artistas')
        plt.savefig('grafica_1_artistas.png')
        plt.close()

        # Gráfica 2: Dispersión
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='Year', y='Streams_Billions', hue='Streams_Billions', palette='magma')
        plt.title('Streams por Año')
        plt.savefig('grafica_2_evolucion.png')
        plt.close()

        # Gráfica 3: Histograma
        plt.figure(figsize=(10, 6))
        sns.histplot(df['Streams_Billions'], bins=15, kde=True, color='teal')
        plt.title('Distribución de Streams')
        plt.savefig('grafica_3_distribucion.png')
        plt.close()
        
        print("Gráficas guardadas correctamente.")

    except requests.exceptions.RequestException as e:
        print(f"Error de red: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    run()