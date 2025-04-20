import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import fitz  # PyMuPDF
import os
import json
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
classified_container_name = "classified-container"
dead_letter_container_name = "dead-letter-container"
db_connection_string = os.getenv("DB_CONNECTION_STRING")
openai_api_key = os.getenv("OPENAI_API_KEY")

# Inicializar o cliente OpenAI
openai_client = OpenAI(api_key=openai_api_key)

def insert_vector_into_db(vector, metadata):
    """Insere um vetor no banco de dados"""
    conn = psycopg2.connect(db_connection_string)
    cursor = conn.cursor()
    query = "INSERT INTO vectors (vector, metadata) VALUES (%s, %s)"
    cursor.execute(query, (vector, json.dumps(metadata)))
    conn.commit()
    cursor.close()
    conn.close()

def main(msg: func.QueueMessage) -> None:
    try:
        # Ler a mensagem da fila
        message = msg.get_body().decode('utf-8')
        logging.info(f"Mensagem recebida: {message}")

        # Extrair informações da mensagem (assumindo que a mensagem contém o nome do blob)
        blob_name = json.loads(message)["blob_name"]

        # Ler o PDF do Blob Storage
        classified_container_client = blob_service_client.get_container_client(classified_container_name)
        blob_client = classified_container_client.get_blob_client(blob_name)
        pdf_data = blob_client.download_blob().readall()

        # Extrair texto do PDF
        pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
        text = ""
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text += page.get_text()

        # Gerar embedding usando o OpenAI
        response = openai_client.embeddings.create(
            input=text,
            model="text-embedding-3-small"
        )

        # Obter o vetor de embedding
        vector = response.data[0].embedding
        metadata = {"blob_name": blob_name, "text": text}

        # Inserir vetor no banco de dados
        insert_vector_into_db(vector, metadata)
        logging.info(f"Vetor inserido no banco de dados para o blob: {blob_name}")

    except Exception as e:
        logging.error(f"Erro ao processar o documento: {e}")
        # Mover a mensagem para a Dead Letter Queue
        dead_letter_container_client = blob_service_client.get_container_client(dead_letter_container_name)
        dead_letter_blob_client = dead_letter_container_client.get_blob_client(blob_name)
        dead_letter_blob_client.upload_blob(msg.get_body(), overwrite=True)
