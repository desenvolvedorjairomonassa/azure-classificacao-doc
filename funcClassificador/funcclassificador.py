import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import fitz  # PyMuPDF
import openai
import os
import json
from azure.storage.queue import QueueClient
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações
openai.api_key = os.getenv("OPENAI_API_KEY")
blob_service_client = BlobServiceClient.from_connection_string(os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
input_container_name = "input-container"
classified_container_name = "classified-container"
dead_letter_container_name = "dead-letter-container"
processing_queue_name = "processing-queue"

def classify_document(text):
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=f"Classifique o seguinte texto em uma das categorias: jurídico, TI, expedição.",
        temperature=0.5,
        max_tokens=60,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    category = response.choices[0].text.strip().lower()
    return category

def main(msg: func.QueueMessage) -> None:
    try:
        # Ler a mensagem da fila
        message = msg.get_body().decode('utf-8')
        logging.info(f"Mensagem recebida: {message}")

        # Extrair informações da mensagem
        blob_name = json.loads(message)["blob_name"]

        # Ler o PDF do Blob Storage
        input_container_client = blob_service_client.get_container_client(input_container_name)
        blob_client = input_container_client.get_blob_client(blob_name)
        pdf_data = blob_client.download_blob().readall()

        # Extrair texto do PDF
        pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
        text = ""
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            text += page.get_text()

        # Classificar o documento usando a API do OpenAI
        category = classify_document(text)
        logging.info(f"Documento classificado como: {category}")

        # Mover o arquivo para o armazenamento classificado
        classified_container_client = blob_service_client.get_container_client(classified_container_name)
        classified_blob_client = classified_container_client.get_blob_client(f"{category}/{blob_name}")
        classified_blob_client.upload_blob(pdf_data, overwrite=True)

        # Enfileirar para processamento adicional
        processing_queue_client = QueueClient.from_queue_url(os.getenv("PROCESSING_QUEUE_URL"))
        processing_queue_client.send_message(json.dumps({"blob_name": f"{category}/{blob_name}"}))

        # Deletar o arquivo do armazenamento de entrada
        blob_client.delete_blob()

    except Exception as e:
        logging.error(f"Erro ao processar o documento: {e}")
        # Mover a mensagem para a Dead Letter Queue
        dead_letter_container_client = blob_service_client.get_container_client(dead_letter_container_name)
        dead_letter_blob_client = dead_letter_container_client.get_blob_client(blob_name)
        dead_letter_blob_client.upload_blob(msg.get_body(), overwrite=True)
