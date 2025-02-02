from google.cloud import storage
import pandas as pd
from google.cloud import bigquery
import tempfile
import os

PROJECT_ID = "isi-group-m2-dsia"
BUCKET_NAME = "m2dsia-attoisse-mohamed-data"
TABLE_ID = f"{PROJECT_ID}.dataset_attoisse_mohamed.transactions"
client = storage.Client(project=PROJECT_ID)
bucket = client.get_bucket(BUCKET_NAME)
storage_client = storage.Client()
bigquery_client = bigquery.Client()

def upload_file(file_path, destination_blob_name):
    """uploder les fichiers dans le dossier 'input/' du bucket."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(f"Fichier '{file_path}' uploadé vers {destination_blob_name}.")

# 🔍 Lister les fichiers dans `input/`
def list_files_in_input():
    """Liste tous les fichiers dans le dossier 'input/' du bucket."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="input/")  
    files = [blob.name for blob in blobs if not blob.name.endswith("/")]
    return files

def download_blob(source_blob_name, destination_file_name):
    """Télécharge un fichier depuis Cloud Storage vers un fichier local temporaire."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"📥 {source_blob_name} téléchargé en local.")

def upload_blob(content, destination_blob_name):
    """Upload un fichier ou une donnée string dans Cloud Storage."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    
    if isinstance(content, str):  
        blob.upload_from_string(content, content_type='text/csv')
    else:  
        blob.upload_from_string(content)
    
    print(f"📤 {destination_blob_name} uploadé dans {BUCKET_NAME}.")

def delete_file(filename):
    """Supprime un fichier du bucket Cloud Storage."""
    try:
        blob = bucket.blob(filename)
        if blob.exists():
            blob.delete()  
            print(f"Le fichier {filename} a été supprimé avec succès.")
        else:
            print(f"Le fichier {filename} n'existe pas dans le bucket.")
    except Exception as e:
        print(f"❌ Erreur lors de la suppression du fichier {filename}: {e}")

def clean_data(df):
    """Nettoie les données du DataFrame."""
    required_columns = ['transaction_id', 'product_name', 'category', 'price', 'quantity', 'date']
    
    if df[required_columns].isnull().any().any():
        raise ValueError("❌ Champs obligatoires manquants. Impossible de nettoyer les données.")

    df = df.copy()  
    df['customer_name'] = df['customer_name'].fillna(df['customer_name'].mode()[0])
    df['customer_email'] = df['customer_email'].fillna(df['customer_email'].mode()[0])

    df['transaction_id'] = df['transaction_id'].astype(int)
    df['product_name'] = df['product_name'].astype(str)
    df['category'] = df['category'].astype(str)
    df['price'] = df['price'].astype(float)
    df['quantity'] = df['quantity'].astype(int)
    df['date'] = pd.to_datetime(df['date']).dt.date

    df.drop_duplicates(inplace=True)

    return df

def validate_data(df):
    """Vérifie que les données sont valides."""
    required_columns = ['transaction_id', 'product_name', 'category', 'price', 'quantity', 'date']
    if not all(col in df.columns for col in required_columns):
        return False
    
    if df[required_columns].isnull().any().any():
        return False
    
    try:
        df['transaction_id'] = df['transaction_id'].astype(int)
        df['product_name'] = df['product_name'].astype(str)
        df['category'] = df['category'].astype(str)
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(int)
        df['date'] = pd.to_datetime(df['date']).dt.date
    except ValueError:
        return False
    
    return True

def process_cloud_file(cloud_file_path):
    """Télécharge, traite et charge un fichier dans BigQuery."""
    local_tmp_dir = tempfile.gettempdir()  
    local_file_path = os.path.join(local_tmp_dir, cloud_file_path.split('/')[-1])
    download_blob(cloud_file_path, local_file_path)  # Téléchargement

    try:
        df = pd.read_csv(local_file_path)

        if not validate_data(df):
            raise ValueError("Données invalides. Champs obligatoires manquants ou types incorrects.")

        df_cleaned = clean_data(df)

        clean_file_path = f"clean/{cloud_file_path.split('/')[-1]}"
        upload_blob(df_cleaned.to_csv(index=False), clean_file_path)

        delete_file(clean_file_path)

        done_file_path = f"done/{cloud_file_path.split('/')[-1]}"
        upload_blob(open(local_file_path, 'rb').read(), done_file_path)

        print(f"✅ {cloud_file_path} traité et déplacé vers clean/ et done/.")
        
    except Exception as e:
        error_file_path = f"error/{cloud_file_path.split('/')[-1]}"
        upload_blob(open(local_file_path, 'rb').read(), error_file_path)

        print(f"❌ Erreur lors du traitement de {cloud_file_path}. Déplacé vers error/. Erreur : {e}")

def main():
    files = list_files_in_input()

    if not files:
        print("🔎 Aucun fichier à traiter dans input/.")
        return
    
    for i, file in enumerate(files, start=1):
        print(f"📂 Processus du fichier {i}/{len(files)} : {file}")
        process_cloud_file(file)
        print()
    
    print("✅ Fin du traitement.")

if __name__ == "__main__":
    main()
