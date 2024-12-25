import psycopg2
from minio import Minio

def fetch_documents_from_db():
    conn = psycopg2.connect(
        dbname="dbname",
        user="user",
        password="password",
        host="host",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT id, document_link FROM Documents WHERE document_link IS NOT NULL")
    documents = cursor.fetchall()
    cursor.close()
    conn.close()
    return documents

def download_file_from_minio(minio_client, bucket_name, object_name, download_path):
    try:
        minio_client.fget_object(bucket_name, object_name, download_path)
    except Exception as e:
        print(f"Error downloading from Minio: {e}")
        raise

def upload_file_to_minio(minio_client, bucket_name, object_name,file_path):
    try:
        minio_client.fput_object(bucket_name, object_name.replace("_", "/"), file_path)
        return f"/S3/{bucket_name}/{object_name}"
    except Exception as e:
        print(f"Error uploading to Minio: {e}")
        raise

def update_document_link_in_db(document_id, new_link):
    conn = psycopg2.connect(
        dbname="dbname",
        user="user",
        password="password",
        host="host",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE Documents SET document_link = %s WHERE id = %s",
        (new_link, document_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

def main():
    source_minio_client = Minio(
        endpoint="host1:port",
        access_key="user1",
        secret_key="password1",
        secure=False
    )

    target_minio_client = Minio(
        endpoint="host2:port",
        access_key="user2",
        secret_key="password2",
        secure=False
    )

    target_bucket = "edc"

    documents = fetch_documents_from_db()

    for document_id, link, research_id in documents:
        source_url = link[len(f"/S3/{research_id}/"):]
        temp_file_path = f"./tmp/{research_id}{source_url.replace("_", "/")}"
        download_file_from_minio(source_minio_client, research_id, source_url.replace("_", "/"), temp_file_path)
        new_link = upload_file_to_minio(target_minio_client, target_bucket ,f"{research_id}/{source_url}", temp_file_path)
        update_document_link_in_db(document_id, new_link)
        print(f"Processed document ID {document_id}: Bucket {research_id} New link {new_link}")

if __name__ == "__main__":
    main()
