from PIL import Image
from psycopg2 import connect
from dotenv import load_dotenv
from minio import Minio
import requests
import os
from io import BytesIO
import json

load_dotenv()

new_bucket_url = "https://%s/%s" % (os.getenv("AWS_ENDPOINT"), os.getenv("AWS_BUCKET_NAME"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# print("BASE_DIR", BASE_DIR)


def download_image(url):
    r = requests.get(url, stream=True)
    return BytesIO(r.content)


def convert_to_webp(file_id, cursor, s3_client):
    # fetch file
    file = cursor.execute("SELECT url,mime,hash,formats FROM files WHERE id=%s", (file_id,))
    print("Fetched file")
    file = cursor.fetchone()
    print(file)

    # download image only png,jpg,jpeg

    if file[1] == "image/png" or file[1] == "image/jpg" or file[1] == "image/jpeg":
        # print("FORMATS", file[3])
        # TODO: Download image
        print("Downloading image")
        image_bytes = download_image(file[0])
        # convert to webp
        print("Converting to webp")
        image = Image.open(image_bytes)
        image.save(
            os.path.join(BASE_DIR, f"./temp/{file[2]}.webp"),
            "webp", quality=70, optimize=True)
        s3_client.fput_object(
            os.getenv("AWS_BUCKET_NAME"),
            f"{file[2]}.webp",
            os.path.join(BASE_DIR, f"./temp/{file[2]}.webp"),
            content_type="image/webp",
        )
        # return
        # change url in database
        print("Updating database")
        cursor.execute(
            "UPDATE files SET url=%s,mime=%s,ext=%s,name=%s WHERE id=%s",
            (f"{new_bucket_url}/{file[2]}.webp",
             "image/webp", 
             ".webp",
             file[2] + ".webp",
             file_id),
        )
        cursor.execute("COMMIT")
        

        formats_dict: dict = file[3]

        if formats_dict is None:
            return

        if formats_dict.get("large"):
            image = Image.open(download_image(formats_dict["large"]["url"]))
            image.save(os.path.join(BASE_DIR, f"./temp/{formats_dict["large"]['hash']}.webp"), "webp", quality=70, optimize=True)
            s3_client.fput_object(
                os.getenv("AWS_BUCKET_NAME"),
                f"{formats_dict["large"]['hash']}.webp",
                os.path.join(BASE_DIR, f"./temp/{formats_dict["large"]['hash']}.webp"),
                content_type="image/webp",
            )
            formats_dict["large"]["url"] = f"{new_bucket_url}/{formats_dict["large"]['hash']}.webp"
            formats_dict["large"]["mime"] = "image/webp"
            formats_dict["large"]["ext"] = ".webp"
            formats_dict["large"]["name"] = f"{formats_dict["large"]['hash']}.webp"

        if formats_dict.get("medium"):
            image = Image.open(download_image(formats_dict["medium"]["url"]))
            image.save(
                os.path.join(BASE_DIR, f"./temp/{formats_dict["medium"]['hash']}.webp"), "webp", quality=70, optimize=True)
            s3_client.fput_object(
                os.getenv("AWS_BUCKET_NAME"),
                f"{formats_dict["medium"]['hash']}.webp",
                os.path.join(BASE_DIR, f"./temp/{formats_dict["medium"]['hash']}.webp"),
                content_type="image/webp",
            )
            formats_dict["medium"]["url"] = f"{new_bucket_url}/{formats_dict["medium"]['hash']}.webp"
            formats_dict["medium"]["mime"] = "image/webp"
            formats_dict["medium"]["ext"] = ".webp"
            formats_dict["medium"]["name"] = f"{formats_dict["medium"]['hash']}.webp"

        if formats_dict.get("small"):
            image = Image.open(download_image(formats_dict["small"]["url"]))
            image.save(f"./temp/{formats_dict["small"]['hash']}.webp", "webp", quality=70, optimize=True)
            s3_client.fput_object(
                os.getenv("AWS_BUCKET_NAME"),
                f"{formats_dict["small"]['hash']}.webp",
                os.path.join(BASE_DIR, f"./temp/{formats_dict["small"]['hash']}.webp"),
                content_type="image/webp",
            )
            formats_dict["small"]["url"] = f"{new_bucket_url}/{formats_dict["small"]['hash']}.webp"
            formats_dict["small"]["mime"] = "image/webp"
            formats_dict["small"]["ext"] = ".webp"
            formats_dict["small"]["name"] = f"{formats_dict["small"]['hash']}.webp"

        if formats_dict.get("thumbnail"):
            image = Image.open(download_image(formats_dict["thumbnail"]["url"]))
            image.save(f"./temp/{formats_dict["thumbnail"]['hash']}.webp", "webp", quality=70, optimize=True)
            s3_client.fput_object(
                os.getenv("AWS_BUCKET_NAME"),
                f"{formats_dict["thumbnail"]['hash']}.webp",
                os.path.join(BASE_DIR, f"./temp/{formats_dict["thumbnail"]['hash']}.webp"),
                content_type="image/webp",
            )
            formats_dict["thumbnail"]["url"] = f"{new_bucket_url}/{formats_dict["thumbnail"]['hash']}.webp"
            formats_dict["thumbnail"]["mime"] = "image/webp"
            formats_dict["thumbnail"]["ext"] = ".webp"
            formats_dict["thumbnail"]["name"] = f"{formats_dict["thumbnail"]['hash']}.webp"
            
            
        formats_json = json.dumps(formats_dict)

        cursor.execute("UPDATE files SET formats = %s WHERE id = %s", [formats_json, file_id])
        cursor.execute("COMMIT")
        # cursor.execute("COMMIT")
        print("Updated database")

    else:
        return


def main():
    try:
        s3_client = Minio(
            os.getenv("AWS_ENDPOINT"),
            os.getenv("AWS_ACCESS_KEY_ID"),
            os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        found = s3_client.bucket_exists(os.getenv("AWS_BUCKET_NAME"))

        if found:
            print("Bucket found")
        else:
            print("Bucket not found")
            return

        conn = connect(
            host=os.getenv("DATABASE_HOST"),
            database=os.getenv("DATABASE_NAME"),
            port=os.getenv("DATABASE_PORT"),
            user=os.getenv("DATABASE_USER"),
            password=os.getenv("DATABASE_PASSWORD"),
        )
        print("Connected to database")

        cur = conn.cursor()

        cur.execute("SELECT id,url,formats FROM files")
        print("Fetched files")
        files = cur.fetchall()
        
        print("Total files", len(files))

        for file in files:
            convert_to_webp(file[0], cursor=cur, s3_client=s3_client)
            # return
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)

    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
