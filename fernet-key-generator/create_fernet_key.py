from cryptography.fernet import Fernet
import os

os.makedirs('/tmp/fernet_key', exist_ok=True)

KEY_PATH = '/tmp/fernet_key/fernet_key.env'

if os.path.exists(KEY_PATH):
    print(f"File {KEY_PATH} exists already. Exiting without creating a new key")
    exit()

key = Fernet.generate_key()
key = key.decode()
with open(KEY_PATH, 'w') as f:
    f.write(f"export FERNET_KEY={key}")
    print(f"Fernet key created and stored in {KEY_PATH}")
