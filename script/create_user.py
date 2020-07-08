import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import os

# Creates a user to login into the Airflow UI
# Uses command line arguments to configure the user:
#   1st arg: username
#   2nd arg: password

user = PasswordUser(models.User())
user.username = os.getenv("AUTHENTICATION_USERNAME")
user.password = os.getenv("AUTHENTICATION_PASSWORD")
user.superuser = True
session = settings.Session()
session.add(user)
session.commit()
session.close()
