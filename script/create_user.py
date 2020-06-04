import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
import sys

# Creates a user to login into the Airflow UI
# Uses command line arguments to configure the user:
#   1st arg: username
#   2nd arg: email
#   3rd arg: password

user = PasswordUser(models.User())
user.username = sys.argv[1]
user.email = sys.argv[2]
user.password = sys.argv[3]
session = settings.Session()
session.add(user)
session.commit()
session.close()
