FROM python:3.6-alpine
RUN apk add gcc musl-dev python3-dev libffi-dev openssl-dev && \
    pip install cryptography --no-binary cryptography
COPY ./create_fernet_key.py /
CMD ["python3"]
