FROM python:3.13.13-slim-bookworm
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py direct_fetch.py public_profile.py ./
CMD ["python", "main.py"]
