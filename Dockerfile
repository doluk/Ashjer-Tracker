FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN uv pip install -r /app/requirements.txt --system

COPY . .

CMD ["python", "core.py"]