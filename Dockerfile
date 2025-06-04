FROM python:3.13

# System packages
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends bash supervisor curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# Update CA certificates to ensure the latest certificates are available
RUN update-ca-certificates

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Log file
RUN touch /app/log.txt && chmod 666 /app/log.txt

# Supervisor configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Add trusted hosts to pip configuration
RUN mkdir -p /etc/pip && echo "[global]\ntrusted-host = pypi.org\n files.pythonhosted.org\n pypi.python.org" > /etc/pip/pip.conf

# Copy the server's certificate into the container and add it to the trusted CA store
COPY atom.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
