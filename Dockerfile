# linux/amd64 candidate. Both runtime image and Xray release archive are pinned.
FROM --platform=linux/amd64 python:3.12-alpine@sha256:2055081c860db8c842b663415b795b026e7e5cc63b59c5193fcc9997713ea5b8
WORKDIR /app
COPY fetch_xray.py /app/fetch_xray.py
RUN python /app/fetch_xray.py /usr/local/bin/xray && rm /app/fetch_xray.py
COPY entrypoint.py /app/entrypoint.py
RUN chmod 0555 /app/entrypoint.py /usr/local/bin/xray
USER 65532:65532
ENV PORT=8080 WS_PATH=/video-ws GOMEMLIMIT=128MiB PYTHONDONTWRITEBYTECODE=1
EXPOSE 8080/tcp
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 CMD python -c "import os,socket;s=socket.create_connection(('127.0.0.1',int(os.environ.get('PORT','8080'))),2);s.close()"
ENTRYPOINT ["python", "/app/entrypoint.py"]
