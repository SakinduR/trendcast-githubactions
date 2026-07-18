FROM jupyter/datascience-notebook:latest

EXPOSE 8888

ENV JUPYTER_TOKEN=password

RUN pip install --no-cache-dir psycopg2-binary

CMD ["start-notebook.sh", "--ServerApp.ip=0.0.0.0", "--ServerApp.port=8888", "--ServerApp.open_browser=False", "--ServerApp.token=password", "--ServerApp.allow_origin=*", "--ServerApp.allow_remote_access=True"]
