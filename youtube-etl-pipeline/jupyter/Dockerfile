# =============================================================================
# Jupyter Dockerfile
# Base: jupyter/datascience-notebook:latest
# Adds: psycopg2, kafka-python, seaborn for the analysis notebook
# =============================================================================

FROM jupyter/datascience-notebook:python-3.11

# Switch to root for system-level installs
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the default Jupyter notebook user (jovyan)
USER ${NB_UID}

# Install additional Python packages on top of the base image
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    kafka-python==2.0.2 \
    google-api-python-client==2.120.0 \
    seaborn==0.13.2 \
    plotly==5.19.0 \
    SQLAlchemy==2.0.27 \
    python-dotenv==1.0.1 \
    ipywidgets==8.1.2

# Enable JupyterLab by default
ENV JUPYTER_ENABLE_LAB=yes

# Copy notebooks into the default work directory
COPY notebooks/ /home/jovyan/work/

# Expose the notebook port
EXPOSE 8888

# Inherited CMD from base image: start-notebook.sh
