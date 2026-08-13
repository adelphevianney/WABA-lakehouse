# Application Streamlit de génération de données (Level 1.1).
# Le contexte de construction est la racine du dépôt : l'application dépend du
# paquet `common`, partagé avec les futurs jobs PySpark.
FROM python:3.12-slim

# Les dépendances sont installées avant la copie du code : une modification du
# générateur ne réinvalide pas la couche d'installation, qui est la plus lente.
WORKDIR /app
COPY generator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --requirement requirements.txt

COPY common/ ./common/
COPY generator/ ./generator/

# Exécution sans privilèges : le conteneur n'a aucune raison d'être root.
RUN useradd --create-home --uid 1000 waba && chown -R waba:waba /app
USER waba

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

EXPOSE 8501

HEALTHCHECK --interval=10s --timeout=5s --retries=12 --start-period=20s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "generator/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
