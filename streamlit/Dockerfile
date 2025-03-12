# Utiliser l'image officielle Python
FROM python:3.9

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers
COPY app.py requirements.txt ./

# Installer les dépendances
RUN pip install -r requirements.txt

# Exposer le port (mettre 7860 pour Hugging Face)
EXPOSE 7860

# Désactiver le mode interactif Streamlit pour Hugging Face
ENV STREAMLIT_SERVER_HEADLESS true

# Lancer l'application sur le bon port pour Hugging Face
CMD ["streamlit", "run", "app.py", "--server.port=7860", "--server.address=0.0.0.0"]
