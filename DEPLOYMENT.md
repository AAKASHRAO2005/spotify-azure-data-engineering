# Deployment Guide: Spotify Lakehouse Interactive Web Dashboard

This guide provides step-by-step instructions to deploy the **Spotify End-to-End Azure Data Engineering Interactive Dashboard** (`app.py`).

---

## 🚀 Option 1: Streamlit Community Cloud (Recommended, Free & Instant)

Streamlit Community Cloud is the fastest, free method to deploy directly from your GitHub repository.

### Step 1: Commit and Push Files to GitHub
Run the following in your terminal from the project root:
```bash
git add app.py requirements.txt Dockerfile docker-compose.yml .dockerignore .streamlit/
git commit -m "feat: add Streamlit web dashboard and deployment configuration"
git push origin main
```

### Step 2: Deploy on Streamlit Cloud
1. Go to **[share.streamlit.io](https://share.streamlit.io)** and log in with your GitHub account.
2. Click **"New app"**.
3. Fill in the repository details:
   - **Repository:** `AAKASHRAO2005/spotify-azure-data-engineering`
   - **Branch:** `main`
   - **Main file path:** `app.py`
4. Click **"Deploy!"**
5. Your live app URL will be generated within 1-2 minutes (e.g. `https://spotify-azure-lakehouse.streamlit.app`).

> **Note:** The application includes an auto-bootstrap mechanism that automatically generates simulated Medallion lakehouse data (Bronze, Silver, Gold marts) on its initial boot.

---

## 🐳 Option 2: Docker / Docker Compose (Local or Cloud Containers)

### Using Docker Compose (Single Command):
```bash
docker compose up -d
```
Access the dashboard at: `http://localhost:8501`

### Using Docker CLI:
```bash
# Build the Docker image
docker build -t spotify-lakehouse-app .

# Run container on port 8501
docker run -d -p 8501:8501 --name spotify_dashboard spotify-lakehouse-app
```

---

## ☁️ Option 3: Azure App Service / Azure Container Apps

Deploy the container directly onto Microsoft Azure:

### 1. Push Image to Azure Container Registry (ACR):
```bash
az acr login --name <your-acr-name>
docker tag spotify-lakehouse-app <your-acr-name>.azurecr.io/spotify-lakehouse:v1
docker push <your-acr-name>.azurecr.io/spotify-lakehouse:v1
```

### 2. Deploy Container App on Azure:
```bash
az containerapp create \
  --name spotify-lakehouse-dashboard \
  --resource-group rg-spotify-de-prod \
  --environment my-container-env \
  --image <your-acr-name>.azurecr.io/spotify-lakehouse:v1 \
  --target-port 8501 \
  --ingress external
```

---

## 🌐 Option 4: Render / Railway / Hugging Face Spaces

1. Create a new **Web Service** linked to your GitHub repo.
2. Set Environment / Runtime to **Python 3.11** or **Docker**.
3. Build Command: `pip install -r requirements.txt`
4. Start Command:
   ```bash
   streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
   ```
