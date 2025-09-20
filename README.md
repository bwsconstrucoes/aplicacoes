# Aplicações (Monorepo)

Este repositório unifica:
- **pdf-processor** (rotas: /compilar, /pdf2texto, /token-status)
- **encurtador** (rotas herdadas do app-bws: /encurtador/* e redirecionador global "/<codigo>")
- **painel** e **api** do encurtador, conforme seus arquivos originais

## Rodando localmente
1. Configure variáveis de ambiente (no Render ficam no painel; localmente um `.env` é opcional):
   - DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN
   - GOOGLE_CREDENTIALS_BASE64, GOOGLE_FOLDER_ID (opcional)
   - PORT (opcional)

2. Instale dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. Execute:
   ```bash
   python app/main.py
   ```

## Deploy no Render
- Use `Procfile` (gunicorn) ou defina Start Command = `gunicorn app.main:app`
- Adicione **Custom Domains** desejados ao MESMO serviço:
  - `link.bwsconstrucoes.com.br` (encurtador)
  - `pdf.bwsconstrucoes.com.br`  (pdf-processor)
- Mantenha as variáveis de ambiente no painel do serviço.

## Observações
- O arquivo `data/links.json` é utilizado pelas rotas administrativas do encurtador (`/admin/upload`, `/admin/download`).
