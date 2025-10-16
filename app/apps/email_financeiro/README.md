# Módulo Email Financeiro - BWS Construções

## Endpoints
- `/api/email_financeiro/run` → executa coleta e parsing
- `/api/email_financeiro/status` → painel de status e métricas

## Funcionalidades
- Multi-conta IMAP
- Filtro de anexos relevantes (PDF/XML)
- Extração financeira via regex, PDF e XML
- OCR automático
- Cross-validation PDF/XML
- Upload Google Drive + link público
- Gravação no Google Sheets (abas Emails, Relatório, Runs)
- Painel com total de valores (R$)

## Deploy (Render)
1. Colar pasta `email_financeiro` em `app/apps/`
2. Variáveis:
   - `GOOGLE_CREDENTIALS_BASE64`
   - `SPREADSHEET_ID=14vu9ey21PVWKE9diNRKTyzUHSZIFxKoA`
   - `GDRIVE_FOLDER_ID=1JCGWsdCSrT5E2muXvOlWJlJEzd7l_pXr`
   - `TZ=America/Fortaleza`
3. Executar:
   ```bash
   git add .
   git commit -m "feat(email_financeiro): módulo completo de coleta e parsing financeiro"
   git push
