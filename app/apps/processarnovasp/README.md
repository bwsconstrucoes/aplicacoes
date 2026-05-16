# 🚀 Módulo `processarnovasp` — Substituição do cenário Make.com

Substitui o cenário **`FIN - Solicitações Financeiro - SPs - Processar SP`**
(47 módulos) por uma única chamada HTTP ao Render.

> ⚠️ **Escopo**: Este módulo processa **apenas SPs de 1× parcela**.
> SPs parceladas continuam sendo processadas pelo cenário "Processar SPs Parceladas".

---

## 📂 Estrutura

```
app/apps/processarnovasp/
├── __init__.py            # exporta blueprint
├── routes.py              # POST /api/processarnovasp/executar
├── core.py                # orquestração das 3 rotas
├── payload_adapter.py     # converte payload Pipefy nested → estrutura plana
├── rateio.py              # cálculo Python (substitui planilha Bases Resumo)
├── lookups.py             # 4 lookups: CC, Tipo Despesa, ClientesOmie, SPsDDA
├── boleto.py              # validação cód. barras + SPsDDA
├── omie.py                # IncluirCliente + IncluirContaPagar
├── pipefy.py              # 5 mutations GraphQL
├── pedidos.py             # vinculação a pedido de compra
├── sheets.py              # 4 variantes de SPsBD + Log + FalhaProcessar
├── notify.py              # alertas Z-API
└── utils.py               # helpers genéricos
```

---

## 🌐 Registro no `app/main.py`

```python
from app.apps.processarnovasp import bp as processarnovasp_bp
app.register_blueprint(processarnovasp_bp, url_prefix='/api/processarnovasp')
```

---

## 🔐 Variáveis de ambiente (Render)

| Variável | Origem | Obrigatório |
|---|---|---|
| `PROCESSARNOVASP_SECRET`   | gerar string aleatória (≥32 chars) | ✅ |
| `GOOGLE_CREDENTIALS_BASE64`| já existe (Service Account JSON em b64) | ✅ |
| `PIPEFY_API_TOKEN`         | mesmo já usado em outros módulos | ✅ |
| `ZAPI_INSTANCE_ID`         | mesmo já usado em validasp | ⚠️ (só alertas) |
| `ZAPI_API_TOKEN`           | idem | ⚠️ |
| `ZAPI_CLIENT_TOKEN`        | idem | ⚠️ |
| `CHATBOT_MASTER_PHONE`     | default `5585987846225` | opcional |

---

## 📡 Contrato da API

### Request — formato Pipefy original (mantido pelo Make)

**POST** `https://aplicacoes.bwsconstrucoes.com.br/api/processarnovasp/executar`

```json
{
  "secret":              "<PROCESSARNOVASP_SECRET>",
  "omieAppKey":          "...",
  "omieAppSecret":       "...",
  "omieIdContaCorrente": "583772104",
  "id":  "1351647768",
  "url": "https://app.pipefy.com/open-cards/1351647768",
  "ia": {
    "Duplicidade": "Sem duplicidades.",
    "Categoria":   "Manutenção compatível.",
    "Descrição":   "Despesa de manutenção de veículo."
  },
  "fields": {
    "Selecione o Procedimento":           "Ordem de Pagamento",
    "Pagamento Futuro de Pedido":         "Não",
    "Antecipação ou Entrada de Pedido":   "Não",
    "Pessoa Física ou Jurídica?":         "Pessoa Jurídica",
    "Nome do Credor":                     "Fornecedor X Ltda",
    "CNPJ do Credor":                     "12.345.678/0001-99",
    "Tipo de Despesa":                    "Manutenção Veicular",
    "Tipo de Pagamento":                  "Boleto",
    "Valor Total da Despesa":             "1.000,00",
    "Ratear entre mais de um Centro de Custo?": "Sim",
    "Centro de Custo 1":                  "SOBRADINHO",
    "Centro de Custo 2":                  "CEIFOR2",
    "Valor Centro de Custo 1":            "600,00",
    "Valor Centro de Custo 2":            "400,00",
    "Data de Vencimento":                 "20/05/2026 10:00",
    "Status Vencimento":                  "Atende",
    "Vencimento Corrigido":               "",
    "Quantidade de Parcelas":             "1",
    "Número do Pedido":                   "PED-001",
    "Código de Barras":                   "00190000090375976...",
    "Responsável pela Solicitação":       "Maria - maria@bws.com",
    "Anuente":                            "João - joao@bws.com",
    "Banco do Pagamento":                 "Itaú"
  },
  "telemetria": {
    "executionId":   "exec-123",
    "scenario_name": "FIN - Processar SP (Render)"
  }
}
```

### Response (200)

```json
{
  "ok": true,
  "rota": "padrao",
  "secoes": {
    "telemetria": { "ok": true },
    "rateio":     { "ok": true, "saida": {}, "descritivo": {} },
    "boleto":     { "ok": true, "executado": true, "valido": true },
    "pedido":     { "executado": true, "pedidos_atualizados": 1 },
    "omie":       { "ok": true, "cliente": {}, "titulo": {} },
    "pipefy":     { "ok": true, "status": 200 },
    "spsbd":      { "ok": true, "rota": "padrao" },
    "log":        { "ok": true, "linhas_inseridas": 2 }
  },
  "response": "<HTML para Webhook Respond do Make>"
}
```

### Response (400) — SP parcelada

```json
{
  "ok": false,
  "erro": "SP 1351647768 tem 3 parcelas. Este endpoint processa apenas SPs de 1× — use o cenário \"Processar SPs Parceladas\".",
  "response": "<HTML de erro>"
}
```

---

## 🔀 Decisão de rota

| Procedimento | Pagamento Futuro / Antecipação | Rota |
|---|---|---|
| `Transferência de Recursos` | qualquer | `transferencia` |
| qualquer outro | `Sim` | `pagamento_futuro` |
| qualquer outro | `Não` | `padrao` |

---

## 🗂️ Tabelas de lookup (confirmadas no Drive em 16/05/2026)

| Tabela | Planilha | Aba | Chave | Valor |
|---|---|---|---|---|
| Centros de Custo | OmieApi  | `Base Centro de Custo` | A (nome) | **N** (Cód. Omie) |
| Tipos de Despesa | OmieApi  | `Base Tipo de Despesa` | **B** (Plano Financeiro) | **D** (Cód. Omie tipo T2.x.x) |
| Clientes Omie   | OmieApi  | `ClientesOmie` | A (CPF/CNPJ formatado) | B (cod_cliente_omie) |
| SPs DDA         | Principal| `SPsDDA` | B (cód. barras) | C (ID da SP) |

Configurado em `lookups.SHEETS_CONFIG` — alterar lá se a estrutura mudar.

---

## 🚢 Deploy

```bash
# 1. copiar arquivos
mkdir -p app/apps/processarnovasp
cp -r processarnovasp/*.py app/apps/processarnovasp/

# 2. registrar no main.py (ver acima)

# 3. variáveis no Render (settings → environment)
PROCESSARNOVASP_SECRET=<gerar>

# 4. commit & push
git add app/apps/processarnovasp app/main.py
git commit -m "feat: módulo processarnovasp — substitui cenário Make.com (47 módulos → 3)"
git push origin main

# 5. Render auto-deploy: ~2 min
```

---

## 📋 Migração do cenário Make.com

Importe `processarnovasp_blueprint_make.json` em um **novo** cenário no Make.
Ele tem apenas 3 módulos:

1. **Webhook** (hook 1943098 — o mesmo do antigo)
2. **HTTP** → POST para o Render com o payload Pipefy original
3. **Webhook Respond** → devolve o HTML estilizado para o Pipefy

> Crie a variável de organização `processarnovasp_secret` no Make com o mesmo valor
> de `PROCESSARNOVASP_SECRET` no Render.

**Antes de ativar o novo cenário, pause o antigo.**

---

## 🧪 Bateria de teste pós-deploy

Recomendado rodar 1 SP de cada cenário:

1. **Transferência de Recursos** (procedimento = `Transferência de Recursos`)
2. **Pagamento Futuro** (`Pagamento Futuro de Pedido = "Sim"`)
3. **Antecipação** (`Antecipação ou Entrada de Pedido = "Sim"`)
4. **Padrão sem boleto** (procedimento normal, sem `Código de Barras`)
5. **Padrão com boleto válido**
6. **Padrão com boleto duplicado** → deve criar card "Cancelar SP" no pipeline 301426645
7. **Padrão com fornecedor novo** → deve criar cliente no Omie
8. **Padrão com fornecedor já cadastrado** → deve usar código existente
9. **SP parcelada** → deve voltar HTTP 400 com mensagem clara
10. **Rateio múltiplo** (`Rateio Múltiplo entre Centros de Custo? = "Sim"`)

---

## 🔍 Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| HTTP 400 "Secret inválido" | `processarnovasp_secret` no Make ≠ `PROCESSARNOVASP_SECRET` no Render | Sincronizar valores |
| Distribuição Omie vazia | Lookup de Centro de Custo retornou vazio | Verificar nome do CC na aba `Base Centro de Custo` |
| `codigo_categoria` vazio | Lookup de Tipo de Despesa retornou vazio | Verificar nome da despesa na aba `Base Tipo de Despesa` (coluna B) |
| `naocadastrado` mas fornecedor existe | CPF/CNPJ na ClientesOmie tem formato diferente | Adapter normaliza só dígitos — deve funcionar; checar logs |
| SPsBD não escreve | Permissão da Service Account | Adicionar email da SA como editor da planilha |
| Pipefy 401 | `PIPEFY_API_TOKEN` ausente/expirado | Renovar token no Render |
