<!-- Este é o README ORIGINAL do programa em Streamlit, que roda no computador
     do dono. Ele estava na pasta de cima e foi movido para cá, ao lado do
     código que descreve, quando o módulo virou blueprint do monorepo.
     O README da versão online está em ../README.md. -->

# Análise de SPs — Streamlit (substituição da aba *Relatório* / *Lote*)

Fonte de dados: aba **SPsBD** (`1lrP1HOvwqyXiVdP2kuTgG7sJjl2QXl0WT4lwkd392DA`).
Esta é a **Fase 1 (offline)**: já roda no seu computador com seus dados reais de exemplo,
com filtros, KPIs (Exibidos × Selecionados), aba **Lote** e **edição otimista**.

**Windows (mais simples):** dê duplo clique em **`Analise_SPs.bat`**. Na primeira
vez ele cria o ambiente virtual `.venv`, instala as dependências e abre o app no
navegador. Nas próximas vezes, só abre.

**Manual / outros sistemas:**
```
pip install -r requirements.txt
streamlit run app.py
```

Na primeira execução o app cria `spsbd_cache.db` a partir de `seed_spsbd.csv`.

---

## Credenciais (Google Sheets)

Coloque o JSON da **Service Account** como **`credenciais.json`** na pasta do app (pode ser o mesmo que você usa no Render). Depois compartilhe a planilha **SPsBD** com o **e-mail da Service Account**:

- **Fase 1 (offline, só leitura):** permissão de **Leitor** já basta.
- **Fase 2 (writeback):** promova para **Editor**.

O app procura as credenciais nesta ordem: variável `SPSBD_CREDENCIAIS` (caminho) → `credenciais.json` → `service_account.json`. Sem credenciais, ele roda com o seed de exemplo. Com `credenciais.json` presente e cache vazio, ele já faz a **carga real** da planilha no primeiro start.

> O `credenciais.json` está no `.gitignore` — não versione.

---

## Por que SPsBD (e não Log)

Conforme combinado, a nova solução usa **SPsBD** como fonte. As colunas de fórmula
**S, T, U, W** e **AC** foram descartadas; a **V** foi reaproveitada como coluna de
**carimbo** (timestamp da sincronização).
O mapeamento completo das 38 colunas (A..AL) → chave interna está em `schema.py` — é a
**fonte única de verdade**; mexer ali ajusta exibição, filtros, KPIs e sync de uma vez.

Colunas que você altera no dia a dia já estão marcadas como editáveis:
**O (Status Pgt)** e **AB (Agendado)**.

---

## A pergunta central: como manter atualizado sem baixar tudo

A estratégia (implementada em `gsheets.py` + `cache.py` + `apps_script_reconciliador.gs`):

1. **Reconciliador agendado (Apps Script).** Como a SPsBD é base de dados escrita
   **só por automações** (Python/Make/Apps Script via API), nenhum gatilho `onEdit`
   dispara. Então um gatilho *time-driven* (de minuto em minuto) lê a planilha,
   detecta as linhas que mudaram (assinatura por linha, guardada na aba oculta
   `_SyncHash`) e grava o timestamp na **coluna V**. **Não exige tocar nas suas
   automações.** (Código em `apps_script_reconciliador.gs`.)
2. **Leitura leve.** `sync_delta()` lê **só 2 colunas** — ID (A) e Carimbo (V) — da
   planilha inteira. É 1 requisição pequena, não importa quantas colunas existam.

3. **Diff.** Compara o carimbo de cada ID com o último guardado no cache:
   IDs novos ou com carimbo maior = mudaram; IDs que sumiram = foram excluídos.

4. **Busca em lote só do que mudou.** `batch_get` traz as 38 colunas **apenas** das
   linhas alteradas. Payload mínimo.

Resultado prático, igual ao que você pediu:
- **Quem está operando vê a própria mudança na hora** → `cache.editar_local()` grava
  no cache local imediatamente (otimista) e marca a linha como pendente.
- **Mudanças de outros** entram a cada ciclo de delta (sugestão: ~90s de auto-refresh).

> Sem o carimbo o app ainda funciona: faz **carga completa** (`bootstrap`). O carimbo
> é o que troca "baixar tudo" por "baixar só o delta".

---

## Estrutura

| Arquivo | Papel |
|---|---|
| `app.py` | UI Streamlit: abas Relatório, Lote e Sincronização |
| `schema.py` | Mapa das 38 colunas SPsBD (fonte única de verdade) |
| `cache.py` | Cache SQLite: upsert, leitura, **edição otimista**, diff de exclusão |
| `dados.py` | Cache → DataFrame tipado (valor BR→float, datas) + KPIs |
| `gsheets.py` | Conector SPsBD: `bootstrap()` + `sync_delta()` (carimbo) |
| `gerar_semente.py` | Gera `seed_spsbd.csv` a partir dos seus dados reais |
| `apps_script_reconciliador.gs` | Reconciliador agendado: carimba a coluna V das linhas que mudaram |

---

## Fase 1 (agora) × Fase 2 (com seus scripts)

**Já entregue (Fase 1, offline):**
- Filtros espelhando a *Pesquisa* (busca livre, status, conta, CC, tipo, responsável,
  forma, validação, período de vencimento, faixa de valor, ordenação).
- KPIs: Σ por Conta (Exibidos × Selecionados), Σ por Forma de Pagamento, buckets de
  agendamento (Agendar/Agendado/Pago/Falha).
- Grid denso com **seleção multi-linha** → alimenta os KPIs de "Selecionados".
- Aba **Lote**: cola IDs → popula o resto e calcula KPIs daquele conjunto.
- **Edição otimista** de Status Pgt / Agendado no cache local (instantânea).

**Fica para a Fase 2 (quando você passar os demais scripts):**
- Enviar as edições do cache de volta ao Sheets (hoje ficam como *pendentes*; ver
  `cache.pendentes_envio()` / `cache.marcar_enviados()`).
- Login multiusuário (esqueleto já previsto no `secrets.toml.example`).
- Visualização de rateio (depende de voltar a usar a aba Log ou uma fonte melhor).
- Demais ações em lote equivalentes aos botões da planilha.

---

## Ligar o modo online (quando quiser)

1. Coloque o JSON da Service Account como `credenciais.json` na pasta do app.
2. Compartilhe a planilha SPsBD com o e-mail da Service Account (Leitor na Fase 1; Editor na Fase 2).
3. Cole `apps_script_reconciliador.gs` no Apps Script da planilha e rode `instalarGatilho()` uma vez.
4. Abra o app: com `credenciais.json` presente, ele já faz a **carga completa** no 1º start.
   Depois use **Sincronizar (delta)** na aba **Sincronização** (ou o auto-refresh abaixo).

Para o delta automático a cada ~90s, adicione no topo do `app.py`:

```python
from streamlit_autorefresh import st_autorefresh   # pip install streamlit-autorefresh
st_autorefresh(interval=90_000, key="poll")
import gsheets
if gsheets.disponivel():
    gsheets.sync_delta()
```

---

## Próximo passo

Me mande os scripts de escrita / ações da planilha (mudança de status, agendamento,
etc.) para eu construir o `push_to_sheets()` e as ações em lote da Fase 2.
