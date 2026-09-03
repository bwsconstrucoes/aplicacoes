# Análise de SPs — onde o trabalho está, decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova pegar o módulo sem repetir o que já
foi discutido e sem repetir o que já deu errado. O `README.md` ao lado explica
**como** ele é; este diz **em que pé está e por quê**. Leia os dois antes de
mexer; atualize este ao encerrar a sessão.

> Reconstruído em 03/09/2026 pela sessão do ERP a partir das mensagens de
> commit do módulo (que são detalhadas) — o chat que fez a conversão, no PC do
> dono, foi fechado antes de escrever isto. O que estava só na conversa e não
> nos commits se perdeu; o que segue é o que os commits permitem afirmar.

---

## Onde o trabalho está

Era um programa em Streamlit no computador do dono, lendo uma base local de
60 MB (59.055 SPs). Virou módulo Flask em `/analisesps`, no serviço que já
existe, com login próprio, dados no Postgres do ERP em schema próprio
(`analisesps`) e carga da planilha SPsBD em processo separado.

**Feito em 02/09/2026, em quatro commits:**

1. `da41ae2` — primeira tela (Solicitações), login com dois perfis, migração
   001, carga em processo separado com retomada por etapas.
2. `a8c3b16` — as outras sete telas: Lote, Relatório, Auditoria, Ratear,
   Bradesco, Agenda, Log; mais a tela de códigos de pagamento (QR Pix e código
   de barras). Migração 002 (agenda, feriados, lote, listas do rateio).
3. `f687e66` — correção: um import "curto" herdado do Streamlit quebrava em
   silêncio a conferência do Bradesco (boleto de 47 dígitos nunca casava).
4. `f58d097` — exportação CSV em todas as telas que geram número de reunião,
   relatório e lote em PDF (fpdf2), ações direto na ficha da SP.

**Estado em 02/09/2026:** 611 testes verdes sem banco (297 deste módulo);
57 com banco rodam só no GitHub Actions. Conferido contra a base real: a soma
das SPs bate na casa do centavo com o Streamlit (R$ 250.061.950,39).

O Streamlit original continua em `app/analisesps/app/` — **não** é importado
pelo serviço e o dono ainda pode rodá-lo no PC.

### O que está pendente AGORA — perguntar ao dono

Nada disto consta nos commits; só o dono sabe:

- As migrações **001 e 002** foram aplicadas no Render (tela de Configurações
  do módulo)?
- As duas senhas (perfil Consulta e perfil Operador) foram definidas na
  Environment do Render? Sem elas ninguém entra — falha fechado.
- A **carga da planilha** já rodou inteira alguma vez online?
- O dono já parou de usar o Streamlit no PC, ou os dois convivem?

---

## Regras que não se discutem

### 1. Nada de abrir a base inteira em memória
Foi o número que decidiu a conversão: cada pessoa no Streamlit segurava
162 MB (pico 195 MB). Aqui quem soma é o Postgres; a tela recebe 200 linhas.
As somas do relatório e as sete checagens da auditoria são SQL.

### 2. Trabalho longo roda em processo separado, nunca dentro do gunicorn
`executar_sync.py`. O `--max-requests` do gunicorn matou três cargas do
painel por isso. **Publicar na `main` reinicia o serviço e mata a
sincronização em curso — perguntar ao dono antes de juntar.**

### 3. Nenhum módulo é importado pelo nome curto, e nada de pandas
Há teste que varre o pacote: import só pelo caminho completo; proibidos
`pandas`, `streamlit`, `numpy`, `altair`, `reportlab`, `openpyxl` (o serviço
não os tem); tudo que se importa tem de estar no `requirements.txt`.

### 4. Autorização padrão NEGAR
Rota que não declara o que exige é recusada. A resposta a uma escrita sem
alçada é sempre 403 — antes de qualquer outra checagem (defeito corrigido no
Lote).

### 5. Toda tela abre com recado quando o banco cai
Nunca 500. Há teste para as nove telas. Downloads leem o banco **antes** de
começar a mandar o arquivo — senão sai HTTP 200 com arquivo pela metade.

### 6. PDF é fpdf2 (não `fpdf` 1.7), e o texto passa por conversão para latin-1
O fpdf2 com fonte embutida estoura no meio da geração com travessão ou aspas
curvas. Todo texto é convertido antes, no mesmo caminho do emissaonf.

### 7. A produção não é alcançável a partir dos testes
Mesma regra do ERP e do painel.

---

## Decisões já tomadas — não reabrir sem motivo novo

- **Blueprint no monorepo, não serviço separado.** Um segundo serviço seria
  assinatura à parte, e o Streamlit não cabe em memória com quatro pessoas.
- **O lote é compartilhado** e a tela diz isso: duas pessoas veem o mesmo
  lote; a segunda a salvar sobrescreve, e a tela mostra quem salvou e quando.
- **Quatro módulos do Streamlit reaproveitados quase inteiros** (QR Pix/BR
  Code, código de barras, conferência do Bradesco, matemática do rateio) e a
  matemática de calendário da agenda sem alteração.
- **CSV, não `.xlsx`**; gráficos em CSS, sem biblioteca (o Plotly custava
  3 MB por tela).
- **Datas:** a conversão recupera 857 autorizações que apareciam vazias
  (1.664 SPs com data em duplicidade e quebra de linha) e recusa cinco com ano
  digitado errado (202, 203, 204, 260, 2925).

## O que ficou de fora, e por quê

- **Cancelar SP no Pipefy** e **gerar BeeVale**: ações sem volta, e o BeeVale
  depende de um Shared Drive (erro 403 de cota da service account).
- **Enviar comprovante por e-mail**: depende de SMTP no serviço.
- **Excel**: exigiria biblioteca nova.

## Incidentes

- **02/09 — import silencioso quebrado** (`f687e66`): o `bradesco.py`
  importava `pagamentos` pelo nome curto dentro de um try/except; a falha não
  aparecia e o boleto de 47 dígitos nunca casava com a SP.
- **02/09 — `fpdf` antigo no PC** mascarava diferenças com o `fpdf2` do
  serviço. Ambiente local foi alinhado.

## Coisas pequenas que mordem

- As telas **não foram vistas num navegador** pelo chat que as fez (a
  extensão não conectava). Foram geradas como HTML com dados de exemplo e
  conferidas estruturalmente. **A primeira navegação real é do dono.**
- Os 57 testes com banco só rodam no GitHub Actions. Sem `ERP_TEST_DATABASE_URL`
  são pulados.
