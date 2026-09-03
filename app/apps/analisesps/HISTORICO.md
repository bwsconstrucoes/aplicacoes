# Análise de SPs — onde o trabalho está, decisões e o que falta

Este arquivo existe para uma sessão nova pegar o módulo sem repetir o que já
foi discutido. O `README.md` ao lado explica **como** ele é; este diz **em que
pé está e por quê**. Leia os dois antes de mexer; atualize este ao encerrar.

> **ATENÇÃO — esqueleto.** Este arquivo foi criado pela sessão do ERP em
> 02/09/2026 a partir do que o `README.md` diz, para o módulo ter o mesmo
> padrão de memória das outras áreas. **O chat que fez a conversão (no PC do
> dono) precisa preencher as seções marcadas "A PREENCHER" antes de ser
> abandonado** — é lá que está a história de verdade.

---

## Onde o trabalho está

Era um programa em Streamlit no computador do dono, lendo uma base local de
60 MB. Virou módulo Flask em `/analisesps`, com login próprio (dois perfis),
dados no Postgres e carga da planilha SPsBD em segundo plano. Tem duas
migrações próprias (`migracoes/001`, `002`), aplicadas por botão.

O Streamlit original continua em `app/analisesps/app/` e **ainda roda no PC do
dono** — não é importado pelo serviço.

**Estado em 02/09/2026:** três commits no dia (as sete telas restantes, a
conferência do Bradesco, exportação CSV/PDF). *A PREENCHER: versão publicada,
número de testes, o que foi conferido contra dado real.*

### O que está pendente AGORA

*A PREENCHER pelo chat da conversão.* Ao menos, pelo README:

- As migrações 001 e 002 já foram aplicadas no Render? A carga da planilha já
  rodou uma vez inteira?
- O dono já parou de usar o Streamlit no PC, ou os dois convivem?

---

## Regras que não se discutem

*A PREENCHER.* Herdadas do resto do repositório e valem aqui:

1. **Nada de abrir a base inteira em memória** — quem soma é o Postgres (o
   Streamlit segurava 162 MB por pessoa; foi o número que decidiu a conversão).
2. **Trabalho longo não roda dentro do gunicorn** (`executar_sync.py` é um
   processo separado). Publicar na `main` reinicia o serviço e mata a
   sincronização: **perguntar ao dono antes de juntar**.
3. **Autorização padrão NEGAR** e **a produção não é alcançável pelos testes**
   — mesmas regras do ERP.

---

## Decisões já tomadas

- **Blueprint no monorepo, não serviço separado** (motivo no README: memória
  por sessão do Streamlit e custo de um segundo serviço).
- **CSV, não `.xlsx`** — sem dependência nova sem combinar.

## O que ficou de fora, e por quê

- Cancelar SP no Pipefy e gerar BeeVale: ações sem volta, e o BeeVale depende
  de um Shared Drive (erro 403 de cota da service account).
- Enviar comprovante por e-mail: depende de SMTP no serviço.

## Incidentes

*A PREENCHER.*

## Coisas pequenas que mordem

- Um `app.py` solto dentro de `app/` sombreia o pacote `app` do projeto se a
  pasta cair no caminho de busca do Python (aconteceu no painel).
