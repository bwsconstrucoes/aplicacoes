# Ponto eletrônico — PLANO da fase 2: gestão, app do colaborador, ocorrências, banco de horas e alertas

Escrito em 03/10/2026, a partir do pedido do dono no mesmo dia (resumo fiel):

> Precisa de um ambiente de **gestão** completo do ponto: ver os cadastros, consultar
> o ponto. E um ambiente **simples, para o celular**, onde quem bateu pode consultar
> o próprio ponto, a frequência, se tem falta, se tem atestado. **Lançar atestado**
> é pelo ponto: anexa e encaminha para aprovação de quem gerencia. **Banco de
> horas** não é para todo mundo — o pessoal de escritório pode ter, o de obra não.
> **Compensação**: trabalhar num dia em substituição a outro, e isso precisa ficar
> registrado. **Licenças**. E usar **inteligência** para gerar alertas que facilitem
> a gestão do ponto.

⚠️ Na mensagem ditada saiu "pessoal de obra não tem banco, mas pessoal de obra
tem". Entendido como **obra não tem, escritório tem** — e de qualquer forma o
banco fica marcado POR PESSOA, então a regra muda sem mexer em código.

**Já feito nesta rodada (não depende de decisão):** o cálculo do dia
(`core/apuracao.py`, 13 testes) — falta, atraso, saída antecipada, hora extra,
tolerância da CLT, intervalo, interjornada, hora noturna, 12x36 atravessando a
meia-noite, feriado, dia de descanso e abono por atestado/férias. Tudo o que vem
abaixo lê esse cálculo.

---

## 1. Os três ambientes

| Ambiente | Quem usa | Onde | O que faz |
|---|---|---|---|
| **Gestão do ponto** | DP, supervisor/encarregado de obra, diretoria | **dentro do ERP**, com o mesmo login, menu e permissões | painel do dia, espelho de cada pessoa, pendências para aprovar, cadastro do ponto (escala, jornada, banco sim/não, obras), aparelhos, feriados, banco de horas, relatórios, alertas |
| **Meu ponto** (celular) | cada colaborador, no celular dele | `/ponto/app`, instalável como aplicativo | bater ponto, ver as próprias batidas e a frequência do mês, faltas e saldo, enviar atestado (foto ou PDF), pedir ajuste ("esqueci de bater"), pedir compensação, comprovante de cada batida |
| **Tablet da obra** | colaborador sem celular | `/ponto/app` no modo compartilhado | só bater: CPF + foto. Não consulta nada — o aparelho é de todos |

**Por que a gestão fica dentro do ERP:** é regra da casa (`CONTEXTO.md` §7): área
nova de gestão entra no ERP, que já tem login, permissão por obra e auditoria.
**Mas o código fica na pasta do ponto**: as telas são do ponto, usam o login e a
moldura do ERP, e no ERP entram só o item de menu e os nomes das permissões. Assim
o chat do ERP e o do ponto não mexem nos mesmos arquivos.

## 2. O que cada tela mostra

### Gestão (ERP › Ponto)

1. **Hoje** — por obra: quem bateu, quem não bateu, quem está em análise, quem
   está de atestado/férias. É a tela do encarregado às 7h30.
2. **Espelho** — por pessoa e mês: cada dia com batidas, previsto, trabalhado,
   extra, débito, falta, abono e o porquê. Abre a foto e o mapa da batida.
3. **Pendências** — a fila de decisões: batidas em análise (fora da cerca, sem
   localização, relógio), pedidos de ajuste, atestados, licenças, compensações,
   aparelhos novos. Aprovar/negar com um toque, motivo obrigatório ao negar.
4. **Pessoas do ponto** — por colaborador (o cadastro continua sendo o do ERP):
   escala, jornada, banco de horas sim/não e de que tipo, obras adicionais,
   aparelhos dele.
5. **Escalas e feriados** — os horários (ex.: obra seg–qui 7–17, sex 7–16) e os
   feriados nacionais, estaduais e municipais por obra.
6. **Banco de horas** — saldo por pessoa, o que vence, o que foi compensado.
7. **Alertas** — ver §5.
8. **Relatórios** — espelho em PDF para assinatura, exportação para a folha.

### Meu ponto (celular)

Tela única, em blocos grandes: **Bater ponto** (com a localização e a foto);
**Hoje** (as batidas do dia e o que falta bater); **Meu mês** (dias trabalhados,
faltas, atrasos, extras e, se tiver, saldo de banco); **Enviar atestado**;
**Pedir ajuste**; **Pedir compensação**; **Meus pedidos** (pendente / aprovado /
negado, com o motivo). Cada batida tem o **comprovante** (exigência do REP-P).

## 3. Ocorrências — atestado, licença, férias, compensação, ajuste

Uma **ocorrência** é tudo o que justifica ou muda um dia. Toda ocorrência tem:
quem pediu, para quais dias, o tipo, o documento (no Drive, como as fotos), quem
aprovou e quando. Nada aprovado apaga batida: a batida original fica, e a
ocorrência diz como o dia passa a ser contado (Portaria 671).

| Tipo | Quem pede | Quem aprova (proposta) | Efeito no dia |
|---|---|---|---|
| Atestado médico | colaborador (app) ou DP | **DP** | abona o dia |
| Licença (casamento, luto, paternidade…) | colaborador ou DP | DP | abona |
| Férias | DP | DP | abona |
| Ajuste de batida ("esqueci de bater") | colaborador | **supervisor da obra** | inclui a batida como AJUSTADA |
| Batida em análise (fora da cerca etc.) | — (o sistema) | supervisor da obra | VALIDA ou REJEITADA |
| Compensação (trabalhar o sábado no lugar da sexta) | colaborador ou supervisor | supervisor + DP | o dia trabalhado e o dia folgado se anulam |
| Folga de banco de horas | colaborador | supervisor + DP | debita o banco, abona o dia |

⚠️ **Atestado é dado de saúde** (LGPD, art. 11). Proposta: só o DP vê o
documento e o CID; o supervisor vê "afastado por atestado" e os dias, nunca o
papel.

## 4. Banco de horas e compensação

Por pessoa, um destes regimes:

| Regime | Para quem (proposta) | Regra |
|---|---|---|
| **Sem banco** | obra | extra é paga no mês; débito é descontado |
| **Compensação no mês** (art. 59, §6º) | qualquer um, por acordo | o que sobra num dia compensa outro **no mesmo mês**; o que não compensar é pago/descontado |
| **Banco de 6 meses** (art. 59, §5º) | escritório | exige **acordo individual escrito** (fica anexado no cadastro) |
| **Banco de 12 meses** (art. 59, §2º) | só se a convenção coletiva permitir | exige acordo/convenção coletiva |

O sistema lança cada crédito e débito com a data, avisa antes de vencer e não
deixa usar banco sem o acordo anexado.

## 5. Alertas — regra primeiro, inteligência depois

**Regra (código testado, número exato):** falta sem justificativa; batida
faltando no dia; atraso e saída antecipada fora da tolerância; extra acima de 2 h;
intervalo curto; menos de 11 h entre jornadas; trabalho em feriado/descanso;
batida fora da cerca; pessoa ativa sem batida há N dias; banco de horas vencendo
ou alto demais; atestado esperando aprovação há mais de 2 dias; aparelho novo
pendente; **sinais de fraude** — o mesmo aparelho batendo para muitas pessoas em
sequência, batidas sempre no limite da cerca, celular de uma pessoa batendo por
outra.

**Inteligência artificial (o que ela faz bem, sem inventar número):**
- **lê o atestado** que chegou e preenche dias, médico, CRM e data — a pessoa do
  DP confere, como a leitura de contrato do ERP já faz;
- **resumo diário** para o gestor por WhatsApp: "Obra X: 3 faltas, 2 atestados
  para aprovar, o João bateu fora da cerca três dias seguidos";
- **padrões que regra não pega**: a pessoa que começou a atrasar toda segunda,
  a obra cuja frequência caiu.

A IA **nunca** calcula saldo nem decide aprovação: ela aponta, gente decide
(decisão da casa de 18/09/2026, "o sistema sugere; quem manda é gente").

## 6. Como o colaborador entra no "Meu ponto"

O aparelho já tem token (fase 1) — isso basta para **bater**. Para **consultar e
pedir**, precisa saber que é aquela pessoa. Proposta: **CPF + PIN de 6 dígitos**,
criado no primeiro acesso com um código enviado por **WhatsApp** para o telefone
do cadastro do ERP (o gateway de WhatsApp já existe). Esqueceu o PIN, pede outro
código. Sem e-mail, sem senha complicada — pessoal de obra usa.

## 7. Ordem de construção (cada etapa é usável sozinha)

| Etapa | O que entrega | Depende de |
|---|---|---|
| **2A** | escalas, feriados e espelho na gestão; painel "Hoje"; pendência das batidas em análise e dos aparelhos | escalas reais (§8, item 1) e o "pode" para o menu no ERP (item 2) |
| **2B** | app do colaborador: bater, hoje, meu mês, comprovante | 2A; login por PIN (item 4) |
| **2C** | ocorrências: atestado, licença, férias, ajuste, com anexo e aprovação | 2B; quem aprova (item 3) |
| **2D** | banco de horas e compensação | 2C; regras do banco (item 5) |
| **2E** | alertas por regra, resumo diário no WhatsApp, leitura de atestado por IA | 2C |
| **piloto** | uma obra batendo nos dois sistemas (Mobponto e o novo) por um mês, comparando | 2B |
| **3** | AFD/AEJ, iDFace, a folha da Análise de SPs lendo daqui, desligar o Mobponto | piloto aprovado + contabilidade |

## 8. O que só o dono decide (com a proposta para seguir se ele disser "segue")

1. **Os horários de verdade.** Quais escalas existem (obra, escritório, vigias) e
   em que horário. *Proposta:* o DP manda a lista; até lá o sistema nasce com o
   cadastro vazio e o espelho mostra "sem escala" em vez de inventar falta.
2. **A gestão dentro do ERP**, com o código na pasta do ponto e só o menu e as
   permissões no ERP. *Proposta:* sim — é mexer fora do módulo, por isso pergunto.
3. **Quem aprova o quê** — a tabela da §3. *Proposta:* como está lá.
4. **Entrada do colaborador por CPF + PIN com código no WhatsApp.** *Proposta:* sim.
5. **Banco de horas** — quem tem, e se há acordo individual ou convenção coletiva
   da construção que trate disso. *Proposta:* obra sem banco, escritório com banco
   de 6 meses por acordo individual — **a confirmar com a contabilidade**, porque a
   convenção coletiva pode mudar tolerância, banco e intervalo.
6. **Atestado só o DP vê** (§3). *Proposta:* sim.
7. **A obra do piloto.**
