# ROTEIRO — ERP BWS

> Backlog vivo. Tudo que o Marcelo pediu ao longo das conversas fica registrado
> aqui para não se perder entre sessões e não precisar ser repetido.
> Marcar `[x]` quando entregue, com o commit. Atualizado a cada bloco.

## Princípios que valem para tudo

- **Amplitude, não recorte.** A solução tem que cobrir o caso real inteiro, não
  o exemplo mais fácil. (Ex.: leitura de documento é foto de celular e PDF ruim,
  não XML.)
- **Migração limpa**: sistema novo, sem herdar vícios do Omie. Nada de importar
  fornecedores/plano do Omie.
- **Nada sem categoria, nada sem obra.** Falta de correspondência vira crítica,
  nunca silêncio.
- **Dedutibilidade é do documento**, não da categoria: decidida depois pelo
  financeiro ou pela IA, com trava antes da conclusão.
- **Retenção é forma de liquidação**, não conta separada.
- **Facilidade de ajuste**: renomear conta, aposentar conta remanejando os
  lançamentos, sem tocar em título.
- **Encadeamento** (como o "conexão database" do Pipefy): clicar na obra, na
  conta, no credor, na compra e ir para o cadastro correspondente.
- **Visual**: linguagem dos painéis (topo com abas, filtros laterais, KPIs,
  tabela densa em cartão), com detalhe que expande como card.
- Trabalhar em blocos grandes e autônomos; avisar quando houver migração.

## Entregue

- [x] Núcleo: schema (21 tabelas), models, auditoria append-only — `eb1f457`
- [x] Regras de título, análise com score, alçadas, segregação, estorno
- [x] Boleto (linha digitável 47/48, DV, valor, vencimento) e OFX
- [x] Consulta de CNPJ na Receita no cadastro do credor (trava BAIXADA/INAPTA)
- [x] ERP dentro do monorepo Flask como blueprint `/erp` — `0976b8f`
- [x] Identidade visual dos painéis + Títulos, Configurações, Importar — `038e5b9`
- [x] Importador de cards do Pipefy (colar IDs/links) — `7ea049a`
- [x] Plano financeiro em 3 níveis, gravado no banco, com tributos unificados,
      aportes com direção, contas da reforma tributária — `7ea89a8`
- [x] Fundo fixo como conta (é processo, não meio de pagamento) + categorias
      editáveis com proteção contra sobrescrita — `0966da3`
- [x] Botão "Aplicar atualizações do banco" (ADMIN) — `4e9d0d3`
- [x] De-para do plano antigo → novo, com fila de pendências — `ab50e9a`
- [x] Lançamento categoria-first + crítica de duplicidade na entrada — `03a0faa`

## Em andamento

- [x] **Leitura ampla de documentos** — `a2da83c` — foto de celular, PDF ruim, múltiplas
      páginas, todos os tipos (NFe, NFSe de qualquer município, CT-e, recibo,
      RPA, guia DARF/GPS/FGTS/DAE, fatura de concessionária, boleto, contrato,
      termo de rescisão, comprovante bancário, prestação de contas de fundo fixo)
- [x] **Pagamentos**: agenda, baixa em lote, Pix copia-e-cola + QR
- [ ] Pagamentos — falta: comprovante anexado ao título e aviso a quem solicitou
- [x] **Lote**: prioridade, colar SPs da mensagem, acompanhar pago/não pago
- [x] **Conciliação automática de verdade** (atribuição ótima, valores
      repetidos, tarifas/transferências classificadas)
- [x] Tela de conciliação linha a linha por conta corrente, com ações
- [x] Movimentações entre contas (lançamento simples)
- [ ] Conciliação — falta: baixa por comprovante (PDF do banco, como o
      baixabradesco faz hoje) e detecção de transferência pelo comprovante
- [x] **Relatórios**: totais por 8 dimensões, DRE gerencial, analítico, CSV
- [ ] Relatórios — falta: exportação em PDF e gráficos

## Fila (pedidos registrados, ainda não iniciados)

- [ ] Detalhe do título que **expande como card**, com anexos e tudo que não
      cabe na tabela
- [ ] **Encadeamento**: obra → cadastro da obra; conta → plano; credor →
      cadastro; compra → pedido
- [ ] **Agenda do ERP**: calendário de obrigações com alerta para não esquecer
- [ ] **BeeVale**: geração das informações (existe no spsbd)
- [ ] **Auditoria**: as checagens do spsbd que ainda não vieram
- [ ] **Ratear**: rateio por categoria (rateio por obra já funciona no lançamento)
- [x] **Títulos a receber**: medição (nº, período, obra/contrato, retenções), baixa com várias notas fiscais
- [ ] **Cruzamento estilo tela do Bradesco**: conferir conta correta e credor do
      Pix contra o que foi lançado
- [ ] **Robô Bradesco**: adaptar para dar baixa via core do ERP
- [ ] **Suprimentos**: pedidos de compra, three-way match; etapa/serviço da obra
      entra aqui (dentro do centro de custo), não antes
- [x] Cadastro completo da obra: endereço (local de entrega), CNO, ART/RRT,
      contrato, vigência, data-base do reajuste, conta de recebimento,
      tributação (ISS/INSS/federais) e ADITIVOS de valor e prazo
- [x] Reclassificar conta/obra com título pago e conciliado, sem desfazer nada
- [x] Desfazer baixa+conciliação em um passo (o ritual do Omie em um clique)
- [ ] Obra — falta: anexos (contrato, CREA, seguro-garantia, OS) e fases de
      acompanhamento; alerta de data-base do reajuste na agenda
- [ ] Integrar com o módulo emissaonf: emitir a nota a partir da medição
- [ ] **Open Finance / API bancária**: extrato e DDA automáticos (futuro)
- [ ] Notificação a quem solicitou (WhatsApp/Telegram já existem no monorepo)
- [ ] Aposentar o `.env` commitado: trocar a senha do banco no Render

## Decisões registradas

| Assunto | Decisão |
|---|---|
| Projeto das obras | Só `CONSVALExLC` (4 obras) no ERP novo |
| Fornecedores | Cadastro próprio via consulta de CNPJ; nada do Omie |
| Plano financeiro | Novo, em grupos; tributos unificados; fundo fixo é conta |
| Etapa/serviço | Fica para suprimentos, dentro do centro de custo |
| Streamlit | Abandonado; interface HTML própria servida pelo Flask |
| Hospedagem | Dentro do serviço `aplicacoes` (sem custo novo) |
| Migrações | Botão no ERP (nunca no start do gunicorn) |
