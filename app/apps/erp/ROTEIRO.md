# ROTEIRO — ERP BWS

> O sistema é um ERP, não um financeiro: cada área é um MÓDULO próprio
> (Financeiro, Obras, Administração), com suas telas. Novos módulos previstos:
> Suprimentos, Contratos/Medições (emissão de nota), Pessoal, Agenda.

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
- [x] Baixa por COMPROVANTE: lê o PDF/foto do banco, acha o título e anexa o
      documento — anexos ficam NO BANCO, comprimidos (sem Dropbox)
- [x] Operadores com perfis e escopo por obra (administrativo de obra,
      supervisor, gestor, administrativo financeiro, admin)
- [x] Pagamentos — aviso a quem solicitou, por TELEGRAM — `e35f00f`, estendido
      aos interessados em `f5a7957` (`core/notificacoes.py`)
- [ ] Pagamentos — falta: a metade do WhatsApp. O `app/apps/notificador.py` já
      tem `notificar()`, que cobre os dois canais; hoje `core/notificacoes.py`
      chama só `enviar_telegram`
- [x] **Lote**: prioridade, colar SPs da mensagem, acompanhar pago/não pago
- [x] **Conciliação automática de verdade** (atribuição ótima, valores
      repetidos, tarifas/transferências classificadas)
- [x] Tela de conciliação linha a linha por conta corrente, com ações
- [x] Movimentações entre contas (lançamento simples)
- [x] Movimentação NEUTRA: dinheiro que entrou/saiu por engano e foi
      devolvido/ressarcido — o par se anula e não entra em relatório algum;
      ponta sem contraparte fica cobrada na conciliação; contas 9.1.02
      (valores de terceiros) e 9.1.03 (pagamento por conta errada), cada uma
      servindo a entrada e a saída
- [x] Conciliação — baixa por comprovante e reconhecimento de tarifa/
      transferência pelo comprovante
- [x] **Relatórios**: totais por 8 dimensões, DRE gerencial, analítico, CSV
- [ ] Relatórios — falta: exportação em PDF e gráficos

## Fila (pedidos registrados, ainda não iniciados)

- [x] Painel de consumo de IA (tokens, custo, por operação/modelo/pessoa)
- [x] Conversão de valores decimais corrigida (30.00 vs 1.234)

- [x] LOCAÇÕES (núcleo): insumos com marca de locável, contrato com itens,
      previsão de parcelas, devolução parcial, remanejo entre obras, alertas
      de aluguel × compra, painel por obra
- [x] Locações: leitura do contrato por IA (aproxima locadora e equipamentos
      contra os cadastros) e MAPA das obras, equipamentos e volume por região
- [x] Empreita com vários serviços (planilha de orçamento) e medição por item
- [ ] Cadastro de insumos como base de SUPRIMENTOS (categoria de insumo +
      conta do plano); mapa das obras e do volume financeiro por região

- [x] EMPREITAS: contrato com saldo, medição consumindo o saldo, foto
      obrigatória, adiantamento abatido, aditivo e geração do título na
      autorização — núcleo e tela
- [x] Bloqueio de período (diretor fecha e destrava janela temporária)
- [ ] Empreita — falta: retenção de garantia (5% por medição, liberada no fim)
      e alçada por valor de contrato

- [x] Categorias PERMITIDAS por operador (administrativo de obra não vê o plano
      inteiro) — seleção simples no cadastro do operador
- [x] Rateio (obra e categoria) editável na reclassificação do título aberto
- [x] IA sugerindo a categoria a partir da descrição do documento, marcada
      como sugestão para o usuário validar
- [x] Guardar a chave de acesso da NFe no título e cruzar CNO/endereço da nota
      com o cadastro das obras para sugerir o centro de custo

- [x] FUNDO FIXO: prestação de contas com itens, comprovante por linha,
      adiantamento × reembolso, histórico do solicitante e críticas antifraude
- [x] Alçada de fundo fixo POR PESSOA (limite por despesa, por prestação,
      autorização e saldo de adiantamento) no cadastro de operadores
- [x] CARTÃO DE CRÉDITO: importa a fatura em PDF, extrai as compras, exige
      obra e categoria por linha
- [x] Rateio de obra obrigatório no lançamento
- [x] Críticas chegam a quem aprova: a tela Confirmar destaca os apontamentos,
      exige análise item a item e só então libera a assinatura; o financeiro e
      o diretor têm fila própria de prestações com indício
- [x] Módulo PESSOAL: colaboradores (cadastro enxuto) e Despesas com
      Colaborador em lote, com cadeia supervisor → DP → diretor, críticas de
      repetição e geração do título rateado — feito a partir das estruturas dos
      pipes Cadastro de Colaboradores (301487297) e Despesas com Colaboradores
      (301433085)
- [x] Todo pagamento de pessoa atrelado ao colaborador (DC, título direto e
      parte em guia coletiva) com ficha de histórico consolidado
- [ ] Pessoal — falta: formato exato do arquivo BeeVale/SomaPay (aguarda
      instrução) e anexo da planilha ao título

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
- [ ] **Suprimentos**: pedidos de compra, three-way match (o ERP deixa de ser só financeiro); etapa/serviço da obra
      entra aqui (dentro do centro de custo), não antes
- [x] Cadastro completo da obra: endereço (local de entrega), CNO, ART/RRT,
      contrato, vigência, data-base do reajuste, conta de recebimento,
      tributação (ISS/INSS/federais) e ADITIVOS de valor e prazo
- [x] Reclassificar conta/obra com título pago e conciliado, sem desfazer nada
- [x] Desfazer baixa+conciliação em um passo (o ritual do Omie em um clique)
- [x] Anexos genéricos (obra, título, fornecedor, movimentação) no banco
- [x] GESTÃO DE OBRAS como área própria (aba Obras): painel com contrato
      vigente, recebido, gasto e saldo; fases com histórico; contrato e
      aditivos; tributação com simulador; documentos; movimento; auditoria
- [ ] Obra — falta: alerta de reajuste na agenda (o painel já sinaliza)
- [ ] Integrar com o módulo emissaonf: emitir a nota a partir da medição
- [ ] **Open Finance / API bancária**: extrato e DDA automáticos (futuro)
- [x] Aviso quando o título é pago, via TELEGRAM, com o comprovante junto —
      idempotente por pessoa, marcando correção quando valor/data mudam
- [x] INTERESSADOS: quem lança escolhe outras pessoas para receberem os avisos;
      a obra pode ter interessados fixos que entram em todo título dela
- [x] Histórico completo por título na tela + histórico de QUALQUER cadastro
      (obra, fornecedor, categoria, movimentação) e consulta ampla de auditoria
- [x] AVAL em duas pessoas: lançamento de administrativo de obra/escritório
      trava até a assinatura de supervisor, gestor ou diretor financeiro
- [x] Perfil DIRETOR_FINANCEIRO; dados de pagamento ocultos para
      administrativo de obra
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
