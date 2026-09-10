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
- [x] Empreita — retenção de garantia e alçada por valor: FEITO em 09/09/2026
      (migração 052). A retenção sai de cada medição, fica gravada NELA (mudar
      o percentual não reescreve o passado) e a devolução vira título a pagar,
      uma vez só. Quem aprova o contrato depende do valor: até 50 mil o
      supervisor, até 200 mil o gestor, acima disso só a direção — faixas
      editáveis em tabela.

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

- [x] Detalhe do título que **expande como card** — FEITO em 10/09/2026. Um
      clique na linha abre a ficha embaixo dela, com apontamentos, parcelas,
      pagamentos, rateio, retenções, anexos, assinaturas e histórico. A janela
      ficou com os formulários e com o endereço direto (`?titulo=N`) que cai
      num título fora dos filtros de hoje.
- [x] **Encadeamento** — FEITO em 10/09/2026. Obra, conta do plano, credor e
      compra viram link, na lista e na ficha, e as quatro telas de destino
      abrem já no registro. O elo só aparece para quem pode abrir o destino —
      link que responde "sem permissão" promete porta que não abre.
- [x] **Agenda do ERP** — FEITA em 09/09/2026 (migração 051). Obras › "Agenda":
      aniversário de reajuste, conferência mensal de locação, vencimento de
      certidão e fim da vigência do contrato, num lugar só, mais a anotação
      manual. O aviso deduzido é RECALCULADO (some quando deixa de valer);
      resolvido, dispensado e anotação nunca somem. O número aparece na porta
      de entrada. **Os cinco avisos estão ligados** — o certificado digital
      entrou em 09/09/2026 com a migração 053.
- [ ] **BeeVale**: geração das informações (existe no spsbd)
- [ ] **Auditoria**: as checagens do spsbd que ainda não vieram
- [ ] **Ratear**: rateio por categoria (rateio por obra já funciona no lançamento)
- [x] **Títulos a receber**: medição (nº, período, obra/contrato, retenções), baixa com várias notas fiscais
- [ ] **Cruzamento estilo tela do Bradesco**: conferir conta correta e credor do
      Pix contra o que foi lançado
- [ ] **Robô Bradesco**: adaptar para dar baixa via core do ERP
- [ ] **Suprimentos**: pedidos de compra, three-way match (o ERP deixa de ser só
  financeiro); etapa/serviço da obra. **Especificação fechada em 04/09/2026 —
  ver `SUPRIMENTOS.md`**, que confronta o ditado do dono com os dados reais das
  planilhas em uso e traz o plano em cinco fases. Nada construído ainda.
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
- [x] Obra — alerta de reajuste na agenda: FEITO em 09/09/2026
- [x] ~~Integrar com o módulo emissaonf~~ — **RETOMADO em 09/09/2026 pelo
      dono**, com desenho próprio: ver `MEDICOES_E_NOTAS.md` e a fila acima.
      Deixou de estar "em espera".
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
- [x] SEGURANÇA: autorização com padrão NEGAR — toda rota declara a ação que
      exige e o que não declara é recusado; o detalhe de um registro passa a
      respeitar o mesmo escopo da listagem, sem exceção entre obras. Fecha as
      brechas de anexo (baixar/apagar qualquer arquivo por id sequencial),
      dados bancários da parcela, lista de avisos do título, cancelamento em
      lote, plano de contas, painéis financeiros, empreitas e locações
- [x] Consistência transacional: travas de linha nas quatro operações de risco
      e restrições únicas no banco (migração 031). Conciliação disputada vira
      recado (409), não erro
- [x] Homologação por perfil — a parte mecânica virou teste com banco
      (`tests/test_homologacao_banco.py`, 393 casos); sobra o olho humano
- [ ] Segurança — falta: navegar em homologação com um usuário de cada perfil.
      Os testes cobrem a RECUSA (que é o lado que importa); o caminho de quem
      TEM acesso precisa de banco e não está coberto. Roteiro pronto em
      `HOMOLOGACAO_PERFIS.md`; operadores de teste em
      `scripts/seed_usuarios_teste.py`. Falta rodar
- [x] Segurança — DECIDIDO: não é regra do cargo, é configuração por pessoa. O
      cadastro do operador escolhe entre "só os meus lançamentos" e "tudo das
      obras designadas" (migração 029). Padrão do que já existe: o mais
      restritivo. Vale igual para listagem e detalhe, porque os dois passam
      pelo mesmo `aplicar_escopo`
- [x] Aposentar o `.env` commitado: senha do banco trocada no Render e usuário
      antigo apagado (2026-09-02). O `.gitignore` foi reescrito — estava com
      marcadores de conflito de merge dentro dele
- [x] IA — consumo registrado de verdade: as cinco operações (leitura de
      documento, comprovante do fundo fixo, fatura de cartão, contrato de
      locação, sugestão de conta) mais o comprovante de pagamento passam pelo
      ponto único do leitor e gravam em `ia_uso`. A função de registro não
      existia e o painel não tinha lugar na tela (2026-09-02)
- [x] IA — teto mensal configurável em Configurações (migração 030) com aviso
      no Telegram aos ADMIN ao passar de 80% e ao estourar. Só avisa
- [x] Testes com banco de verdade para o escopo por obra e por autoria, num
      Postgres descartável que o GitHub Actions sobe a cada envio
      (`tests/test_escopo_banco.py`, trava de URL no `conftest`)
- [ ] IA — depois da migração 030: definir o teto na tela e conferir que o
      painel enche conforme os documentos são lidos
- [ ] Trocar na origem o token da prefeitura que ficou no histórico do Git
      (commit `fa985ab`); o código já lê de `EL_NFSE_TOKEN`
- [ ] **PENDENTE DE JUNÇÃO — ramo `claude/oi-vjvrn8`** (2026-09-02). O dono
      preferiu segurar enquanto o sistema está em construção. Contém: escopo do
      operador por pessoa (migração 029), roteiro de homologação, operadores de
      teste, credencial fora do código e `.gitignore` consertado. Ao juntar,
      lembrar de: (1) definir `EL_NFSE_TOKEN` na Environment do Render, senão o
      script de consulta de NFS-e para; (2) apertar "Aplicar atualizações do
      banco" em Configurações — juntar o código NÃO aplica a migração

### Medições, contrato e emissão de nota — pedida pelo dono em 09/09/2026

O lado do que a BWS RECEBE. Especificação inteira em `MEDICOES_E_NOTAS.md`.
Substitui o processo "Protocolos e Medições" do Pipefy, que existe porque o
Omie não dá conta disso.

- [x] 1. **O cadastro que destrava tudo** — FEITO em 09/09/2026 (migração 047): expor na tela da obra os campos
      fiscais que JÁ existem no modelo (CNO, alíquota de ISS, ISS retido,
      regime, conta de recebimento); **chave Pix** na conta bancária; filtro
      **por conta** nas telas de título — pedido dele: "às vezes é mais fácil
      do que filtrar por obra"; e **os dados de emissão POR EMPRESA**
      (município, endereço do serviço, token, alíquota, modo API ou MANUAL).
      ⚠️ Este último entra aqui e não no passo 6: o dono confirmou em
      09/09/2026 que são DUAS empresas, em municípios diferentes, uma por API e
      outra manual — deixar para o fim faria a segunda não emitir.
- [x] 2. **A medição completa** — FEITO em 09/09/2026 (migração 049). Tipo
      EDITÁVEL em tabela (normal, reajuste, aditivo, subsidiária, complementar),
      número em TEXTO LIVRE, correlação entre a medição e a de reajuste dela
      (funciona nos dois jeitos de numerar: "1R" e "medição 3"), e protocolo com
      número e data. O sistema recusa reajuste de si mesma, de outro contrato e
      de reajuste.
- [x] 3. **O quadro financeiro do contrato** — FEITO em 09/09/2026. Tela nova
      em Obras › "Contratos e medições": uma linha por medição e os totais
      (contratado, aditivado, vigente, medido, faturado, recebido, a receber,
      saldo). MEDIDO ≠ FATURADO ≠ RECEBIDO em três colunas separadas, e o
      reajuste NÃO consome saldo do contrato. O indicador de dias entre
      protocolar e receber (item 7) já nasce aqui.
- [x] 4. **Tela de controle de notas emitidas** — FEITO em 09/09/2026.
      Financeiro › "Notas emitidas": cada tributo em sua coluna (ISS, IR, INSS,
      PIS, COFINS, CSLL), líquido, recebido com data e conta, conferência da
      numeração (buraco ≠ queimado), registro da nota que saiu pelo portal da
      prefeitura, cancelamento com motivo, e exportação em Excel e PDF.
- [x] 5. **Emissão a partir da medição, modo MANUAL** — FEITO em 09/09/2026.
      Botão "Emitir nota" em cada medição do quadro do contrato: o ERP monta o
      bloco com prestador, tomador, discriminação e as retenções JÁ CALCULADAS
      pelo cadastro da obra; a pessoa copia, emite no portal, volta e anexa o
      PDF — a IA lê e preenche número, data, valor e retenções.
- [x] 6. **Emissão automática** — FEITA em 10/09/2026 (migração 056). Botão
      "Emitir agora" na medição: o ERP confere o cadastro, reserva o número da
      declaração (que é NOSSO), assina com o certificado A1 da empresa **em
      memória** — o .pfx nunca vira arquivo em disco —, manda pelo canal
      NACIONAL e guarda o número da nota, a chave de acesso e o XML.
      Falhando, o número fica QUEIMADO com o motivo e não se recicla. Roda na
      fila (migração 055) porque a prefeitura leva até dois minutos, e **não
      se repete sozinha**: emitir duas vezes criaria duas notas de verdade.
      ⚠️ **A primeira conversa real com a prefeitura só acontece no Render.**
      O que sobrou do texto antigo, para contexto: **Petrolina saiu da conta em 10/09/2026**:
      o dono avisou que é empresa FUTURA. O que sobra é a emissão automática da
      BWS no Eusébio, que já tem inscrição, token e certificado — e cuja
      primeira chamada real só acontece no Render, porque a saída de internet
      do ambiente de desenvolvimento é filtrada.
      Com município e endereço virando configuração por empresa. ⚠️ Apontar para o **canal NACIONAL**, não para o ABRASF: a
      LC 214/2025 tornou o padrão nacional obrigatório e o ABRASF tem data para
      acabar. O `el_nfse_nacional.py` já fala esse padrão.
- [~] 7. **Indicadores**: dias entre protocolar e receber. Já pronto POR
      CONTRATO, dentro do quadro (média, mais rápida, mais lenta, e as que estão
      esperando há mais tempo). Falta o corte por OBRA e por ÓRGÃO numa tela
      só — o cálculo já aceita o filtro por obra.
- [x] 8. **REAJUSTE** — FEITO em 09/09/2026 (migração 050). A data-base é campo
      do contrato, com a origem escrita (orçamento, proposta, assinatura); o
      direito nasce depois da periodicidade (12 meses por padrão, configurável);
      a previsão é calculada pelo índice acumulado e vira título a receber com
      valor EDITÁVEL, correlacionado à medição de origem. O previsto fica
      guardado ao lado do aprovado, para a diferença aparecer.
- [x] 9. **Tabela do INCC dentro do sistema** — FEITA em 09/09/2026. Em
      Configurações › "Índices (INCC)": busca no Banco Central pelo botão
      (série **192** do SGS, pública e gratuita — evita o FGVDados, que é
      licenciado), lançamento à mão pelo boletim da FGV para o mês que ainda
      não saiu, e a coleta NUNCA sobrescreve o que foi digitado.
      ⚠️ **A primeira chamada de verdade só acontece no Render**: a saída para
      a internet do ambiente de desenvolvimento é filtrada e bloqueia o
      endereço do Banco Central. O caminho de erro foi exercitado (a tabela
      continua intacta e a tela explica), mas o caminho de sucesso contra o
      serviço real, não.

**Feito em 09/09/2026, fora da ordem porque ele corrigiu/perguntou:**

- [x] **Por onde a nota sai NÃO se escolhe, se deriva** (medição → obra →
      empresa). Obra sem empresa recusa em vez de chutar; título rateado entre
      obras de empresas diferentes manda separar.
- [x] **Controle da numeração** (migração 048): o ERP é dono da sequência da
      DPS; a prefeitura devolve o número da nota. Reserva antes de emitir,
      número queimado não recicla e exige motivo, homologação separada de
      produção, e a conferência separa BURACO de QUEIMADO.
- [x] **Título rateado entre obras de contas diferentes: bloqueado** no
      lançamento, dizendo quais obras, quais contas e qual a saída.

**Sobre Petrolina (pergunta dele, pesquisada em 09/09/2026):** o município tem
webservice **e usa o MESMO provedor do Eusébio** (E&L), com o endereço no mesmo
molde. O emissor não precisa ser reescrito — o endereço e o código IBGE, hoje
fixos no código, viram configuração. O que depende de providência dele:
Inscrição Municipal em Petrolina, credenciamento, token próprio do canal e os
códigos de serviço/alíquota de lá.

### Gestão de documentos da empresa — pedida pelo dono em 09/09/2026

Especificação inteira (taxonomia, nomenclatura, blocos, permissão) em
`GESTAO_DOCUMENTOS.md`. O resumo do pedido, nas palavras dele: *"um ambiente
onde eu pudesse simplesmente jogar esse documento, ele fosse interpretado,
lido, e a partir dali categorizado, renomeado e salvo"*.

- [x] 1. **Catálogo e arquivo**: tipos, donos, validade, nome padronizado.
      FEITO em 09/09/2026 (migração 045), 59 tipos em sete grupos.
- [x] 2. **Tela de gestão** (Administração › Arquivo): filtros por
      tipo/grupo/empresa/obra/competência/validade, busca e abertura do
      arquivo. FEITA em 09/09/2026.
- [x] 3. **Leitura por IA** sugerindo tipo, dono, datas e nome — FEITA em
      10/09/2026. A pergunta é montada a partir do catálogo QUE ESTÁ NO BANCO,
      então tipo criado pela empresa entra sozinho. Não achar o dono é
      resposta válida (a tela mostra o nome lido e manda escolher); validade
      anterior à emissão é descartada; a leitura DIZ o que não resolveu; e o
      texto extraído é guardado junto, o que já resolve o item 5.
      ⚠️ **A chamada real ao serviço de IA só acontece no Render** — aqui não
      há chave. O fluxo inteiro foi provado com a IA dublada e o caminho de
      erro, no navegador.
- [x] 4. **Blocos** (FISCAL, HABILITACAO, CADASTRO-FORNECEDOR, MEDICAO, OBRA)
      em `.zip`, **com a lista do que está faltando dentro**. FEITO em
      09/09/2026 (migração 046). O bloco aponta para TIPOS, não para
      documentos — por isso o de agosto e o de setembro são o mesmo bloco.
      Conteúdo do bloco FISCAL confirmado pelo dono como "o que o cliente pede
      na medição".
- [x] 5. **Busca dentro do texto** do documento — FEITA junto com o item 3 em
      10/09/2026: a busca do Arquivo já olhava o campo de texto; o que faltava
      era alguém preenchê-lo, e agora a leitura preenche.
- [x] 6. **Avisos de vencimento** de certidão e documento, na Agenda — o que
      vence já avisava desde a migração 051; em 10/09/2026 entrou o outro lado
      (migração 057): o documento que **nunca foi arquivado**. A pasta fiscal
      incompleta de uma obra em execução e a habilitação incompleta da empresa
      viram aviso, dizendo quais documentos faltam. É a conferência do bloco
      rodando sozinha, no recálculo em segundo plano.
- [x] 7. **Botões nos outros lugares** — FEITO em 10/09/2026. Na ficha do
      título (documentação fiscal daquela obra e competência, medição e
      dossiê da obra), na aba Documentos da obra, e na ficha da empresa
      (habilitação e cadastro como fornecedor). Sempre com a lista do que
      falta dentro do .zip.

### Notas fiscais — o cruzamento

- [x] **A TELA DO CRUZAMENTO** (nota × pedido × título × fundo fixo) — FEITA em
      09/09/2026, migração 044, em Financeiro › Notas fiscais. Entrada por
      importação de XML; casamento automático só pela chave de acesso; trava
      contra contar a mesma despesa duas vezes; dedutibilidade pelo lado das
      notas. Detalhe em `HISTORICO.md` e `NOTAS_FISCAIS.md` §8-B.
**DECIDIDO em 09/09/2026 pelo dono:** o ERP **só avisa**, não manifesta ao
fisco; e o **FSist sai** — a captura passa a ser própria, direto na SEFAZ. Os
dois itens abaixo deixaram de depender de decisão e viraram trabalho.

- [x] Certificado digital por empresa — FEITO em 09/09/2026 (migração 053). O
      arquivo .pfx e a senha vão CIFRADOS, a validade é lida de dentro do
      próprio arquivo, o CNPJ tem de bater com o da empresa, o anterior vira
      histórico, e o vencimento avisa na agenda 45 dias antes. Destrava a
      assinatura da DPS na emissão automática.
- [ ] **Estudar o serviço de distribuição da SEFAZ antes de codar**: limites de
      consulta, o que acontece ao perder o número de sequência, e se o
      certificado A1 da BWS tem o perfil necessário. Nada disso foi verificado.
- [ ] Captura das notas direto na SEFAZ, guardando o ponto de onde parou. A
      importação de XML CONTINUA existindo como rede — desligar o FSist antes
      da captura própria estar conferida seria trocar o certo pelo duvidoso.

### Fila de DESEMPENHO — pedida pelo dono em 08/09/2026

Nasceu da pergunta dele: *"e quando essa base de dados for crescendo? Como é
que é a estratégia de manter isso rápido?"*. A resposta longa está no
`HISTORICO.md`, seção "Velocidade: o que cresce e o que não cresce". A fila:

- [x] **ANEXOS SAEM DO BANCO E VÃO PARA O GOOGLE DRIVE.** FEITO em 09/09/2026 (migração 043), desligado até o dono criar a pasta e colar o endereço. Decidido pelo dono em
      08/09/2026, com o motivo dele: o plano de banco é de 2 GB e ele já paga
      2 TB de Drive por pouco. É a peça que mais cresce em tamanho.
      Cuidados que NÃO podem ser esquecidos na hora de fazer:
      1. **O link do Drive nunca vai para a tela.** O arquivo continua sendo
         servido pelo endereço do ERP, que confere permissão e escopo antes de
         entregar (hoje `exigir_anexo_no_escopo`). Link direto do Drive é link
         que qualquer um abre — e ali tem holerite, comprovante e contrato.
      2. **Pasta de serviço, que ninguém mexe à mão.** Arquivo movido ou
         apagado por uma pessoa no Drive some do ERP e ninguém fica sabendo.
      3. **A troca é num arquivo só** (`core/documentos/armazenamento.py`):
         todo mundo já salva e lê por ele. O modelo `Anexo` até já tem a coluna
         `dropbox_path` de legado — o caminho novo entra do lado, sem
         reescrever quem chama.
      4. **Mudar o jeito de guardar e mover o que já existe são DUAS etapas.**
         Primeiro o novo passa a ir para o Drive; depois um trabalho em
         segundo plano leva os antigos, conferindo o hash de cada um antes de
         apagar do banco. Nada é apagado sem cópia conferida.
      5. **O que se perde:** o ERP passa a depender do Drive estar no ar para
         mostrar um comprovante. Hoje não depende de nada externo. É o preço,
         e o dono aceitou sabendo.
      6. **NÃO copiar o jeito do `emissaonf`.** Aquele módulo sobe os PDFs da
         nota e marca cada arquivo como "qualquer pessoa com o link pode ver"
         (`publico=True` em `drive_upload.enviar`). Para anexo do ERP isso
         seria um vazamento: holerite e comprovante ficariam abertos a quem
         tivesse o link. O que se reaproveita dali é a MECÂNICA (conta de
         serviço, personificação, `supportsAllDrives`), não a permissão.

      **Como o acesso será dado** (respondido ao dono em 09/09/2026): não
      precisa credencial nova — a identidade do Google que o sistema já usa
      serve. Falta só a pasta e o código dela. Preferência: **Drive
      compartilhado** (os arquivos pertencem à empresa, não a uma pessoa, e
      dispensa a personificação). Alternativa que já funciona hoje sem mexer em
      nada: pasta no Drive de `contato@bwsconstrucoes.com.br`, porque o sistema
      já sabe agir como essa conta. ⚠️ Conta de serviço NÃO tem espaço próprio
      no Google — por isso ou é Drive compartilhado, ou é personificação; as
      duas coisas resolvem o mesmo problema de cota.

- [x] **Separar o trabalho pesado das telas** — FEITO em 10/09/2026 (migração
      055). Fila em segundo plano guardada no BANCO, porque o serviço se
      reinicia sozinho e fila na memória perderia trabalho calada. Uma linha
      de trabalho só, de propósito. Quem morre no meio volta para a fila —
      menos o que não pode repetir (emitir nota), que para e explica.
      Já usam a fila: **importação de cards do Pipefy**, **recálculo da
      agenda** (que abre na hora, com o cálculo por trás) e **emissão de nota**.
      Acompanhamento em Configurações › "Trabalhos em segundo plano".
- [ ] **Tirar a trava do "um processo só"** (o estado em memória do `chatbot`).
      Enquanto ela existir, aumentar o plano do Render rende menos do que
      deveria — parte da máquina maior fica sem uso.
- [x] **Tela de saúde do sistema** — FEITA em 10/09/2026 (migração 054). Em
      Configurações › "Saúde do sistema": tempo por tela (ordenado pelo tempo
      TOTAL, não pela média), memória em uso contra o teto do plano, tamanho do
      banco e o que mais ocupa, e avisos que dizem o que fazer. A medição é
      agregada por dia e rota, gravada em lote, e nunca derruba uma tela.
- [ ] Números do topo das telas pré-calculados, quando as somas começarem a
      pesar. Não antes.
- [~] Listas do ERP com "próxima página". **Solicitações: FEITO em 09/09/2026**
      — "carregar mais" que acrescenta, a linha dizendo "200 de 1.340", e os
      quadrinhos do topo somando o FILTRO INTEIRO em vez da página. Dois
      defeitos foram achados aí e corrigidos: o filtro de situação era aplicado
      depois do corte (não achava o registro antigo) e as somas do topo
      mentiam.
      **Falta nas demais**, todas ainda com corte silencioso: Notas fiscais,
      Notas emitidas, Arquivo, Agenda, Conciliação e Extratos (500);
      Empreitas, Locações, Despesa com colaborador, Movimentações e o painel
      "por pedido" (300). Nenhuma delas incomoda no volume de hoje — a de
      solicitações incomodava — e todas usam o mesmo `core/comum/paginacao.py`
      quando chegar a vez.

### Assistente virtual para os colaboradores — ideia registrada em 08/09/2026

Palavras do dono: *"eu tenho algumas ideias de utilização de inteligência
artificial para dialogar com os colaboradores, assistente virtual, coisas desse
tipo"*. Ainda não foi detalhado e **não está na fila** — está aqui para não se
perder. Quando ele retomar, o que já existe e serve de base: o `chatbot` e o
`whatsapp_gateway` (canal), o `notificador` (envio), o controle de consumo de
IA com teto (migração 030) e o agente de cobrança (migração 040), que já é um
robô que fala com pessoas por WhatsApp a partir de pendência do banco.

### Cadastro e arquivo juntos — princípio dado pelo dono em 10/09/2026

Palavras dele: *"gostaria que o sistema já preenchesse os campos de cadastro de
obra e ainda arquivasse o arquivo. Dessa forma não perco tempo"* — e a
generalização, que é o que importa: *"matariamos duas ações... Esse é um
princípio inclusive que deveríamos ampliar para o sistema como um todo. E já
estamos adotando, por exemplo na parte do financeiro essa leitura e deveremos
seguir pra parte de colaboradores. Cadastros e arquivo estarem associados
quando fizer sentido."*

- [x] **OBRA** — FEITO em 10/09/2026. Na aba Documentos da obra: joga o
      arquivo, o sistema lê, arquiva com nome padronizado E mostra o que
      preencheria no cadastro, campo a campo. Cada tipo de documento só
      preenche o que ele PROVA (matrícula → CNO; ART → responsável técnico;
      contrato → valor, vigência, data-base, índice; OS → ordem de serviço;
      apólice → seguro). Termo aditivo vira REGISTRO de aditivo, não
      sobrescreve o contrato. Campo em branco entra marcado; campo com valor
      diferente entra desmarcado, com os dois lados à vista.
- [ ] **COLABORADORES** — o próximo, pedido por ele na mesma mensagem. Mesma
      mecânica: jogar o RG/CPF, a ficha de registro, o ASO ou o certificado de
      NR e ter cadastro e arquivo resolvidos juntos. A peça genérica já existe
      (`core/arquivo/preenchimento.py`): falta a lista de campos por tipo do
      lado de pessoas e a área na tela.
- [ ] **FORNECEDOR** — o cartão CNPJ e o contrato social preencheriam o
      cadastro do parceiro. Menos urgente: a consulta à Receita já resolve a
      maior parte disso desde 10/09/2026.

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
