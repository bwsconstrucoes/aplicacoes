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

- [x] **As oito alterações do plano de contas** — FEITAS em 10/09/2026
      (migração 058), a partir do documento `PLANO_CONTAS_alteracoes.md` do
      dono. Devolução/estorno/reembolso saíram das receitas e viraram contas
      REDUTORAS de custo (3.5.01 a 3.5.03, com sinal negativo no relatório); a
      retenção conjunta CSRF/PCC (2.1.06) foi desfeita e a guia é rateada entre
      PIS, COFINS e CSLL; a CSLL virou conta única; o parcelamento tributário
      (9.4.03) deixou de ser fluxo; o grupo 8 virou RESULTADO (e por isso o
      relatório de desembolso por obra **deixou de ser necessário**);
      nomenclatura e descrições de "quando usar / com o que não confundir" em
      todo o plano. Detalhe em `HISTORICO.md`.
- [x] **Aposentar conta sem apagar histórico** — FEITO em 10/09/2026.
      Instalar o plano padrão desativa só o que nunca foi usado; conta com
      lançamento continua ativa e é relatada numa janela, com a contagem e o
      motivo, para o dono remanejar.
- [x] **Importar a base de 3.279 insumos em Excel** — FEITO em 10/09/2026. O
      importador lê .xlsx direto (primeira aba), casa a conta do plano por
      apelido e usa a coluna "Subcategoria = Locação" para marcar o insumo como
      locável — que é o que decide quem aparece na tela de Locações.
- [ ] **Decisão do dono: o critério de valor da ferramenta.** Ficou R$ 1.200,00
      por unidade (ou vida útil menor que um ano) separando 3.1.19 Ferramentas
      de 8.1.04 Ferramentas e equipamentos duráveis. Trocar o número em
      `LIMITE_FERRAMENTA` muda os dois textos de uma vez.
- [ ] **Decisão do dono: o que fazer com 2.1.06 e 9.4.03** se elas tiverem
      lançamento em produção. As duas não têm destino único (a primeira se
      reparte em três contas; a segunda depende de qual tributo foi parcelado),
      então o remanejamento é caso a caso, pela tela.
- [ ] **Decisão do dono: o nome do grupo 8.** Ele continua "Investimentos
      (ativo)", mas as contas agora são de RESULTADO — o "(ativo)" entre
      parênteses pode confundir. Trocar é barato; não foi feito porque mudar
      nome de grupo mexe em como todo mundo lê o relatório.

## Fila (pedidos registrados, ainda não iniciados)

### O ASSISTENTE DE IA E O RELATÓRIO DE TRABALHO — pedidos de 10/09/2026

Dois pedidos grandes que o dono fez na mesma conversa, e que valem juntos
porque o segundo é o primeiro ensaio do primeiro: os dois vivem da trilha de
auditoria.

**O que ele pediu, nas palavras dele:** *"eu poder fazer qualquer pergunta ao
sistema e, se houver dado daquela pergunta, que ele me retorne. E não só os
dados exibidos em tela, porque às vezes a gente pode ter em algum momento que é
necessário alguma informação que a gente não tenha pensado na construção do
sistema."* Exemplos que ele deu: o que tem a pagar hoje na obra X; quais obras
estão em andamento; quanto foi medido e quanto falta receber; o resultado da
obra agora; quantos títulos não estão conciliados; a lista de insumos de uma
categoria; **e também AGIR** — cadastrar insumo, lançar título. Com **áudio** e
**anexo**, e sempre **dentro da permissão da pessoa**.

- [x] **1. Relatório de uso e trabalho por pessoa** — FEITO em 10/09/2026,
      em Administração › **Trabalho no sistema**. Sem migração: o dado já
      estava na trilha. Cada pessoa vê a própria semana (primeira e última
      ação do dia, quantas ações, o que fez por tipo de trabalho) e pode abrir
      o passo a passo de qualquer dia seu. Quem tem a ação nova
      `ver_uso_da_equipe` (ADMIN e diretor) vê a equipe toda numa linha por
      pessoa. O que a fila de segundo plano fez sozinha sai numa linha
      separada, para não virar produção de ninguém. O aviso de que isto não é
      controle de ponto está na tela E viaja junto com o dado.
      **Ainda em aberto deste item:** "onde a fila está parada" (quem tem mais
      coisa esperando decisão) — é outra fonte de dados, vem depois.
      ⚠️ **A RESSALVA QUE NÃO PODE SUMIR NUMA REFORMA DE TELA:** log de
      atividade **não é jornada de trabalho**. Quem está lendo contrato, no
      telefone com fornecedor ou na obra trabalha sem gerar evento. Isto mede
      ENTREGA (quantos títulos, conciliações, medições) e se houve movimento no
      dia — não bate ponto. Se um dia virar controle de jornada, isso tem
      exigência legal própria e não se improvisa. **A equipe precisa ser
      avisada de que o sistema registra.**
      ✔ **DECIDIDO pelo dono em 10/09/2026:** *"na verdade não é pra controlar
      a jornada não, é só pra entender"*, e **pode ficar à vista dos outros
      também** — *"pra cada um entender o que é que ela está produzindo dentro
      do sistema"*. Foi por isso que a tela nasceu aberta à equipe.
- [ ] **1b. O catálogo de perguntas** — `app/apps/erp/PERGUNTAS.md`, criado
      em 10/09/2026 a pedido do dono, e agora regra do `CLAUDE.md`:
      funcionalidade nova só está pronta quando as perguntas que ela responde
      entram lá. O arquivo já começa com a parte que mais evita número errado:
      a lista das PALAVRAS que precisam de uma definição só ("a pagar", "este
      mês", "custo da obra", "quanto falta receber"). Enquanto uma dessas não
      estiver decidida, o assistente pergunta de volta em vez de escolher.
- [x] **1c. As primeiras perguntas respondidas por CÓDIGO** — FEITO em
      10/09/2026, em Financeiro › **Perguntar**. Cinco perguntas do grupo
      financeiro, todas passando pelo mesmo escopo por obra e por autoria das
      telas, e cada uma devolvendo o caminho de volta para os lançamentos:
      o panorama de vencimentos (vencido / hoje / próximos 7 dias), o que tem
      a pagar num período, o que está vencido e não foi pago, **o que está
      parado esperando decisão e de quem é a vez** (o pedaço que faltava do
      relatório de trabalho) e os títulos sem documento anexado.
      **Sem IA nenhuma** — é a fundação: quando a IA entrar, ela só escolhe
      QUAL destas funções chamar, e a conta continua sendo do sistema.
      A rota é por GRUPO de pergunta (`/erp/api/perguntar/financeiro`), para
      cada grupo declarar a sua própria ação sem mentir.
- [x] **1d. A régua do recebimento** — FEITA em 10/09/2026. "Quanto falta
      receber" responde com as TRÊS leituras lado a lado (do contrato, do
      medido, do faturado), porque o dono mostrou que todas são legítimas e
      escolher uma seria responder certo para uma e errado para as outras
      duas. Vieram junto "o que foi medido e não virou nota" e "o que tem nota
      e não entrou". O quadro do contrato ganhou as duas subtrações que
      faltavam, e a pergunta REUSA esse quadro — há teste exigindo que o número
      da pergunta e o da tela batam campo a campo.
      Grupo próprio (`contratos`), com rota e ação próprias: `ver_contratos` é
      estreita porque o quadro mostra o contrato de ponta a ponta e não se
      recorta por obra designada sem mentir no total.
- [x] **1e. As perguntas de Suprimentos** — FEITAS em 10/09/2026, sob a ação
      `ver_suprimentos`: os insumos de uma categoria (pedida pelo dono com
      estas palavras), quanto já se pagou por um insumo, o que a obra pediu e
      ainda não foi resolvido, e os insumos sem conta do plano.
      Duas naturezas convivem no grupo: o CATÁLOGO é cadastro da empresa e não
      se recorta por obra; a FILA DE PEDIDOS passa pelo filtro por pessoa da
      tela de Solicitações.
      Vieram junto duas regras que valem para TODA resposta: o **teto de
      linhas** (a conta é sobre tudo, o corte é só do que aparece na tela — a
      base tem 3.285 insumos) e a **busca sem acento** (quem procura
      "Hidráulico" digita "hidra").
- [x] **1f. As perguntas de Locações** — FEITAS em 11/09/2026, dentro do grupo
      de Suprimentos: o que está locado e em qual obra, qual locação já pedia
      decisão (aluguel que já pagou a compra, devolução vencida, prazo
      estourado) e que aluguel venceu sem virar título. Reusam
      `locacoes.listar`, que já calculava tudo isso — refazer a conta seria
      inventar um segundo número sobre a mesma coisa.
- [x] **BRECHA DE ESCOPO NAS LOCAÇÕES, fechada em 11/09/2026** — achada por um
      teste ao escrever as perguntas acima, e **anterior a este trabalho**.
      Contrato de locação NÃO TEM AUTOR, e a listagem usava `obras_do_usuario`,
      que devolve "sem filtro" para quem enxerga por autoria: o administrativo
      que só deveria ver o que ele lançou via TODOS os contratos da empresa.
      Pior: `painel_por_obra` não recebia usuário nenhum, e a rota que o serve
      é aberta a todo operador — qualquer pessoa via quanto CADA obra tem de
      aluguel. A regra virou `obras_de_registro_sem_autor`, em
      `permissoes.py`: para registro sem autor o único recorte é a obra, e sem
      obra designada não se vê nenhum.
- [x] **1g. PERGUNTAR ESCREVENDO** — FEITO em 11/09/2026, depois de o dono
      corrigir o rumo: *"o assistant não pode ficar somente focado nessas
      perguntas, né? Isso é só um norte"*. A tela ganhou um campo de texto.
      `core/perguntas/entender.py` lê a frase e diz QUAL pergunta ela é — **sem
      tocar no banco**, que é o que permite a rota ser aberta a todo operador
      sem mentir na declaração: quem responde continua sendo a rota do grupo.
      Três finais honestos: **entendi** (responde, já com os filtros que a
      frase disse), **qual delas?** (empate no topo vira pergunta de volta) e
      **ainda não sei** (guarda a pergunta e sugere as parecidas).
      **O que ele não entende fica registrado**, e há uma rota que lista isso —
      é a lista do que construir em seguida, escrita por quem usa o ERP.
      ⚠️ Ainda é casamento por PALAVRAS, não IA. A IA entra exatamente aí
      quando a chave existir em produção, e o resto não muda.
- [x] **1h. O GRUPO DE OBRAS** — FEITO em 11/09/2026. Três perguntas de
      conferência: **o que falta no cadastro para emitir nota** (os mesmos
      quatro campos que a emissão exige), **seguro garantia vencido ou
      vencendo** e **obra aberta com a vigência do contrato vencida**.
      ⚠️ **Nasceu pequeno de propósito.** "Quanto custou a obra tal" — a
      pergunta mais óbvia do assunto — NÃO entrou: "custo da obra" ainda não
      tem uma definição escolhida, e cada leitura dá um número diferente com
      cara de certo. Um teste da suíte recusa qualquer pergunta deste grupo que
      use "custo", "resultado", "lucro", "margem" ou "gastou", para que
      ninguém a acrescente sem combinar a palavra antes.
      **Falta o dono decidir**, e aí o grupo cresce de uma vez: "custo da
      obra", "obra em andamento", "este mês", "gastei com fulano" e "resultado
      da obra" (ver `PERGUNTAS.md` §1).
      Junto vieram duas correções que a construção fez aparecer: o **tipo de
      cada coluna passou a ser dito pelo servidor** (a tela vinha escrevendo
      data como 2026-08-30 e dinheiro sem R$, e pergunta nova nascia com o
      defeito calado), e o **painel de Obras parou de mostrar contrato, gasto
      e margem de todas as obras para quem enxerga só o que lança** — ver
      `HISTORICO.md`.
- [ ] **2. Assistente SÓ DE LEITURA, dentro do ERP.** Painel lateral (não
      caixinha), com o catálogo de perguntas conhecidas respondido por CÓDIGO —
      exato, rápido e sem custo de IA — e a pergunta imprevista caindo numa
      consulta gerada pela IA, **marcada como tal na tela**. Toda resposta com
      "ver de onde veio", abrindo a lista por trás do número.
      ⚠️ **O risco que manda no desenho:** consulta gerada por IA sobre um
      banco grande acerta a maior parte das vezes e erra em silêncio no resto.
      Número errado com cara de certo é pior que resposta nenhuma. Por isso o
      catálogo primeiro, o "não sei" explícito, e a origem sempre visível.
- [ ] **3. Áudio e anexo na conversa**, e o ERP virando **PWA** (ícone no
      celular que abre no navegador, com aviso por notificação). É o mesmo
      sistema, não um segundo aplicativo — app nativo aqui seria duas bases de
      código e loja para nada.
- [ ] **4. As AÇÕES pelo assistente** (cadastrar insumo, lançar título) —
      sempre **preparar e confirmar**: o assistente preenche e mostra, a pessoa
      aperta. Nunca "já lancei". E passando pelas MESMAS funções do core que a
      tela usa, para herdar permissão, escopo por obra e regra de negócio — não
      um caminho paralelo até o banco.
- [ ] **5. WhatsApp/Telegram como porta secundária**: aviso e pergunta curta,
      com link para abrir no ERP. Não como canal principal — ver o porquê em
      `HISTORICO.md` › "Por que o assistente não nasce no WhatsApp".
- [ ] **6. Assistente PROATIVO** (ideia trazida na conversa, não pedida): o
      valor maior não é responder, é falar primeiro. "Estas 3 medições estão
      liberadas para faturar", "a CND deste fornecedor venceu e há título para
      pagar amanhã", "este material está 40% acima do que esta obra costuma
      pagar". A Agenda já existe; o assistente é a voz dela.
- [ ] **7. Pergunta boa vira relatório salvo E AGENDADO PELA PRÓPRIA
      CONVERSA.** O dono voltou nisto em 10/09/2026, e com razão — é a peça que
      faz o resto valer: *"toda segunda-feira me manda determinado tipo de
      informação. Aí a própria [IA] agendar essa necessidade minha e fazer
      aquela ação executar e me mandar."* Ou seja: ele PEDE em português, e o
      agendamento nasce da frase; ele não vai configurar nada em tela.
      **Está mais perto do que parece** — as quatro peças já existem: a fila de
      trabalho em segundo plano (migração 055, com recuperação de tarefa órfã),
      o envio por Telegram (`core/notificacoes.py`), o e-mail pela conta da
      empresa (`core/comum/email.py`) e a exportação em Excel e PDF
      (`core/comum/exportar.py`). Falta o relógio, a pergunta guardada e a cola.
      O que decide se funciona ou vira lixo:
      - **Guardar a CONSULTA, não a frase.** Se toda segunda a IA reinterpretar
        o texto, o relatório muda de forma e de critério sozinho, e não dá para
        comparar uma segunda com a outra. Guarda-se a consulta que gerou o
        resultado que ele aprovou.
      - **Comparar com a semana passada.** Número solto é ruído; "R$ 340 mil a
        pagar (era R$ 280 mil)" é gestão. Exige guardar o resultado de cada
        rodada — barato, e é o que dá valor.
      - **O "só me avise se".** Relatório que chega igual todo mês vira spam e
        para de ser lido — acontece em toda empresa que faz isso. Além do fixo,
        o condicional: manda só quando cruzar uma linha.
      - **Relatório para OUTRA pessoa roda com a permissão DE QUEM RECEBE**, não
        de quem criou. Senão o gestor de uma obra recebe, sem querer, o número
        da empresa inteira.
      - **Relatório que quebrou tem de RECLAMAR.** Se a obra acabou ou a conta
        foi aposentada, ele não pode mandar zero em silêncio — zero silencioso
        é pior que erro, porque parece resposta.
      - **Agendado pode LER sozinho; para AGIR, prepara e espera o dono
        apertar.** Mesma regra do item 4.
- [ ] **8. Teto de custo de IA POR PESSOA**, não só global (`core/comum/
      ia_custo.py` já tem o teto do mês). Sem isso, a curiosidade de uma pessoa
      come o mês inteiro.
      ✔ **DECIDIDO em 10/09/2026:** o assistente é para **qualquer pessoa,
      dentro das atribuições e permissões dela** — não só o dono. E por isso
      mesmo o teto por pessoa é requisito, não enfeite: *"a gente pode ter
      muitas pessoas aí utilizando, a brincar às vezes, e a gente não pode
      estourar os limites"*. O VALOR mensal por colaborador fica para o dono
      definir depois.
- [ ] **9. Guardar toda pergunta e toda resposta.** Serve para controlar custo,
      para auditar e — o mais útil — porque a lista do que perguntam repetido é
      a lista das telas que faltam.


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
- [x] **COLABORADORES** — FEITO em 10/09/2026, logo depois da obra. Na ficha
      da pessoa: joga o RG, a carteira, a ficha de registro, o contrato ou o
      termo de rescisão e o cadastro se preenche junto com o arquivamento.
      Duas regras próprias daqui: o **CPF é conferido e nunca gravado** (é a
      identidade da pessoa — trocá-lo repontaria pagamento e histórico), e
      **função só entra se já estiver cadastrada**, senão "PEDREIRO",
      "Pedreiro" e "Pedreiro(a)" virariam três diárias diferentes. Com o CPF
      divergindo, o preenchimento fica TRANCADO até alguém confirmar que o
      documento é daquela pessoa.
- [x] **OBRA QUE AINDA NÃO EXISTE** — FEITO em 10/09/2026, pedido do dono na
      mesma conversa: *"nós havíamos conversado sobre a criação de obras a
      partir de um documento, da leitura de um documento. Então isso ficaria
      associado a obras."* No painel de Obras, "+ Nova obra" abre em **A partir
      de um documento**: manda o contrato (ou a CNO, a ART, a ordem de
      serviço), o sistema lê, propõe os campos, e num clique cria a obra e
      guarda o documento dentro dela. O **código** é a única coisa que ele
      pergunta e não adivinha — é convenção da casa, não sai de documento
      nenhum, e código errado contamina rateio, medição e nota.
      Guarda contra duplicar: mesma matrícula CNO ou mesmo número de contrato
      **param** a criação até alguém marcar "sei que é outra obra".
- [ ] **FORNECEDOR** — o cartão CNPJ e o contrato social preencheriam o
      cadastro do parceiro. Menos urgente: a consulta à Receita já resolve a
      maior parte disso desde 10/09/2026.

### Um lugar só para cadastrar obra — pedido do dono em 10/09/2026

Ele foi cadastrar obra e viu dois formulários diferentes: *"eu posso criar a
obra tanto pela administração como posso criar a obra por obras, e lá aparecem
menos campos. Então acho que tem que unificar isso aí: se a gente tem o painel
de obras, não tem mais que ter obras em administração."*

- [x] **FEITO em 10/09/2026.** Configurações não cria mais obra — o cartão
      "Obras" de lá virou um ponteiro para o painel. O formulário do painel
      passou a ter o cadastro de identificação inteiro (código, nome,
      contratante, CNPJ, contrato, objeto, município, UF, CNO, valor e ISS), e
      depois de criar já abre a ficha para completar vigência e tributação.
- [x] **Efeito colateral que era defeito de verdade:** o formulário antigo de
      Configurações gravava a alíquota de ISS numa coluna que a tributação e a
      tela **não leem** — e a emissão automática da nota lia justamente essa.
      Obra cadastrada pela tela de tributação era recusada por "sem alíquota de
      ISS"; obra com as duas preenchidas diferentes mandaria à prefeitura um
      percentual que ninguém viu na tela. Unificado.

### O primeiro contato de verdade com os cadastros — 10/09/2026 (tarde)

O dono começou a usar o sistema para valer e mandou seis coisas de uma vez.
Todas feitas no mesmo dia.

- [x] **Filtro de obras repetindo o nome** (Contratos e medições, Agenda, Notas
      emitidas). O código da casa costuma SER o nome abreviado, e escrever os
      dois colados fazia parecer duplicado. Agora, quando um já contém o outro,
      aparece só o mais completo.
- [x] **Cadastro de conta bancária** — o formulário mostrava menos campos que a
      tabela ao lado. Agora a **chave Pix entra junto** e o **banco se escolhe
      pelo nome**, de uma lista de 118 bancos que veio embutida no sistema (não
      depende de internet). Um botão troca essa lista pela oficial do Banco
      Central.
- [x] **Zerar as obras** em Banco e limpeza. É CADASTRO, então ganhou um bloco
      vermelho separado do movimento, e o colaborador **não sai junto**: só
      deixa de estar ligado à obra.
- [x] **Contas do plano no cadastro do operador** — o grupo marcado agora fica
      **verde**, e **âmbar** quando está pela metade, com a contagem ao lado.
      A lista passou a ser em colunas, agrupada.
- [x] **Perfis de obra pré-configurados** — administrativo de obra, supervisor
      e gestor já nascem podendo lançar custos de obra, pessoal e despesas
      administrativas, com o fundo fixo liberado. É sugestão: aparece marcado
      e desmarcar é um clique.
- [x] **Arrastar o documento para dentro da tela do Arquivo** — soltar o
      arquivo em qualquer lugar da tela já dispara a leitura e abre o
      formulário preenchido.

⚠️ **Um defeito de estilo antigo apareceu no caminho**, e era a causa real do
"muito espaçada, ruim de visualizar": toda caixinha de marcar dentro de um
campo virava bloco — quadradinho em cima, texto embaixo, duas linhas por opção
—, e o quadradinho esticava para a largura inteira. Valia para a lista de
contas, a de obras designadas e a de permissões do operador. Corrigido no
estilo, num lugar só.

### O lançamento visto de perto — 10/09/2026 (noite)

O dono lançou um título de verdade pela primeira vez e mandou uma lista.

- [x] **Empresas saiu da aba do topo** e passou a morar dentro de
      Configurações: *"a gente vai cadastrar três, quatro empresas, é uma
      coisa de configuração"*. A tela é a mesma.
- [x] **Descrição do título virou campo de várias linhas.** Estava pela
      metade no HTML — um `input` fechado com `</textarea>` —, então o pedido
      anterior nunca tinha funcionado. A quebra de linha é guardada e aparece
      igual na ficha do título.
- [x] **Parcelas que se preenchem sozinhas**: a primeira nasce com o líquido
      inteiro, acrescentar parcela divide o valor, e o vencimento seguinte cai
      um mês à frente do anterior.
- [x] **Vários documentos no mesmo lançamento** — e, o que era pior, **os
      documentos passaram a ficar guardados**: antes o arquivo servia para a
      leitura e ia embora. Agora nota, boleto e comprovante ficam anexados ao
      título, e cada um pode ser lido ou só arquivado.
- [x] **Rateio por conta do plano**: dava para usar duas contas no mesmo
      título, mas a tela só falava em obra. Rótulos corrigidos, linha nova já
      repete a obra anterior, e um botão divide igualmente.
- [x] **Importar as categorias de insumo** junto com os insumos, por uma
      marcação na tela — a carga continua sem inventar categoria por padrão.

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
