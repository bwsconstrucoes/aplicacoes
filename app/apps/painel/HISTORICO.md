# Painel OMIE — decisões, incidentes e o que falta

Este arquivo existe para uma sessão nova (ou uma pessoa nova) pegar o trabalho
sem repetir o que já foi discutido e sem repetir o que já deu errado.

O `README.md` ao lado explica **como o painel é**. Este aqui explica **por que
ele é assim**, e o que aconteceu no caminho. Leia os dois antes de mexer.

---

## Onde o trabalho está

O painel financeiro do OMIE rodava em Streamlit, no computador do dono, lendo
arquivos de uma pasta de 153 MB. Virou módulo Flask do monorepo, em `/painel`,
com login próprio e os dados no Postgres do ERP (schema `painel`).

**Nove telas convertidas**, todas conferidas contra a versão original com dados
reais: Visão Geral, DRE, Despesas Analítico, Receita de Obra, Fluxo de Caixa,
Resultado por Obra, Comprometido × Executado, Necessidade de Caixa e Prestação
de Contas.

**Estado em 04/09/2026 (fim do dia):** duas levas foram publicadas — o
`painel-filtro-e-velocidade` (junção `a0dcfef`) e o `painel-duas-datas`
(junção `0bfa75b`, com a migração `006` aplicada pelo dono). No ramo de trabalho,
**prontos e não publicados**: a correção da mensagem duplicada em Configurações e
a tela nova de **Cenários de rateio**.

Com isso a **conversão do painel terminou**: dez telas e o relatório em
PDF, tudo o que o Streamlit fazia.

### O que está pendente AGORA

**Nada de código.** Tudo o que foi feito em 04/09/2026 está publicado: as duas
datas (`0bfa75b`), os cenários de rateio e a mensagem duplicada (`376fe72`), o
relatório em PDF (`3132e72`) e as correções do Analítico mais a senha com acento
(`b1ae033`).

O que está pendente é **conferência com dado real** — ver "O que falta" no fim
deste arquivo. Nenhum teste alcança isso: depende de abrir a tela publicada e
comparar com o Streamlit.

<details>
<summary>O que já foi publicado nesta leva (04/09/2026)</summary>

**Decidir se o `painel-duas-datas` vai ao ar.** Ele responde ao item 2 de "O que
falta": vencimento e pagamento viram colunas próprias no Despesas Analítico, com
o atraso em dias entre as duas, seletor de por qual data a faixa filtra, e as
duas datas na planilha.

O que a publicação dele exige, e que os ramos anteriores não exigiam:

- **Tem migração de banco** (`006_duas_datas_no_fato.sql`). Ao juntar, apertar
  "Aplicar atualizações do banco" **no mesmo momento**.
- **A migração cria as colunas vazias.** Quem preenche é a próxima atualização
  do fato — a carga da madrugada resolve sozinha; para ver no mesmo dia, é
  Configurações › "Só refazer os números" (não baixa nada do OMIE). Enquanto
  estiverem vazias, a tela avisa, com o caminho escrito.
- Sem dependência nova.

Verificado em 04/09/2026, com a `main` de hoje já dentro do ramo: **1140 testes
passando com Postgres de verdade** (75 pulados, todos por decisão registrada no
próprio teste de homologação do ERP), aplicação subindo com os 16 blueprints
registrados, e as seis migrações do painel aplicadas do zero num banco limpo,
uma a uma, sem erro.

O que **não** foi verificado, e é o risco a dizer em voz alta: o número que sai
na tela com a base real. As colunas novas nunca viram dado do OMIE de verdade —
os testes provam que o vencimento vem do título e o pagamento vem do movimento,
mas nenhum deles olha a base da empresa.

Três coisas continuam sem conferência contra dado real, e as três dependem de
alguém abrir a tela publicada:

1. o **bloco de aportes** do DRE, que nunca viu a base de verdade;
2. **quanto o painel ficou mais rápido.** O rodapé agora mostra o tempo da tela
   e quantas consultas foram — é ler o número no Analítico e comparar com a
   sensação de antes. Se continuar lento com poucas consultas, o gargalo não é
   o código.
3. as **duas datas e o atraso**, depois de refeito o fato.

</details>

### 08–09/09/2026 — a passagem do painel Streamlit

O dono continuou evoluindo o painel Streamlit depois que a conversão começou, e
trouxe um documento de passagem com o que foi feito lá de 04 a 08/09. Ele pediu
para consolidar tudo aqui, seguindo os quatro itens sem parar.

**O que já estava pronto aqui** (conferido antes de refazer): a seção 5 inteira
do documento — a Necessidade de Caixa, com caixa reconstruído, saldo inicial,
leitura automática, composição dos conjuntos e empréstimo tomado no mês.

**A pendência 4.1 dele existia aqui também.** Rótulo começando com "=" vira
FÓRMULA no Excel: "= RESULTADO", "= Receita Líquida" e "= Total
Custos/Despesas" chegavam na planilha do DRE como fórmula inválida, mostrando
erro no lugar do rótulo — justamente nas três linhas que se procura primeiro.
Corrigido, e vale para +, − e @ pelo mesmo motivo.

**Item 1 — o código da categoria e um rótulo só.** A `categoria` do fato é a
descrição; para ALTERAR no OMIE é preciso o código, que existia no espelho e
parava ali. Migração 007, com a marca `REFAZER-O-FATO`. E os cinco literais
"(sem obra)" viraram uma constante `SEM_OBRA = "(não apropriado)"` — é por esse
rótulo que se procura o que precisa ser saneado, e dois nomes para a mesma
coisa fariam a busca não achar nada.

**Item 2 — o Explorador.** A diferença dele para todas as outras telas: olha a
base INTEIRA, não só o DRE. É assim que se acha o lançamento na análise errada,
o aporte lançado como despesa e o título sem apropriação. **Fora do menu
principal**, a pedido do dono: *"uma tela mais escondida, para não ser algo tão
exposto"*. Chega-se a ela por Configurações, e há teste exigindo as duas coisas
— que não esteja no menu e que o link exista.

**Item 3 — alterar no OMIE. A única parte do painel que escreve num sistema de
fora.** O dono: *"é realmente algo sério, eu não posso falhar nem errar"*.
Quatro proteções, cada uma com teste que acusa se for desfeita:

1. **Senha própria** (`PAINEL_SENHA_ESCRITA`), diferente da de entrar. Sem ela
   no ambiente, alterar fica **desligado** — o ensaio continua funcionando.
2. **Simulação por padrão.** Só sai do ensaio quem marca E acerta a senha.
3. **Trava do rateio.** Trocar o departamento põe 100% no novo e apaga o rateio
   anterior. Título dividido entre obras é **recusado antes mesmo de ser
   consultado no OMIE**, a menos que quem altera diga explicitamente que sabe.
4. **Registro de tudo no banco** — inclusive dos envios que deram errado, que é
   quando o registro importa. Migração 008. **Em tabela e não em arquivo**: o
   painel Streamlit gravava num `.jsonl`, e aqui o disco do Render é apagado a
   cada reinício.

Mais um teto de 200 títulos por vez: não é limite técnico, é para um engano de
seleção não virar um estrago de mil títulos antes de alguém perceber.

**O que continua NÃO verificado, e é do documento original:** a escrita nunca
foi testada contra a API real do OMIE. O protocolo é **ensaio → UM título
conferido no OMIE → lote**, e está escrito na própria tela.

**Item 4 — Rateio da Administração.** Simulação: como o custo da matriz se
divide entre dois lados da empresa, mês a mês. **Duas repartições diferentes**,
e confundi-las seria o erro caro:

- o **custo da matriz** vai pelo CRITÉRIO (faturamento ou pessoal, numa janela
  de meses): quem produziu mais carrega mais estrutura;
- os **juros do banco** vão pelo DÉFICIT: quem estava com o caixa negativo
  naquele mês. Juros não é estrutura, é o preço de faltar dinheiro.

Sem circularidade: o déficit que decide quem paga os juros é medido **antes**
dos juros. Há teste exigindo isso.

A matriz fica fora dos dois lados — ela é o bolo, não um pedaço dele — e o bolo
nunca vira positivo: matriz que num mês recebeu mais do que gastou não
distribui lucro, e uma sobra dessas viraria caixa fantasma nas obras.

Reusa `pesos_do_conjunto` e `meses_do_periodo` da Necessidade de Caixa, como o
próprio documento sugere. Também fora do menu, em Configurações.

**Conferido com dado montado, na tela:** matriz de −20.000/mês por 12 meses,
CASA faturando 90.000 e PONTE 30.000. O critério manda 75% do bolo para A
(−180.000) e 25% para B (−60.000). Mas A nunca ficou negativa e B ficou nos 12
meses — então **100% dos juros foram para B**, contra os 75% que o critério
mandaria. É exatamente a distinção que a tela existe para mostrar, e ela
apareceu certa na imagem.

### 08/09/2026 — os juros que sumiam do resultado

O dono comparou duas telas da mesma obra: **Visão Geral R$ 931.718,04** contra
**DRE R$ 888.419,91**. A diferença, **R$ 43.298,13**, era exatamente a linha
"Juros e Multas Pagos".

**A causa:** de todo o `consultas.py`, só o **DRE** e o **Analítico** somavam os
encargos. Visão Geral, Resultado por Obra, Comprometido × Executado, o gráfico
do DRE, as três telas de caixa e — o mais grave — a **base da Prestação de
Contas** somavam só o principal. O resultado saía maior do que é, e o que se
dividia entre os sócios também.

**Não foi defeito da conversão.** O Streamlit original fazia igual: `Encargo`
aparece em duas linhas do arquivo de referência, as duas dentro do DRE. A
Visão Geral dele calculava `desp = Comprometido.sum()`, sem encargo. Confirmado
lendo `referencia_streamlit/telas_streamlit.py` antes de mexer.

**A decisão foi do dono**, com a pergunta posta e o custo dito: *"eu considero
que seja despesa também"*, *"não pode dar uma visão de resultado de obras sem
essa informação"*, e **"todas as telas, prestação incluída"** — sabendo que o
resultado dividido entre os sócios diminui e passa a discordar de acerto feito
com número antigo.

**Como ficou:** duas medidas novas, `COMPROMETIDO_COM_ENCARGO` e
`EXECUTADO_COM_ENCARGO`, mais `MOVIMENTO_DE_CAIXA` para as telas de caixa (juros
pago sai da conta como qualquer pagamento). **Use-as em qualquer soma de
despesa** — o `COMPROMETIDO` cru só serve para receita.

**O que NÃO mudou, de propósito:**
- **a aba Despesas por grupo/categoria**: encargo não tem categoria no plano de
  contas do OMIE, e a decisão de 03/09 (linha própria só na planilha) continua;
- **o lado da receita**: juros recebido é receita financeira, outra conversa;
- **o custo de pessoal do rateio**: é peso de proporção, não total.

**A conferência que vale:** há teste com banco de verdade exigindo que Visão
Geral, DRE, Resultado por Obra, Comprometido × Executado, Top Credores, caixa e
Prestação de Contas **deem o mesmo número** sobre a mesma base. É a classe do
problema, não o caso — qualquer tela que volte a somar só o principal quebra a
suíte.

E os **R$ 999 de juros previstos** num título em aberto continuam fora: encargo
só entra quando foi pago. Isso já era assim e o teste guarda.

**Custou 15 testes atualizados**, cada um por 25 centavos-de-cenário a mais de
despesa. Nenhum deles era código quebrado — era a regra mudando, e o número
antigo virou o número errado.

### 08/09/2026 — o gráfico que engolia a tela

O dono mandou uma foto da Visão Geral filtrada numa obra: o gráfico ocupava
**620px de altura** e as barras eram blocos de ~157px de largura. "Veja a
desproporcionalidade."

**Duas causas, somadas:**

1. **Sem teto de altura.** O desenho é 900×320 e o CSS mandava `width:100%`.
   Numa tela de 2000px isso vira 1740px de largura — e, como a proporção é
   mantida, **620px de altura**. Quanto maior o monitor, pior.
2. **Sem teto de largura da barra.** Com poucos períodos (uma obra filtrada tem
   3 ou 4 anos), cada barra ficava com um terço da fatia e virava paredão.

**A tentativa que NÃO serviu, e por quê.** A saída óbvia era deitar o desenho
(1500×320) para ele preencher a largura sem crescer para baixo. Medido antes de
aceitar: num notebook de 1366px o desenho encolhe para 69% e **o texto do eixo
cai para 7,6px** — ilegível. **Tudo escala junto, inclusive o texto.** Com
900×320 e o teto de altura, o texto fica entre 12,6px e 13,1px em qualquer tela,
do notebook ao ultrawide. O preço é uma folga nas laterais em monitor largo, que
é só espaço em branco.

**Resultado:** gráfico de 1069×380px, barras de ~52px. Conferido com foto da
tela de verdade, não só por número.

**A armadilha do teste, que quase passou.** A primeira versão do teste comparava
a largura da barra com a **própria constante** que ela deveria vigiar — subir a
constante para o infinito deixava o teste passar. Agora a medida é contra a
largura do gráfico: uma barra não pode passar de 6% dele. **Teste que se mede
pela régua que deveria vigiar não vigia nada.**

### A leva de 04/09/2026 (noite) — quatro defeitos que o uso real mostrou

O dono foi usar o Despesas Analítico de verdade e trouxe quatro coisas. As duas
primeiras são graves porque **mentem sobre número**.

**1. A tela dizia 481 lançamentos e o arquivo baixado tinha 316.** Os botões de
download faziam `url_for(..., **request.args)`. O `**` sobre um MultiDict pega
**um valor por chave** — e ano, projeto e obra são múltiplos. Quem filtrava três
obras baixava o arquivo de **uma**, sem aviso nenhum: o arquivo abre, só vem
incompleto. Valia para **os dez** botões de download do painel.

A correção é um ajudante `link_baixar()` no `web.py`, que usa
`to_dict(flat=False)` — o mesmo cuidado que o `pagina_link` ao lado já tomava
desde sempre. E há teste varrendo **todos os templates** atrás de `**request.args`
num link de download: é a classe inteira do problema, não o caso.

**2. A "Visão" do Analítico não filtrava.** Escolher "Só a pagar" e clicar em
Aplicar devolvia a mesma lista, com as contas quitadas no meio mostrando zero na
coluna. A visão só decidia **por qual coluna ordenar** — o rótulo prometia um
recorte que não existia. Agora ela entra no `WHERE`. "Só pagas" inclui a linha
em que só os encargos foram pagos: juros quitados são dinheiro que saiu.

**3. A coluna Documento aparecia com o dado duplicado.** Quando a observação não
traz medição, a chave de agrupamento cai no **próprio documento**
(`DOC:<numero>`), e o rótulo dela vira o número. A tela mostrava o documento e,
embaixo, o mesmo número como "medição". Agora a medição fica vazia quando é eco
do documento — e vazio ali quer dizer o que tem de querer: esta despesa não está
amarrada a nenhuma medição. Vale para a tela, a planilha e o PDF.

**4. O rodapé da paginação não dizia quantas páginas existem.** O total só
estava no `max` do campo, que ninguém vê: quem chegava lá embaixo digitava um
número sem saber até onde ia. Agora diz "de N" e quantos lançamentos são.

**A lição que atravessa as três primeiras:** nenhuma delas quebra a tela. Todas
devolvem 200 e um número plausível. Teste que só confere status não pega nada
disso — é a mesma lição do filtro que não pegava, em 03/09, e é a terceira vez
que ela aparece neste arquivo.

### A queda de 04/09/2026 — a senha com acento

O dono abriu o painel e recebeu a tela de erro com **"comparing strings with
non-ASCII characters is not supported"**.

**O que aconteceu de verdade:** ele estava **errando a senha**, e o que digitou
tinha acento. Em vez de "senha incorreta", levou a tela de erro. A senha
configurada não tem acento — **ninguém ficou trancado fora**, e não houve
indisponibilidade.

**Causa:** o `hmac.compare_digest` do Python, usado para conferir a senha em
tempo constante, **recusa texto com qualquer caractere fora do ASCII** — e não
devolve `False`: levanta `TypeError`. Basta o texto DIGITADO ter acento para
derrubar a tela.

**O caso pior, que não aconteceu mas era possível:** se a `PAINEL_SENHA`
configurada tivesse acento, ninguém entraria nunca, nem sabendo a senha. Fica
registrado porque a correção fecha os dois casos, e porque trocar a senha para
uma com acento era uma armadilha esperando.

**Correção:** comparar **bytes** em vez de texto. O `compare_digest` aceita
bytes de qualquer conteúdo e continua sendo tempo constante. Vale também para o
`PAINEL_SECRET` — um acento ali derrubaria a carga da madrugada, e a falha
apareceria de noite, sem ninguém olhando.

Junto veio a **normalização NFC**: "ç" pode ser gravado como um caractere ou
como "c" mais a cedilha, dependendo do teclado. Os dois são iguais na tela e
diferentes em bytes, então a mesma senha digitada no celular e no computador
podia não bater. É o que a RFC 8265 recomenda para senha, e não afrouxa nada:
texto idêntico continua idêntico depois de normalizado.

**Por que nenhum teste pegou:** todos os testes de login usavam
`"segredo-de-teste"` — ASCII puro. **Cenário de teste sem o caractere que
importa não testa o caractere que importa**, e é a segunda vez que essa mesma
lição aparece neste arquivo (a primeira foi o `juros` zerado, em 03/09). Agora
há cinco testes com acento, inclusive o do segredo do agendador.

**O MESMO DEFEITO EXISTE NO ANÁLISE DE SPs**, em três lugares
(`analisesps/auth.py` linhas 120 e 271, `analisesps/web.py` linha 628). Não foi
mexido daqui — é outra área, outro chat. **Foi avisado ao dono.** O ERP não tem
o problema: lá a comparação é entre hashes, que são sempre ASCII.

### A leva de 04/09/2026 — as duas datas no ar, e duas pendências fechadas

**As duas datas foram publicadas** (junção `0bfa75b`) e o dono aplicou a
migração. As colunas nasceram vazias, como estava previsto, e ele teve de
apertar "Só refazer os números" — o que o incomodou, com razão. Fica a lição:
**entrega que exige um clique do dono para valer é entrega pela metade.**
Migração que cria coluna derivada deveria deixar a reconstrução agendada
sozinha. Proposto a ele e não decidido.

**Quanto o painel ficou rápido: medido.** O dono leu o rodapé — **478 ms de
tela, 8 consultas, 443 ms delas no banco**. Ou seja **93% do tempo é banco**, e
o código gasta 35 ms. Isso encerra a dúvida que estava aqui desde 03/09: não
adianta mexer em Python para acelerar; o que sobra está nas consultas.

Medido aqui também, com banco de verdade, quantas consultas cada tela faz:
Analítico 7, Visão Geral 7, DRE 6, Fluxo 4, Resultado por Obra 4. Dessas, no
Analítico, **3 eram a mesma pergunta trivial repetida** — o carimbo da última
carga, que o `_lembrando` consultava a cada chamada.

**Corrigido no fim do dia:** o carimbo passou a ser perguntado **uma vez por
requisição**, guardado no `g` do Flask (que morre junto com a requisição, então
não há risco de carimbo velho sobreviver a uma carga nova). Fora de requisição —
na carga, que roda em processo separado e vive horas — não guarda nada e
pergunta sempre. **O Analítico caiu de 7 para 5 consultas.** Há teste contando
as idas ao banco numa mesma tela, e outro exigindo que carga nova continue
jogando fora a lista velha.

**A mensagem duplicada em Configurações, corrigida.** A causa era pior que
"texto repetido": cargas mortas acontecem em série (a causa é o serviço
reiniciar, e toda publicação reinicia), então a linha "Última atualização" e a
caixa vermelha mostravam a **mesma frase sobre duas execuções diferentes** —
quem lia procurava dois problemas onde havia um. Agora, com a caixa na tela, a
linha de cima passa a responder outra pergunta: quando a base foi atualizada de
verdade. E parou de dizer "Nenhuma atualização feita ainda" quando houve
atualização e ela morreu.

Para isso a execução encerrada pelo faxineiro de órfãs passou a ser
**reconhecível**: quem termina sozinho zera a `etapa`, quem morreu a mantém.
Isso já acontecia por acaso; agora é de propósito, escrito e com teste.

**Cenários de rateio: convertidos.** Era o item 5 da lista de pendências e a
penúltima tela do Streamlit sem equivalente. A pessoa mexe em **%**, **escopo**,
**vigência** e **liga/desliga** de cada regra, clica em Recalcular, e vê obra a
obra o que mudaria — com os quatro números do topo (rateado e não rateado, dos
dois lados), a tabela de diferenças e um gráfico do Δ Resultado. Nada toca o
banco até apertar Gravar, atrás de uma confirmação.

Três decisões que valem registro:

- **O cenário viaja na URL, não numa sessão.** O serviço roda com um worker e
  reinicia a cada ~150 requisições: estado de simulação em memória não
  sobreviveria. Na URL sobrevive, e o link dá para mandar para o contador.
- **Gravar é UPDATE por id, não DELETE + INSERT.** A tela antiga apagava todas
  as regras e reinseria — os ids mudavam a cada gravação. E só os parâmetros são
  gravados: grupos e categorias continuam sendo da tela de Regras, onde existe a
  lista para escolher.
- **Uma leitura do banco para as duas contas.** A tela roda a apuração duas
  vezes (gravado e cenário) sobre os mesmos dados. Ler duas vezes varreria o
  fato em dobro; a primeira versão fazia isso e custava 17 consultas por tela.
  Agora são 8.

O que **não** foi convertido do original, de propósito: **acrescentar regra
nova** dentro do cenário. O editor antigo permitia, mas com as colunas de grupo
e categoria desabilitadas — uma regra nascia sem saber o que pega. Criar regra
continua sendo na tela de Regras, e a tela diz isso.

**O PDF do DRE: a conversão terminou.** Era o ultimo item sem equivalente, e
ficou por ultimo por um motivo concreto — o gerador original usa `reportlab`,
que **não está no serviço**. Feito com `fpdf2`, que já está: **nenhuma
dependência nova**.

A decisão que faz o relatório ser confiável: **o PDF recebe as MESMAS abas que o
Excel**. Quem monta o relatório completo é a rota de download, num lugar só. Não
há um montador por formato — se houvesse, o dia em que os dois discordassem
ninguém saberia qual está certo. A única diferença é o teto: o PDF corta cada
seção em 2.500 linhas (como o original fazia) e **escreve na página** que
cortou, com o caminho para a planilha.

Duas armadilhas resolvidas, e ambas com teste que acusa se alguém as desfizer:

- **O acento.** Com a fonte embutida, o `fpdf2` só escreve latin-1 — e o que não
  couber **estoura no meio da geração**, sem avisar. O português inteiro cabe;
  os sinais tipográficos não (travessão, aspas curvas, reticências), e eles
  estão espalhados pelos textos deste projeto. Todo texto passa por `_texto()`.
  É o mesmo caminho que o `emissaonf` e o `analisesps` já usam — **e o código
  não é compartilhado de propósito**: cada área é mexida por uma sessão que não
  conhece as outras, e o relatório do painel não pode quebrar porque alguém
  ajustou o PDF de outro módulo.
- **O texto que não cabe na coluna.** Sem cortar, o `fpdf2` escreve por cima da
  coluna vizinha e a tabela vira mancha justamente nas linhas mais longas.

**O gráfico do PDF usa a geometria do `graficos.py`** — a mesma que desenha o
SVG na tela, redesenhada com as primitivas do `fpdf2`. Recalcular aqui abriria a
porta para o gráfico do PDF e o da tela contarem histórias diferentes.

**Memória medida**, porque é o recurso escasso: o pior caso que o teto permite
(oito seções de 2.500 linhas, 20 mil linhas) gera um PDF de 1,2 MB com **pico de
32,5 MB**. Para comparar: o painel antigo gastava 179 MB só para abrir a
primeira tela.

**Ainda sem conferência contra a base real:** os números do cenário. A conta foi
conferida à mão num caso montado (CASA −750, PREDIO −250, PONTE +1.000, com o
resíduo caindo do lado certo) e há teste com banco de verdade exigindo esses
valores — mas nenhum deles olhou a base da empresa.

### A leva de 03/09/2026 — o que o uso real mostrou

Publicado o DRE, o dono usou o painel de verdade pela primeira vez. Saiu daí:

**Um defeito que nenhum teste pegaria e que estava em duas telas.** Escolher um
valor no filtro e clicar em Aplicar não fazia nada — a tela voltava ao que era,
sem erro. O formulário mandava o mesmo campo **duas vezes**: escondido com o
valor velho (para "levar os filtros adiante") e na lista com o valor novo. Vira
`?visao=comprometido&visao=aberto`, e o Flask fica com o primeiro. Valia para
qualquer campo que já tivesse valor, no Analítico **e** no DRE.

A lição: um filtro que não pega é invisível para teste que só confere status
200. `tests/test_painel.py` agora varre **todos os formulários de todas as
telas** atrás de nome repetido — a classe inteira, não o caso.

**Oito varreduras da base para mostrar uma tela.** Abrir qualquer tela varria
as 185 mil linhas cinco vezes antes de chegar na consulta que interessa:
`base_vazia()` fazia `COUNT(*)` para saber se existia UMA linha, e as listas de
Ano/Projeto/Obra (mais Grupo/Categoria no Analítico) eram refeitas a cada
clique para devolver sempre a mesma coisa. Agora ficam guardadas na memória do
processo, com o carimbo da última carga como chave.

**Por que o carimbo é seguro:** toda atualização, nos quatro modos, abre e
fecha uma linha em `execucoes` (`tarefas.py`). O fato não muda sem isso, então
lista velha depois de carga nova não é possível. Nos **testes** é, porque eles
trocam o banco na mão — daí a fixture `autouse` no `conftest.py`.

**E um cronômetro, porque "está lento" não é acionável.** O rodapé mostra o
tempo da tela, quantas consultas foram e quanto delas foi banco; consulta acima
de 1 segundo sai no log com o começo do SQL. Sem número, otimizar é adivinhar —
e este ramo melhora o que dava para provar que era desperdício, não o que
parecia lento.

**Os pedidos de quem usa:** filtro que sobrevive à troca de aba (antes as abas
do topo iam para a tela limpa); rótulos de data que não se amontoam no Fluxo
Financeiro (o gráfico de linhas já pulava rótulos, o de barras não); faixa de
data no Analítico; e — porque o dono perguntou "que data é essa?" — está
escrito na tela que a data é a do pagamento quando quitado e a do vencimento
quando em aberto.

**Quatro dados que estavam no banco e não apareciam em tela nenhuma:** situação
do vencimento (quitado / vencido / a vencer), pedido de compra, medição e o
número do lançamento no OMIE. Vale procurar por outros antes de inventar
coluna nova.

### A lição de 02/09/2026 — fidelidade vem antes de gosto

A primeira versão do DRE tinha metade da tela antiga. Eu havia decidido por
conta própria que as abas de Receitas, Top Credores e o bloco inteiro de
Aportes não eram necessárias, e reescrevi os rótulos das linhas. O dono abriu
a tela e disse, com razão:

> *"Levei muito tempo pra construir o que tinha, pra simplesmente mudar. Tô
> achando que era melhor ter deixado o Streamlit tal qual estava."*

Ele chegou a pedir para voltar ao Streamlit. **Converter não é redesenhar.**
Quando a tela nova tira coisa da antiga, quem perde é quem já sabia usá-la — e
o ganho técnico não compra isso. O que se pode melhorar é o que ele reclamou:
o filtro de obras com mais de cem itens, que não tinha busca.

Antes de mexer numa tela, abra a original em `referencia_streamlit/` e confira
item por item. O que sair, sai porque **ele** decidiu, não porque pareceu
supérfluo.

### Erro numérico que isso escondeu

Na pressa de simplificar, a linha **"Juros e Multas Pagos"** ficou de fora do
DRE. Não era só uma linha a menos na tela: os encargos sumiam do total de
custos, e o resultado saía maior do que é. Está de volta.

### A planilha voltou a fechar com o DRE

Os encargos entram no DRE mas **não têm categoria** no plano financeiro do
OMIE. Por isso a planilha antiga acrescentava, de propósito, uma linha
"Juros e Multas Pagos" na aba de categorias — sem ela, duas abas do mesmo
arquivo mostram totais diferentes, e quem soma a de categorias acha que a
despesa é menor do que o próprio arquivo diz.

Essa linha não tinha sido convertida. Voltou (03/09/2026), **só na planilha**:
na tela a aba de despesas continua sendo o que veio do plano de contas, como no
Streamlit. Há teste com banco de verdade exigindo que as duas abas fechem.

### E o teste que deveria ter pego isso não rodava no PC

Ao mudar o formato do DRE, o teste `test_dre_fecha_de_cima_a_baixo` ficou lendo
o formato antigo e quebrou — mas ele é `@pytest.mark.banco`, e **sem Postgres
local é pulado calado**. A suíte no PC deu tudo verde; quem acusou foi o GitHub
Actions, que sobe o banco.

Duas consequências práticas, que valem para qualquer mudança aqui:

- **Verde no PC não é verde.** São ~158 testes pulados sem banco, e são
  justamente os que olham o SQL. Antes de pedir para publicar, conferir o
  resultado do GitHub Actions do ramo — ou subir o `docker-compose.teste.yml`.
- **Cenário de teste com campo zerado não testa o campo.** O `juros` e a
  `multa` do cenário eram zero em todas as linhas, então o SQL novo dos
  encargos não era exercitado por nenhum teste com banco de verdade. Agora o
  cenário tem encargo pago (entra) e encargo previsto num título em aberto
  (não entra).

---

## Regras que não se discutem

Cada uma custou horas. Não são preferências.

### 0. O dono deu autorização permanente para publicar — com duas exceções

Em 04/09/2026, cansado de ser perguntado seis vezes no mesmo dia, o dono
autorizou: **publicar sozinho sempre que a suíte estiver verde e não houver
migração de banco.** Continuam exigindo a pergunta:

- **mudança com migração**, porque ela precisa do clique dele no mesmo momento;
- **qualquer suspeita de carga rodando**, pelo motivo da regra 1 abaixo.

**Isto vale só para a área do painel.** Não foi escrito no `CLAUDE.md`, que as
três áreas leem: o ERP e o Análise de SPs são outros chats, e o dono não
autorizou nada para eles. Foi um deles que matou uma carga juntando na main sem
saber — a regra 1 nasceu disso.

### 1. Não publique na `main` enquanto uma carga estiver rodando

Publicar reinicia o serviço no Render, e o reinício mata a carga. Isso já
aconteceu **duas vezes**, uma delas causada por outra sessão do Claude
trabalhando no ERP, que juntou um ramo na main sem saber que havia carga em
curso.

**Pergunte antes de publicar. Sempre.** Trabalhar num ramo é seguro; juntar na
main não é.

### 2. Trabalho longo não roda dentro do gunicorn

O serviço sobe com `--workers 1 --max-requests 150`. O gunicorn **reinicia o
processo** a cada ~150 requisições — proteção contra vazamento de memória, posta
depois do OOM de julho de 2026. Com um worker só, esse reinício leva junto
qualquer thread de fundo.

Três cargas morreram por isso, sem deixar rastro. E a própria tela de
acompanhamento, consultando de 5 em 5 segundos, era parte do que as matava.

A saída **não** é mexer no `--max-requests`: ele protege os outros 14 módulos.
A saída está em `executar_sync.py` — processo separado e destacado.

### 3. Nada de abrir a base inteira em memória

A instância tem 2 GB, divididos com 14 módulos, e já teve OOM. O painel antigo
abria um arquivo de 4 MB que virava **179 MB** na memória. Hoje a reconstrução
inteira usa **14,6 MB** porque quem soma é o Postgres.

### 4. Tabelas em schema próprio

O ERP tem tabelas chamadas `titulos`, `categorias` e `rateios`. O espelho do
OMIE tem tabelas com esses mesmos nomes e significado completamente diferente.
Schemas separados são o que faz as duas conviverem.

### 5. O SQL é Postgres, não SQLite

O código do espelho nasceu falando com SQLite. **Três** construções que só
existem lá chegaram em produção, cada uma custando uma carga inteira:
`GROUP BY <apelido>`, `conn.cursor()` sem tradução, e `MAX(a, b)`.

`tests/test_painel_sql_portavel.py` varre a classe inteira desse problema.
Rode-o sempre que tocar em SQL.

### 6. A produção não é alcançável a partir dos testes

O `.env` da raiz aponta para o banco de **produção** — é assim que se
desenvolve local. Um teste que esqueça de dublar a conexão vai lá; aconteceu.
`db.py` agora recusa qualquer banco que não seja local e com "teste" no nome
enquanto o pytest roda.

### 7. Como falar com o dono

Ele não é programador. Português simples, o efeito e não a implementação, e o
risco sempre explícito. Nada de esconder o que não foi testado atrás de
"está pronto".

---

## Decisões já tomadas — não reabrir sem motivo novo

| Decisão | Por quê |
|---|---|
| Postgres do ERP, schema `painel` | O disco do Render é apagado a cada reinício; a configuração da prestação de contas não é regenerável |
| Módulo do serviço que já existe, não serviço novo | Sem custo adicional; reusa login, deploy e banco |
| Gráficos em SVG desenhado na página | O Plotly custava 3 MB de JavaScript por tela |
| Exportação em `.xlsx` (`openpyxl`), **não** CSV | Revertida em 03/09/2026: o relatório tem oito abas, e em CSV isso vira oito arquivos soltos. O dono pediu Excel. Escreve célula a célula, sem `pandas` |
| `pandas` não é dependência do painel | A única parte que o usava foi feita em Python puro |
| Migrações aplicadas por botão, nunca no boot | Uma migração com defeito no start derrubaria os 15 módulos juntos |
| Hora convertida para Brasília **na fonte** | O servidor roda em UTC; se cada tela convertesse, uma esqueceria |

---

## A conferência dos números

Cada tela foi comparada com a original, sobre a base real:

- a tabela `fato`: **185.422 linhas nas duas**, diferença máxima de **R$ 4,60**
  em R$ 343 milhões — arredondamento para centavos ao gravar;
- a prestação de contas, sócio a sócio: diferença máxima de **R$ 0,22** em
  R$ 11 milhões.

Se alguma tela for mexida, refaça a comparação. As telas originais estão em
`referencia_streamlit/` justamente para isso.

---

## O banco de produção recusou conexão em 03/09/2026 — passou

Na manhã de 03/09 o Postgres respondeu:

    FATAL: role "erp_admin" is not permitted to log in

Não era senha errada: era o servidor recusando o usuário. Na tarde do mesmo dia
o dono confirmou que painel e ERP voltaram a abrir. Fica registrado porque, se
acontecer de novo, **ERP e painel caem juntos** — usam o mesmo banco — e o lugar
de olhar é a instância do Postgres no Render, não a senha.

O que aquele dia deixou pendente continua pendente: o SQL do **bloco de aportes
nunca rodou contra a base real**. Ele passou pelo parser do Postgres, pelo teste
de portabilidade e pelos testes com dublê — nenhum dos três olha o número que
sai.

## O que falta

**A conversão do painel terminou** — dez telas e o relatório em PDF, tudo o que
o Streamlit fazia. O que resta não é código a escrever; é **conferência com dado
real**, e ela depende de alguém abrir a tela publicada.

### Conferir com a base da empresa (só o dono consegue)

1. **O bloco de aportes do DRE.** Nunca viu dado de verdade. É o pedaço mais
   antigo nessa condição, e este módulo já mandou três erros de SQL para a
   produção.
2. **As duas datas e o atraso**, no Despesas Analítico, contra o OMIE.
3. **Os cenários de rateio:** simular uma mudança óbvia e ver se o efeito bate
   com a intuição de quem conhece as obras.
4. **O PDF contra a planilha** do mesmo recorte. Por construção os dois saem das
   mesmas abas — o teste é confirmar isso com dado real.
5. **Se o arquivo do Analítico agora traz o mesmo número da tela.** Era 481 na
   tela e 316 no arquivo; a causa foi corrigida em 04/09 e a confirmação é do
   dono.

### Decidido e feito

6. **Migração que cria coluna derivada agenda a reconstrução sozinha.**
   ~~Proposto e não decidido~~ — o dono aprovou em 04/09/2026 e está feito. A
   migração DECLARA isso no próprio arquivo, com a marca `REFAZER-O-FATO`; ao
   terminar de aplicar, o painel dispara "Só refazer os números" e escreve na
   tela que o recálculo começou. Ninguém mais precisa descobrir sozinho que
   tinha um clique faltando.

### Fora desta área

7. **O mesmo defeito da senha com acento existe no Análise de SPs**
   (`analisesps/auth.py` linhas 120 e 271, `analisesps/web.py` 628). Não foi
   mexido daqui — outra área, outro chat. **Avisado ao dono em 04/09.** O ERP
   não tem o problema: lá a comparação é entre hashes, sempre ASCII.
8. **Converter `app/apps/spsbd_app`.** Já está em andamento pelo chat da área,
   que publicou várias vezes em 04/09.

### Melhorias possíveis, nenhuma urgente

9. **O que sobrou de lentidão está no banco, não no código.** Medido pelo dono
   em 04/09: 478 ms de tela, 443 deles no banco — 93%. Otimizar Python daqui
   não move o ponteiro; o que resta é SQL e índice.

---

## Coisas pequenas que mordem

- Nunca deixe um arquivo chamado `app.py` solto numa pasta: ele sombreia o
  pacote `app` do projeto. Aconteceu com `referencia_streamlit/app.py`, hoje
  renomeado.
- `/painel/saude` mostra a versão publicada (`RENDER_GIT_COMMIT`). Use isso
  para saber se uma correção já subiu, em vez de clicar e torcer.
- Banco fora do ar dá erro em 10 segundos, não trava a tela. Antes travava.
- A carga inicial **retoma de onde parou**, por etapas marcadas no banco. Uma
  carga concluída apaga as marcas — senão a próxima pularia tudo e não faria
  nada.
