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

**Dez telas convertidas**, todas conferidas contra a versão original com dados
reais: Visão Geral, DRE, Despesas Analítico, Receita de Obra, Fluxo de Caixa,
Resultado por Obra, Comprometido × Executado, Necessidade de Caixa, Prestação de
Contas e os Cenários de rateio — mais o relatório em PDF, o **Explorador** e o
**Rateio da Administração**.

**Estado em 13/09/2026:** a conversão terminou e o painel já passou do que o
Streamlit fazia. Em 09/09 (`07b026a`) subiram o Explorador, a **alteração de
classificação no OMIE** e o **Rateio da Administração**; o dono aplicou as
migrações 007 e 008 e criou a `PAINEL_SENHA_ESCRITA` no Render. Em 13/09 o
Explorador foi **reprovado por ele e refeito**: filtros na barra da esquerda,
com busca e marcação de vários, e **uma lista só**, editável linha a linha ou
em lote — ver a seção própria mais abaixo.

### O que está pendente AGORA

**Nada de código.** O que falta é conferência com dado real, e duas coisas
merecem destaque:

1. **A primeira escrita no OMIE nunca aconteceu.** O caminho que grava foi
   testado só contra dublê. Protocolo: ensaio → **um** título conferido dentro
   do OMIE com os olhos → só então lote.
2. **O saneamento da base em si** — os empréstimos classificados fora do lugar,
   os títulos sem obra. A ferramenta está pronta; o trabalho é de quem conhece
   as obras.

O resto está em "O que falta", no fim deste arquivo.

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

### 0. A autorização permanente para publicar FOI REVOGADA — 13/09/2026

Em 04/09/2026 o dono havia autorizado publicar sozinho com a suíte verde. **Em
13/09/2026 ele cancelou essa autorização, com todas as letras:**

> *"só publique algo se eu autorizar."*

Cancelou porque eu publiquei antes de ler o resultado da suíte — que já estava
vermelha — e o Explorador ficou fora do ar em produção. A regra hoje é simples e
não tem exceção: **nada vai para a `main` sem ele dizer "pode".**

Junto com isso ele pediu duas coisas:

- *"nao publique nada pq analisesps tá publicando"* — confirmar que não há outra
  área publicando nem carga rodando (regra 1 abaixo);
- *"me avise quando tiver aguardando minha autorizacao"* — terminar o trabalho no
  ramo e **avisar** que está pronto, em vez de ficar calado esperando.

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

## O Explorador foi reprovado e refeito — 13/09/2026

O dono abriu a primeira versão e reprovou, com razão. Três defeitos, e nenhum
deles era detalhe:

1. **Os filtros estavam no alto da tela**, e em todas as outras telas do painel
   eles ficam na barra da esquerda. Quem usa o painel todo dia tropeça.
2. **Escolher dois grupos ou duas categorias era, na prática, impossível.** Eram
   listas de rolagem `<select multiple>`: aceitavam vários itens, sim, mas só
   segurando Ctrl — e uma dica escrita embaixo não conserta uma interação que
   ninguém descobre. Vale a frase dele: *"eu posso buscar só um grupo ou só uma
   categoria, assim não funciona"*.
3. **Havia DUAS listas de lançamentos**: a de procurar, embaixo, e outra dentro
   do bloco "Alterar no OMIE", com as caixas de marcar. Quem achava o lançamento
   numa tinha de reencontrá-lo na outra. *"Basta uma lista e a gente vai
   trabalhar em cima dessa lista."*

### Como ficou

**Os filtros foram para a esquerda**, na mesma barra do resto do painel, e
viraram listas de marcar com busca: análise, grupo, categoria, obra, projeto,
conta corrente e situação. Marca-se quantos quiser em cada uma, a busca ignora
acento, o que está marcado sobe para o topo, e há "marcar os que aparecem". Nada
disso foi inventado: é a mesma peça que a barra padrão já usava — ela saiu de
dentro do `painel_filtros.html` e virou `painel_filtros_lista.html` mais
`static/filtros.js`, usados pelas duas. Uma cópia de cada lado divergiria na
primeira correção.

**Sobrou uma lista só, e é nela que se edita.** Dois jeitos de trabalhar, os
dois que o dono descreveu:

- **Um a um:** clicar na célula de Categoria ou de Obra de qualquer linha e
  escolher a nova no seletor que abre ali.
- **Em lote:** marcar várias linhas e usar os botões do alto — "Mudar a
  categoria dos marcados", "Mudar a obra dos marcados" —, que valem para todas
  de uma vez.

Os dois se misturam à vontade: dá para mandar trinta títulos para uma obra e
depois corrigir dois deles individualmente, tudo antes de enviar.

### O que se decidiu no caminho

**Nada é enviado enquanto se edita.** O que se monta na tela é um *rascunho*:
a célula mostra `valor de hoje → valor novo`, a linha fica amarela, e um contador
diz quantos títulos estão pendentes. Só o Ensaiar e o Alterar de verdade falam
com o OMIE. Sem isso, um clique errado numa célula seria um envio.

**Só a célula que mudou aparece riscada.** A primeira versão riscava a linha
inteira — quem trocasse só a categoria via a obra riscada também, e a tela
estava mentindo sobre o que o botão ia fazer. Apareceu em captura de tela do
navegador, não em teste: número nenhum pega isso.

**Um título rateado em três obras aparece em três linhas, mas é UM cadastro no
OMIE.** Então marcar qualquer uma das linhas marca as três, editar uma edita as
três, e o envio manda **uma** chamada. O contador diz "1 título marcado", não
"3 linhas". Fingir que são registros separados seria mentir sobre o que o botão
faz — e mandaria o mesmo título três vezes para o OMIE.

**O servidor passou a aceitar um destino POR TÍTULO** (`normalizar_alvos`, em
`saneamento.py`), em vez de um valor único para todos. A forma antiga continua
valendo, e não por preguiça: é ela que funciona com o JavaScript travado, e é
ela que os testes exercitam desde o começo.

**O seletor de categoria é um só na tela inteira**, que se move para perto de
quem o chamou. São centenas de categorias: desenhar a lista dentro de cada uma
das até 3.000 linhas seriam centenas de milhares de elementos. Este painel já
morreu de falta de memória uma vez (§9 do `CONTEXTO.md`) — não é hipótese.

**O teto de 200 títulos por envio agora avisa enquanto se edita**, não só depois
de enviar. Descobrir o limite depois de montar duzentas alterações seria cruel.

### O que ficou de fora, e por quê

**O ensaio depende do OMIE estar no ar.** A recusa por rateio é calculada pelo
espelho do próprio painel e não precisaria de rede nenhuma, mas hoje ela só
aparece depois de o painel conseguir falar com o OMIE. Se a API estiver fora, o
ensaio inteiro falha em vez de ao menos listar os títulos rateados. Não mexi
nisso agora: é o caminho que escreve, e alargá-lo sem pedido não vale o risco.

**A escrita continua sem nunca ter tocado a API de verdade.** Nada nesta leva
mudou isso. O protocolo segue igual: ensaio → **um** título conferido dentro do
OMIE com os olhos → lote.

### O que foi conferido antes de publicar

Suíte completa com banco de verdade, já com o Análise de SPs junto na `main`:
**5038 passaram, 129 pulados**. Os treze testes novos foram conferidos
quebrando o código de propósito, um a um — inclusive o da trava do rateio, que
passava sem provar nada até ganhar a asserção de que a mudança pedida acontece
mesmo. A tela foi exercitada em navegador de verdade: editar uma linha, marcar
três e mudar todas, desfazer, e o título rateado virando um envio só. Publicado
em `86636c7`, sem migração.

## O empréstimo que devolveu mais do que entrou — 09/09/2026

A base diz que a empresa pagou **R$ 9,25 milhões** de principal contra
**R$ 9,08 milhões** tomados. Isso não fecha sozinho: ninguém paga principal de
dinheiro que não tomou. Só há duas explicações — parte dos empréstimos é
anterior ao período que a base cobre, ou há título de empréstimo classificado em
outra categoria no OMIE.

A segunda explicação é a cara: um empréstimo classificado como despesa comum
**vira custo de obra em todas as telas**, não só na Necessidade de Caixa. O
resultado da obra fica pior do que é, e ninguém desconfia, porque o número
sai limpo.

**O que foi feito:** a leitura em português da Necessidade de Caixa passou a
dizer isso quando acontece, com os dois valores e a conclusão, e a apontar o
Explorador como o lugar de corrigir. O que **não** foi feito — porque não é
código — é sanear a base: isso é olhar os lançamentos das categorias de
empréstimo no Explorador e reclassificar o que estiver errado.

A escolha foi deliberada: o painel não deve adivinhar nem "consertar" o número
por dentro. Um ajuste automático esconderia o erro de classificação em vez de
mostrá-lo, e o erro continuaria contaminando as outras telas em silêncio.

## A tarde de 13/09/2026: um centavo custou uma tarde

O dono quis conferir as devoluções de aporte de uma empresa contra a tela do
OMIE. Sete lançamentos lá, dois no painel. Levantei **cinco** explicações e ele
derrubou **quatro**, sempre com dado:

1. *"A data do painel é outra"* → ele: está conciliado em 24/12, na data certa.
2. *"É lançamento de conta corrente, não título"* → ele: os dois foram lançados
   igual, e abriu os dois para conferir.
3. *"A janela da carga de pagamentos não alcança data retroativa"* → ele tirou o
   filtro de data e continuou sem aparecer.
4. *"O nome do fornecedor está vazio"* → o recálculo já tinha a correção e não
   mudou nada. (E eu ainda errei o fuso ao comparar os horários, e o fiz
   recalcular à toa.)
5. *"O título está rateado, então cada linha tem uma fração"* → ele: não é
   rateado.

**A causa real:** o lançamento estava no painel o tempo todo. A busca por valor
comparava por **igualdade exata**, e o valor guardado é `784.647,06` — o OMIE
mostra `784.647,07`. Um centavo. A busca errava por um centavo e afirmava que o
lançamento não existia.

### Por que falta esse centavo — e onde mais ele falta

Todas as colunas de dinheiro do espelho (`titulos`, `movimentos`, `rateio`) são
**`REAL`**: ponto flutuante de 4 bytes, ~7 dígitos significativos. Acima de
**R$ 131.072** o menor passo representável passa de um centavo, e o valor gravado
deixa de ser o valor real. Não é só o `valor_documento`: é o valor pago, juros,
multa, os impostos retidos e os valores do rateio.

A tabela `fato` usa `NUMERIC(16,2)`, exata — mas ela é **calculada a partir
das colunas `REAL`**, então guarda com precisão um número que já veio errado.
Ou seja: **as telas carregam esse erro de centavos em todo título grande.**

**O tamanho, medido e não estimado no susto:** centavos por título, e só nos
grandes. Um título de R$ 8 milhões pode estar até ~50 centavos fora. Somada a
base inteira, a diferença deve ficar em poucos reais. Não move decisão de
negócio nenhuma. **Atrapalha exatamente uma coisa: bater o painel com o OMIE no
centavo** — que era o que o dono estava tentando fazer.

### O que foi feito, e o que não foi

**Feito:** toda busca por valor (no Explorador e na conferência da base crua)
compara com **meia unidade de folga**, não por igualdade. Há teste provando que
a folga acha o centavo perdido e que não confunde dois títulos próximos.

**Não feito, e é decisão de negócio:** trocar as colunas para `NUMERIC`. Não
adianta converter o que está gravado — a precisão se perdeu na escrita, o número
certo não está mais lá. Seria migração **mais uma carga completa do OMIE**, que
pelo próprio código leva horas. Recomendei não fazer agora: o dano prático era a
busca, e ele está resolvido. Fica como dívida conhecida. No dia em que for
preciso conferir no centavo, é isso que tem de ser feito.

### A lição que vale mais que o conserto

Gastei a tarde inteira raciocinando sobre o código sem enxergar a base do dono,
e cada hipótese custou uma ida e volta com ele. **A ferramenta que resolveu — o
campo "procurar na base crua", em Configurações — levou vinte minutos para ser
feita e respondeu na primeira tentativa.** Devia ter sido a primeira coisa, não
a sexta. Quando o dado está do outro lado, construir o instrumento é mais barato
que adivinhar.

## A madrugada de 20/09/2026 — o extra derrubou o essencial

A atualização automática das 03:45 terminou assim:

    remove: path should be string, bytes or os.PathLike, not NoneType

**O erro em si é bobo.** No fim da varredura de títulos excluídos, os arquivos
de checkpoint são apagados; um dos caminhos vinha vazio, e `os.remove(None)`
levanta `TypeError` — que **não é `OSError`**, então passava direto pelo
`except (FileNotFoundError, OSError)` que existia ali para tolerar exatamente
esse tipo de coisa.

**O estrago não foi o erro, foi onde ele caiu.** A varredura de excluídos roda
ANTES da etapa que refaz os números das telas. Estourando ali, a carga inteira
foi para o `except` de cima e o recálculo **não rodou**. Resultado: a base do
OMIE atualizou e as telas continuaram mostrando número velho — sem ninguém
perceber, porque a tela dizia "falhou" num erro que parecia de arquivo temporário.

Dois consertos, e o segundo é o que importa:

1. `_ckpt_remover` ignora caminho vazio e engole qualquer exceção. Limpeza de
   arquivo temporário nunca derruba carga.
2. **A varredura de excluídos virou etapa não essencial.** Se ela falhar, a
   falha é registrada e a carga **segue** para o recálculo. A mensagem final
   ganha um `ATENÇÃO: ...` dizendo o que não foi feito — "concluída" não pode
   virar meia-verdade.

A regra que fica: **achar título apagado no OMIE é um extra semanal; refazer os
números é o que faz a tela valer. O extra nunca mais custa o essencial.**

Três testes em `tests/test_painel_carga.py` seguram isso: o caminho vazio, o
arquivo que existe continuando a ser apagado (tolerar vazio não pode virar
tolerar tudo) e a varredura falhando sem impedir o recálculo. Os três foram
conferidos ao contrário — com o conserto desfeito, os três quebram.

## O botão das conferências em silêncio — 20/09/2026

O dono: *"EU JÁ havia tentado rodar mas não tava apresentando resultado."*

A tela não tinha como contar o que houve. As quatro conferências rodavam em
sequência dentro da mesma requisição; qualquer erro numa delas levava a página
inteira para a tela de erro do painel — e sem dizer qual das quatro foi. Pior:
o teste que existia trocava as quatro por dublê, então provava que **o botão
chama**, nunca que **o resultado chega**. Um erro de SQL em qualquer uma passava
verde aqui e só aparecia na tela dele.

O que mudou:

- cada conferência roda por si. A que falhar aparece **nomeada**, em vermelho,
  com o erro escrito na tela; as outras três seguem. Elas só leem — falha de
  uma é informação, não motivo para esconder as outras;
- a caixa "Conferências da base" fica sempre visível, diz quando elas acabaram
  de rodar e traz **"Rodar de novo"**;
- o botão leva âncora: a página volta já no lugar dos quadros;
- se as quatro voltarem vazias **sem erro**, a tela diz isso — antes, vazio e
  quebrado eram indistinguíveis.

Um teste novo aperta o botão **com banco de verdade e sem dublê** e exige que os
quadros cheguem; outro quebra uma de propósito e exige a tela de pé com o nome
da que falhou.

A lição, que é a mesma do centavo de 13/09: **dublê prova que o caminho existe,
não que ele entrega.** Onde o dono aperta um botão, o teste tem de apertar o
mesmo botão.

## A devolução de R$ 784.647,07 — era o AGRUPAMENTO, 20/09/2026

Fim da caçada que durou de 13 a 20/09. A cascata de conferência, rodada com a
base real, deu o veredito:

| Corte | Devolvido | Quanto levou |
|---|---|---|
| Tudo com categoria de aporte | R$ 2.745.993,08 | — |
| Só o que entra no saldo | R$ 2.745.993,08 | — |
| Tirando transferências | R$ 2.745.993,08 | — |
| Tirando o que não é pago | R$ 2.745.993,08 | — |

**Corte nenhum come nada.** O dinheiro está na base e entra na soma geral — o
que o escondia era o **agrupamento**. O bloco somava por razão social, e a mesma
empresa com dois cadastros no OMIE (ou com o nome escrito de dois jeitos) virava
duas linhas, cada uma menor do que a empresa é. Os R$ 567 mil que ele via eram
uma das linhas; a de R$ 784.647,07 estava logo ali, com outra grafia do mesmo
nome.

O conserto: **a identidade passou a ser o CNPJ/CPF, só os dígitos** — assim
"12.345.678/0001-90" e "12345678000190" são a mesma empresa. O nome virou só
rótulo. Sem documento, cai no nome em maiúsculas, que é o que dava antes —
nunca pior. Vale para o recorte por sócio, por obra e para o quadro de
dividendos.

E, para o dono poder **corrigir no OMIE** em vez de só conviver, a conferência
ganhou a tabela "Devoluções por contraparte", que mostra quantas grafias
diferentes cada documento tem e marca em vermelho as repetidas.

Dois testes com banco de verdade: um cria a mesma empresa com duas grafias e o
mesmo CNPJ e exige uma linha só, com a soma certa; o outro exige que a tela
DENUNCIE a repetição — juntar em silêncio não basta, senão o cadastro errado
nunca é arrumado no OMIE.

### O que a mesma rodada de conferência revelou, e ainda espera decisão

- **R$ 96.750,00 em 2 títulos com status ATRASADO** que a carga deu por pagos e
  nenhuma tela conta. A carga aceita "liquidado" na baixa; as telas leem só o
  texto do status. Corrigir faz o DRE e a Visão Geral subirem — é migração e
  decisão do dono.
- **0 de 120.772 títulos têm a observação do OMIE.** O dono desconfiou em 17/09
  e estava certo: o `backfill_observacoes` nunca rodou. São ~120 mil consultas,
  uma por título. Os 2.137 a receber sairiam em minutos.

## A varredura dos dados errados — 20/09/2026

O dono, depois de ver as conferências: *"faça tudo que for necessário pros dados
ficarem o mais correto possível. nao posso trabalhar com dados errados."* Quatro
frentes, todas fechadas nesta leva.

### 1. O centavo (migração 010) — a causa raiz de sete dias de caçada

As colunas de dinheiro do espelho nasceram **REAL**, float de 4 bytes, que
guarda ~7 algarismos significativos. **Acima de R$ 131.072,00 o centavo não
cabe.** Foi assim que R$ 784.647,07 virou R$ 784.647,06 — e por isso a busca por
valor exato nunca o achava. A `fato` já era NUMERIC, mas é CALCULADA a partir
dessas colunas: guardava com precisão o número errado.

Agora são NUMERIC(16,2) em `titulos`, `movimentos`, `rateio.nvaldep` e
`ajustes.valor`. Percentuais ficaram REAL de propósito: valem no máximo 100 e
nunca chegam perto do limite.

**Trocar o tipo NÃO devolve o centavo perdido** — ele só existe no OMIE. Por
isso a tela ganhou um aviso vermelho que só some quando uma **carga inicial**
terminar bem depois da migração, e o botão "Primeira carga", que ficava
escondido enquanto houvesse base, passou a aparecer sempre.

Detalhe de implementação que vai morder quem mexer: NUMERIC volta como `Decimal`
no Python, e Decimal não soma com float. As leituras passaram a pedir
`::float8` — o banco guarda exato, o transporte usa float de 8 bytes, que tem
dígitos de sobra para centavo.

### 2. "Foi pago?" passou a ter uma resposta só (migração 011)

A carga considerava quitado quem tivesse status pago/recebido/conciliado **ou**
baixa liquidada no OMIE. As telas olhavam só a primeira metade. Título baixado
cujo status ficou "ATRASADO" era dado por pago pela carga e **não era contado
por tela nenhuma** — R$ 96.750,00 em 2 títulos na base do dono.

O conserto não foi repetir a regra da carga nas telas: regra repetida diverge de
novo. A carga já grava a decisão em `situacao_vencimento`; a coluna `pago` passou
a ser essa decisão, e nada mais. A conferência das duas regras continua na tela
como guarda — se um dia voltar a acusar algo, é porque alguém criou uma segunda
regra outra vez.

### 3. O agrupamento por documento

Ver a seção própria acima. Resumo: o bloco somava por nome; agora soma por
CNPJ/CPF.

### 4. As observações viraram um modo da atualização

0 de 120.772 títulos tinham a observação do OMIE. O trabalho que as busca
existia desde sempre, mas só pela linha de comando — ou seja, nunca rodava.
Virou o modo **"Buscar as observações"**, com botão e barra de andamento.

Ele só LÊ do OMIE (há teste que falha se alguém puser Alterar/Incluir ali). Os
títulos a receber (~2 mil, as medições) vão inteiros na primeira rodada; os a
pagar vão em blocos de 8 mil, cerca de uma hora cada, porque 120 mil consultas
são mais de 15 horas e nenhuma publicação de código sobreviveria a isso. É
retomável de verdade: cada título consultado fica marcado e não volta.

### O que continua fora, e é decisão dele

**Movimento de conta corrente sem título nunca entra no painel.** O painel é
montado a partir dos títulos; movimento lançado direto na conta, sem título, não
tem de onde vir. O dono já notou isso ("era pra aparecer todos os lançamentos
igual o relatório de conta corrente do OMIE"). Trazer esses movimentos é fonte
de linha nova, com risco real de contar dinheiro duas vezes — precisa de desenho
e de decisão, não de um remendo.

## O movimento sem título saiu do escuro — 20/09/2026

O dono, depois de eu dizer que isso ficava de fora: *"esse movimento sem titulo,
eu não sei exatamente quem sao e de qual forma afeta. como saber?"*

**Não dava para saber, e a culpa era do código:** a carga descartava esses
movimentos ANTES de gravar (`gravar_movimentos` contava e seguia), então o
número só existia numa linha de log que ninguém lê. Pergunta legítima sem
resposta possível.

Agora eles são guardados em `painel.movimentos_sem_titulo` (migração 012) e há
um quadro na conferência: total, entrou × saiu, por conta, por categoria, por
ano, e os 50 maiores um a um, com data, conta, categoria e contraparte.

**Essa tabela não entra em número de tela nenhum**, e há teste que falha se
entrar. Ela existe para ser olhada. O caminho normal de correção é criar o
título no OMIE — aí o lançamento entra sozinho na atualização seguinte.

Dois casos diferentes, separados na tela porque têm causas diferentes:

1. **movimento sem título nenhum** — lançado direto na conta corrente. É
   dinheiro que o painel não conhece;
2. **movimento que aponta para um título que o painel não tem** — título
   excluído no OMIE depois, ou que a carga não trouxe. Some com uma atualização
   completa.

Cuidado que já está coberto por teste: a atualização do dia apaga a janela de
datas nas DUAS tabelas antes de rebaixar. Sem isso, os sem título seriam
reinseridos toda madrugada e o número cresceria sozinho — erro que só apareceria
semanas depois.

**A tabela só tem conteúdo depois da próxima rebaixa de movimentos.** Enquanto
isso, zero ali quer dizer "ainda não olhei", e a tela diz isso com todas as
letras em vez de mostrar um zero tranquilizador.

## A carga passou a retomar POR PÁGINA — 20/09/2026

**Incidente, e a culpa é minha.** O dono começou a Primeira carga às 18:51 e eu
publiquei código em cima dela. Publicação reinicia o serviço no Render, e o
reinício mata a carga — é a regra 1 deste arquivo, escrita justamente por isso,
e eu passei por cima dela duas vezes no mesmo dia sem perguntar se havia carga
rodando.

**O estrago foi maior do que precisava ser.** A marca de retomada só existia
para etapa INTEIRA, e as contas a pagar — 118 mil títulos, a mais longa das sete
— não tinham terminado. Horas de download jogadas fora. O dono: *"melhor, visto
que posso iniciar e dar outro problema"* — e preferiu esperar o conserto a
recomeçar e arriscar de novo. Ele estava certo.

### Como ficou

A página em que cada etapa está também é gravada, na mesma `sync_state` e com o
mesmo prefixo — então "começar do zero" continua sendo um lugar só.

**As duas etapas longas retomam de jeitos diferentes, e a diferença é o que mais
importa aqui:**

| | Título | Movimento |
|---|---|---|
| Tem chave? | sim, o código do OMIE | **não** |
| Regravar a mesma página | atualiza | **duplica dinheiro** |
| Retoma em | uma página ANTES da salva | exatamente na seguinte |
| Zera a tabela | nunca (é upsert) | só quando começa da página 1 |

O passo atrás do título existe porque o OMIE pode criar títulos entre uma
tentativa e outra, e aí as páginas se deslocam: retomar exatamente na seguinte
poderia **pular** alguns. Regravar uma página não custa nada quando há chave.

No movimento esse truque seria o desastre: sem chave, regravar é somar de novo.

### A defesa que não depende de eu ter acertado

No fim de cada etapa a carga **conta**: o OMIE informa o total de registros, e a
base tem de ter isso. Se faltar (ou, no movimento, se sobrar — sinal de
duplicata), **a etapa é refeita do zero, uma vez**. Se nem assim fechar, a
mensagem final da tela ganha um `ATENÇÃO: ...` dizendo quanto a base tem e
quanto o OMIE diz existir.

Carga que termina com título faltando não pode se anunciar como "concluída" e
mais nada — ninguém teria como desconfiar.

### Um vazamento de memória que estava ali do lado

A etapa de títulos guardava os 118 mil registros numa lista só para calcular, no
fim, a marca d'água do incremental. Isso é exatamente o que a regra de memória
(`CONTEXTO.md` §3.7) proíbe numa instância de 2 GB. A função que grava a marca
já compara com o valor guardado, então passou a ser chamada por página — mesmo
resultado, sem a lista.

## Os aportes, refeitos sobre o plano financeiro — 21/09/2026

O dono mandou os números que espera e os que a tela mostra:

| | Esperado | Painel mostrava |
|---|---|---|
| Aportado | 4.113.536,46 | 3.786.327,89 |
| Devolvido | 3.348.891,66 | **887.000,00** |
| Saldo | 764.644,80 | 2.899.327,89 |

E mandou junto a regra de verdade, que eu não tinha: **o plano financeiro por
código**, e a instrução que muda tudo.

### 1. O lado provedor não conta — e era isso que duplicava a lista

```
CONTA PROVEDORA (quem manda)       CONTA DA PARCERIA (a obra)
  saída   2.08.97 Aportes BWS        entrada 1.02.02 Aportes Parceiros
  entrada 1.02.95 Devolução BWS      entrada 1.02.94 Aportes BWS
                                     saída   2.08.02 Devolução de Aportes
```

> *"Ocorre que para efeitos de aporte, não devemos considerar os lançamentos da
> conta provedora, senão fica meio duplicado os lançamentos."*

E ele mostrou: R$ 10,00 de 04/09/2025 aparecendo **duas vezes** na lista — +10
entrando na 22069, −10 saindo da 7011. Somar dava certo (os sinais se anulavam),
mas **listar mostrava dobrado**, e quem lê a lista conclui que o painel erra.

Agora o lado provedor é excluído **pelo código da categoria**, e some do bloco
inteiro — inclusive da lista. Quanto ele representa aparece à parte, em vez de
virar o próximo mistério.

### 2. O corte de transferência estava engolindo o bloco

**É quase certo que era aqui que estavam os R$ 2,4 milhões de devolução.** Todas
as telas descartam `analise = 'TRF'` por padrão, e com razão. Mas **o bloco de
aportes é a exceção, e é a exceção porque o assunto dele é exatamente esse
dinheiro**: aporte da BWS para a obra ANDA entre contas da empresa. Se a
categoria estiver marcada como transferência no plano financeiro do OMIE — e
depois da carga inicial as categorias vieram com as marcas novas —, o corte
levava tudo.

O que impedia a duplicação era esse corte. Hoje quem impede é a exclusão do lado
provedor, que é o jeito certo: tira o espelho e mantém a operação.

### 3. Quem aportou sai da contraparte, não do rótulo

> *"A diferença de Aportes BWS e Aportes Parceiros é somente a nomenclatura (…)
> e há várias situações que o pessoal do financeiro fez o lançamento trocado e
> não colocou Aportes BWS. Veja pra isso não prejudicar a análise."*

No exemplo dele, a entrada estava como "Aportes Parceiros" e a contraparte era
BWS CONSTRUÇÕES LTDA (MATRIZ) — dinheiro da casa com nome de parceiro. O rótulo
erra; a contraparte não. Então a categoria decide **se é aporte ou devolução**, e
a contraparte decide **de quem é o dinheiro**.

A heurística da casa é o nome conter "BWS" (`PADRAO_EMPRESA_DA_CASA`). É um
palpite razoável e o dono pode corrigir.

### 4. E o aviso que faltava: o filtro da tela

Eu passei dias comparando a conferência das Configurações — que roda **sem
filtro** — com o bloco do DRE, que roda **com os filtros da barra lateral**, e
concluindo que faltava dinheiro. Não tinham por que bater. Um ano selecionado na
lateral já explica uma devolução "sumida".

O bloco agora diz, na cara: quanto está vendo, quanto é na base inteira, quanto
está fora **por causa do filtro**, e um link para tirar o filtro. E a cascata
pode ser aberta **dentro do bloco, com os mesmos filtros da tela** — nunca mais
comparar dois recortes diferentes.

## O resto estava sem obra — 21/09/2026

Com a tela na mão (ele mandou o bloco inteiro colado), a conta fechou o
diagnóstico:

```
devolvido, filtrado em MERCADOBARBALHA ....  R$   887.000,00
devolvido, base inteira ..................  R$ 5.491.600,35
fora do filtro de obra ...................  R$ 4.604.600,35
o que ele esperava a mais ................  R$ 2.461.891,66
```

**O dinheiro está na base.** Ele cabe folgado nos R$ 4,6 milhões que o filtro de
obra deixa de fora. Ou seja: são aportes e devoluções que existem, estão
classificados certo, foram reconhecidos como pagos — e **não estão apropriados a
obra nenhuma**. O filtro por obra os descarta, e a tela não dizia por quê.

A devolução de R$ 784.647,07 que ele caçou por uma semana é um desses.

Por isso o bloco ganhou, dentro do próprio aviso de filtro, a quebra
**obra por obra na base inteira**, com a linha `(não apropriado)` à vista. É a
linha que ele conserta — no OMIE ou pelo Explorador —, e depois dela o número
entra sozinho na obra certa.

**A lição, que vale para o resto do painel:** quando um número parece faltar, a
tela tem de mostrar ONDE ele está, não só quanto falta. "O filtro esconde X" não
resolve nada se a pessoa não souber para onde olhar.

## O lote que travou o sistema — 21/09/2026

O dono mandou um lote grande de apropriações pela tela de saneamento e o painel
inteiro ficou lento por horas. O log do Render contou a história:

```
HTTP 500 (tent. 1/8): O período contábil de Dezembro de 2023 foi bloqueado
                      por Integração em 01/07 Qua às 06:00.
HTTP 500 (tent. 2/8): (a mesma coisa)
HTTP 500 (tent. 3/8): Consumo redundante detectado. Aguarde 57 segundos.
...
[425] API bloqueada por consumo indevido. Tente novamente em 664 segundos.
```

**Quatro defeitos empilhados, e o primeiro causou todos os outros.**

### 1. Período contábil fechado era tratado como erro passageiro

A Omie devolve isso como HTTP 500, e HTTP 500 dela costuma ser transitório —
por isso o código retentava. Só que **título de período fechado nunca vai ser
alterado**: repetir é impossível de dar certo. Foram 8 tentativas por título, e
na terceira a própria Omie já acusava *"Consumo redundante detectado"* — ou
seja, a repetição da MESMA requisição. Daí veio o bloqueio geral.

Agora entra na lista de erros definitivos: aborta na primeira.

### 2. O tempo que a Omie pede não era entendido

O código conhecia só `"Aguarde N segundos"`. A mensagem do bloqueio usa outra
redação: `"Tente novamente em N segundos"`. Sem reconhecer, caía no backoff
normal — 8 tentativas somando 112 s, todas DENTRO de um bloqueio de 664 s, cada
uma prolongando-o.

### 3. Esperar dentro da tela trava o sistema inteiro

Dois tetos agora, porque são dois mundos. A **carga** roda num processo
separado, sozinha, e esperar 10 minutos ali é melhor que abortar 120 mil
títulos. A **tela** roda no processo que atende todo mundo, com 4 vias de
atendimento: uma espera de um minuto prende uma das quatro e trava o painel — e
o ERP junto, que divide o mesmo processo. Na tela o teto é 30 s: acima disso,
para e diz quanto falta esperar.

**É quase certo que era isto por trás da lentidão que ele relatou como "clico e
demora minutos".**

### 4. Alteração que não alterava nada

`Departamento: 1651586723 → 1651586723 (100%)`. Gastava uma chamada e, ao
falhar, mais oito. Agora responde "já está assim" sem tocar na Omie.

### E o lote para no primeiro bloqueio

Insistir no próximo título cai no mesmo bloqueio e o prolonga. A tela passa a
dizer quanto esperar e quais títulos **não foram tentados** — nada fica pela
metade: cada um ou foi gravado ou nem começou.

**Como parar um lote em andamento, se acontecer de novo:** só reiniciando o
serviço no Render. O trabalho roda dentro da requisição; fechar a aba não para
nada. Publicar também reinicia, então publicar um conserto já dá o stop.

## Excluir título no OMIE, pelo Explorador — 21/09/2026

O dono pediu um botão para excluir os títulos marcados. Perguntei antes de
fazer, porque "excluir" tinha dois sentidos muito diferentes com o mesmo nome, e
um deles não tem volta. As escolhas dele, registradas:

- **apagar no OMIE mesmo** (não só esconder do painel);
- **título com baixa vai do mesmo jeito** — quem decide é o OMIE, não o painel.

### As travas

| | Alterar | Excluir |
|---|---|---|
| Teto por lote | 200 | **50** |
| Senha de execução | sim | sim |
| Ensaio por padrão | sim | sim |
| Palavra digitada à mão | — | **EXCLUIR** |
| Registro no banco | sim | sim |

O teto menor não é capricho: alterar 200 errado custa alterar 200 de volta;
excluir 200 errado custa **redigitar 200** — quando se sabe o que havia.

A palavra digitada existe porque **marcar uma caixinha por engano acontece;
digitar EXCLUIR por engano, não**.

Título com baixa não é recusado, mas **aparece marcado em vermelho no ensaio**,
antes de confirmar. Recusar é uma coisa; esconder é outra.

### O título sai do painel na hora, e só depois do OMIE confirmar

Excluir no OMIE **não** chega pela atualização do dia: ela pergunta "o que
mudou?", e um título apagado não aparece numa lista de mudanças. Quem nota é a
varredura de excluídos, que só roda na **Atualização completa** — no caso do
dono, toda madrugada.

Então quem exclui pelo botão tira o título da base local no mesmo movimento.
Sem isso ele apagaria, olharia a tela, veria o título lá e concluiria que não
funcionou. **Mas só depois de o OMIE confirmar**: apagar aqui antes seria perder
de vista um título que continua existindo lá.

### Um detalhe de tela que estava faltando desde sempre

A caixa de marcar das linhas não tinha `name`: a marcação só existia na memória
do navegador, e o servidor nunca ficava sabendo. Funcionava para a alteração
porque o JavaScript monta campos escondidos com o que foi editado — mas marcar
sem editar não chegava a lugar nenhum. Agora chega, que é o que a exclusão
precisa.

## A conta do relatório era a da PREVISÃO, não a da baixa — 21/09/2026

O dono:

> *"No OMIE existe a conta de previsão de pagamento e existe a conta onde
> efetivamente foi realizado o pagamento. A informação que está sendo colocada
> nesse relatório analítico é exatamente a primeira. E a primeira é errada."*

Ele está certo, e o erro era **silencioso**. A coluna saía do título
(`id_conta_corrente`), que é onde se **previu** pagar. Quem previu pagar pelo
Bradesco e pagou pelo Itaú aparecia no Bradesco, e nenhuma análise por conta
dava sinal.

Agora sai do **movimento de baixa** (`ncodcc`). Título ainda em aberto não tem
baixa — aí a previsão é a única informação que existe e continua valendo, o que
é diferente de estar errada.

Quando há mais de uma baixa, vale a do **maior valor liquidado**; empate, a mais
recente. Não existe resposta certa para um título pago metade em cada conta — a
linha do relatório é uma só —, e a escolha está escrita no código para ninguém
ter de adivinhar.

**Efeito colateral bom:** a lista de aportes mostrava as devoluções na conta
7011-4. Parte disso pode ter sido a previsão; depois de refazer os números, a
conta exibida passa a ser a de onde o dinheiro saiu de verdade.

**Pega por três testes**, um deles reproduzindo a frase dele: previsto na conta
7, pago na conta 9, o relatório tem de dizer 9. Sem o conserto, a mensagem de
falha é literalmente *"o relatório mostrou 'Bradesco (previsão)'"*.

Basta **"Só refazer os números"** — os movimentos já estão na base, não precisa
baixar nada do OMIE.

**Correção em 22/09/2026:** o conserto acima trocou a fonte mas lia a perna
errada do movimento, e a tela não mudou. Ver *"A conta da baixa continuava
errada"*, mais abaixo.

## Título pago em parcelas: UMA LINHA POR BAIXA — 21/09/2026

O dono, no mesmo dia e sobre o mesmo relatório:

> *"Ele foi pago em duas parcelas, em 2 dias diferentes e valores diferentes. Só
> que no relatório de despesa analítica aparece um único lançamento (…) do total
> do título. Se você for olhar no extrato, dá uma coisa. Aí você olha no
> relatório analítico, dá outro valor. Isso confunde."*

Os dois erros que ele achou hoje são **o mesmo defeito**: a `fato` montava
**uma linha por título**, quando o certo é **uma linha por baixa**.

**O painel já fazia certo do lado das receitas.** `montar_recebimentos` abre uma
medição recebida em três parcelas em três linhas, cada uma com sua data e seu
valor. Nas despesas esse caminho nunca tinha sido ligado. Agora vale para as
duas, e reusa o mesmo `_escolher_recebimentos` — que é quem sabe desmontar a
armadilha do OMIE de guardar a mesma baixa em duas pernas (a consolidada e os
créditos bancários). Regra repetida divergiria; reusada, não.

### O que muda, e o que não muda

**Não muda:** o total do título. Cada parcela é escalada por
`realizado / soma_das_baixas`, e juros e multa vão pela mesma proporção — as
pernas de crédito bancário vêm com encargo zerado, e usar o de cada uma faria o
total encolher sem ninguém notar.

**Não muda:** título pago de uma vez, que é a esmagadora maioria. Continua uma
linha só, idêntica à de antes.

**MUDA, e o dono foi avisado antes:** um título pago metade em março e metade em
abril contava **inteiro em abril**; agora conta **metade em cada mês**. Meses já
olhados podem mudar de valor. Não é o painel ficando errado — é ele parando de
estar.

**Cuidado que estava fácil de errar:** o saldo em aberto é do TÍTULO, não de
cada baixa. Repeti-lo em cada linha multiplicaria o "a pagar" pelo número de
parcelas — um erro que cresceria com o uso, silencioso. Ele vai numa linha só, e
há teste para isso.

E a retenção de imposto também: uma linha por título, mesmo com várias baixas.

### De quebra

O teste "pago em duas contas" mudou de resposta para melhor. Antes valia a conta
do maior valor, porque a linha era uma só; agora **cada parcela mostra a conta
dela**. A regra do maior valor ficou só para quando as baixas não dão para
separar.

Basta **"Só refazer os números"**.

## Acesso por pessoa, preso a obras e a telas — 21/09/2026

O dono: *"quero poder criar acesso a um usuário para ele entrar e ver somente
determinada ou determinadas obras no painel."* E, sobre o que liberar:
*"queria poder selecionar quais telas. Gostaria era de liberar a princípio DRE,
Despesas Analítico, mas se de repente entender que seja necessário liberar outra
tela, já estaria configurado."*

Até aqui o painel tinha **uma senha só** e quem entrava via tudo.

### Como ficou

| | Senha do dono (`PAINEL_SENHA`) | Usuário e senha próprios |
|---|---|---|
| Vê | tudo | só as obras marcadas |
| Telas | todas | só as marcadas |
| Configurações e Explorador | sim | **nunca** |
| Escreve no OMIE | sim | **nunca** |
| Baixa Excel/PDF | tudo | só das obras dele |

Cadastro na tela de Configurações: usuário, nome, senha, as obras e as telas.
DRE e Despesas Analítico já vêm marcadas; as outras ficam prontas.

### O que sustenta isso: UM lugar só

O escopo é aplicado em `_filtros_do_pedido`, por onde **toda tela, todo
download e todo gráfico passam** para saber o que mostrar. Amarrar ali
significa que nenhuma tela pode esquecer — que é exatamente como esse tipo de
coisa vaza quando se protege tela por tela.

A linha que mais importa é um `or`: se a pessoa não escolheu obra (ou escolheu
uma que não é dela), o filtro vira **a lista dela** — e nunca "sem filtro".
Sem isso, bastava apagar a obra da barra de endereço para ver a empresa inteira.
Há teste para os dois ataques óbvios: apagar o filtro e escrever a obra do
vizinho.

### Falhar FECHADO, em todo lugar

- **sem obra marcada, não entra.** Lista vazia quer dizer nenhuma, nunca todas —
  um cadastro pela metade não pode virar acesso total;
- **sem tela marcada, não entra;**
- **tela não liberada responde 404**, não 403. Dizer "sem permissão" confirmaria
  que a tela existe, e varrer os endereços mapearia o sistema sem abrir nada
  (mesma regra do ERP);
- **tirar a obra de alguém vale na hora**, não quando ele fechar o navegador: a
  sessão só guarda o número da pessoa, e o escopo é relido a cada pedido;
- a barra lateral **não lista as obras dos outros** — o nome delas é informação
  que ele não teria de outro jeito;
- as abas do topo mostram **só o que abre**: aba que responde "não encontrado"
  ao ser clicada é pior que aba nenhuma.

### Um vazamento que o teste pegou antes de existir

A lista de áreas proibidas é por prefixo de rota, e `painel.usuarios` tinha
ficado **de fora**. Resultado: uma pessoa presa a uma obra conseguia **criar
outro acesso** — inclusive um com todas as obras. O teste que tenta abrir
Configurações, Explorador e o cadastro pegou na hora.

A lição, que vale para a próxima rota: **a lista por prefixo só protege o que
está escrito nela.** Rota nova numa área sensível precisa entrar lá, e o jeito
de não esquecer é ter um teste que tenta abrir.

### A senha

Guardada embaralhada (PBKDF2 com sal, do `werkzeug` que o Flask já traz). Nem o
dono lê a senha de alguém depois — só troca. Este banco tem o financeiro inteiro
da empresa.

## Extrato de Conta Corrente — 21/09/2026

O dono: *"seria até similar com o relatório analítico, só que ao invés de ser o
da obra, seria o da conta corrente (…) e ali só iriam poder ser vistos os
lançamentos que aconteceram na conta corrente."* Para poder conferir lado a lado
com o extrato do próprio OMIE.

### O que separa esta tela do Analítico

1. **Só o que virou dinheiro.** Título em aberto não entra — extrato é caixa,
   não compromisso. Por isso **não há coluna de vencimento**: ele disse com
   todas as letras que ali ela não interessa.
2. **As duas pontas juntas**, entrada e saída, na ordem da data — como o banco
   mostra e como dá para comparar.
3. **NÃO filtra por DRE**, e este é o ponto. Tarifa bancária e rendimento ficam
   de fora do resultado porque o plano financeiro do OMIE não lhes dá conta de
   DRE (ver a seção sobre isso). No extrato eles **aparecem**, porque saiu e
   entrou dinheiro de verdade — era exatamente o que ele não estava conseguindo
   achar quando foi olhar as tarifas do Mercado Barbalha.
4. Transferência entre contas também aparece: sai do resultado, não sai do
   extrato.

Colunas, como ele ditou: data, cliente/fornecedor, CNPJ, conta, categoria, obra,
documento, observação, valor e o link do Pipefy. Filtros de período, categoria e
busca (nome, CNPJ, documento ou observação), com download.

### O acesso por conta (migração 014)

*"eu queria poder disponibilizar essa tela para um determinado usuário, mas
definir qual conta e quais contas ele poderia visualizar."*

`usuario_contas`, tabela separada de `usuario_obras` de propósito: são recortes
independentes. Alguém pode ver a obra inteira e só uma das contas por onde ela
passa — e o contrário também.

Mesma regra das obras: **sem conta marcada, não vê conta nenhuma no Extrato.**
Com uma diferença: conta liberada é opcional. Quem não tem nenhuma simplesmente
não usa o Extrato, e as outras telas seguem normais.

O escopo é aplicado no mesmo `_filtros_do_pedido` — o único lugar por onde toda
tela passa. Pedir a conta de outro na barra de endereço não funciona, e há teste.

### As transferências, com os dois lados juntos

Logo depois de ver a tela, o dono perguntou: *"se eu quiser filtrar, eu quero
ver todas as transferências num determinado período da conta tal para a conta
tal. Consigo visualizar isso aí?"*

Não conseguia, e o motivo é do OMIE: **uma transferência são DOIS lançamentos
separados**, um saindo de uma conta e outro entrando na outra, **sem nada que
ligue um ao outro**. Filtrando a conta de origem via-se a saída, nunca o destino.

O Extrato ganhou uma segunda visão que **pareia os dois lados por mesma data e
mesmo valor**, e mostra "saiu de X, entrou em Y". Dá para filtrar por período e
por conta de destino, e a conta filtrada na barra aparece nos **dois sentidos** —
"as transferências desta conta" são as que saem e as que entram.

**O limite está escrito na tela**, não escondido: se duas transferências do mesmo
valor acontecerem no mesmo dia, o par pode trocar de destino. Não há como fazer
melhor sem um vínculo que o OMIE não guarda, e inventar um vínculo que não existe
seria pior que mostrar o que se sabe.

Cuidado que virou teste: **cada entrada serve a UMA saída**. Sem isso, duas
saídas do mesmo valor no mesmo dia casariam com a mesma entrada e o total
dobraria.

**O que NÃO pareia é a parte mais útil da tela.** Saída sem entrada do outro lado
quase sempre quer dizer que o lançamento do outro lado **não está classificado
como transferência** — e então ele está entrando no resultado como se fosse
receita ou despesa. Aparece num quadro à parte, com o número do OMIE.

E quem só pode ver certas contas **não descobre o nome das outras por aqui**: o
destino que ele não pode ver vira "(outra conta)". Ele vê que o dinheiro foi
para algum lugar; para onde, não.

### Detalhes que quase passaram

- **`pagina_link` apontava para o Analítico com o nome escrito.** Virar a página
  do extrato jogaria a pessoa para dentro do analítico, com o filtro de conta
  junto — mostrando uma tela que não é a que ela está lendo. Passou a usar a
  tela atual.
- **A observação só aparece depois do "Buscar as observações"**, e a tela diz
  isso em vez de mostrar travessão e deixar parecer que o dado não existe.

## Os juros de empréstimo passam a ser pagos por quem demandou o caixa — 21/09/2026

O dono parou e disse, em bom português, o que o painel ainda não faz — e que é
o motivo de existir:

> *"As telas que eu estou basicamente usando: DRE, analítica e o extrato. Todas
> as outras telas juntas, elas têm sido uma tentativa ainda frustrada de
> realizar a prestação de contas da empresa (…) a gente precisa entender o
> resultado final e quanto é de direito para cada envolvido."*

E apontou o que trava, na ordem em que ele mesmo colocou: (1) os aportes — já
quase resolvidos; (2) os **percentuais de cada sócio e parceiro**, que só ele
tem; (3) **o ponto mais crítico: pulverizar o custo da matriz nas obras**, com
dois detalhes:

> *"Quando a gente fala em custo de juros de empréstimo (…) a gente tem que
> utilizar a necessidade de caixa da obra. Quem é que paga aquele juro de
> empréstimo mês a mês? É a obra que está demandando caixa. Essa é a forma
> justa. Se a obra não demanda caixa, não tem por que estar pagando um juro de
> empréstimo. Esse juro de empréstimo, eles estão consolidados dentro do custo
> da matriz."*

> *"Segundo, dividir o custo das obras do Ceará com as demais obras. Por quê?
> As obras do Ceará demandaram mais do que as outras obras."*

### O que já existia, e por que não resolvia

As duas metades do que ele pede já moravam no repositório — em telas
**diferentes, que não se falavam**:

- o **Rateio da Administração** já alocava os juros pelo déficit de caixa, que
  é exatamente a régua dele. Mas só divide em **dois lados** (A e B): não fala
  de obra individual nem de sócio, e o resultado não entra na prestação;
- a **Prestação de Contas** já pulverizava o custo da matriz **obra a obra** e
  já dividia entre sócios, com o caso do sócio externo. Mas jogava o juro
  dentro do bolo da estrutura, repartido pelo **custo de pessoal**.

Ou seja: quem sabia alocar juro direito não sabia falar de obra; quem falava de
obra alocava juro errado. Obra que se paga sozinha pagava juro só por ter
gente; obra que viveu de dinheiro emprestado pagava de menos.

### O que mudou

Na Prestação de Contas, o juro de empréstimo **sai do bolo da estrutura antes
do rateio** e ganha régua própria:

1. reconstrói-se o caixa acumulado de cada obra, mês a mês, **incluindo o
   rateio da estrutura que coube a ela** — a administração que a obra consumiu
   é dinheiro que ela fez a empresa gastar;
2. o juro de cada mês é dividido entre as obras que estavam **com o acumulado
   negativo naquele mês**, na proporção do tamanho do buraco;
3. não há circularidade: o déficit que serve de régua é o de **antes** do juro.

O buraco **não se apaga num mês sem movimento** — só quando a obra recupera o
dinheiro. Sem isso, bastaria a obra ficar parada um mês para deixar de pagar o
juro que ela mesma provocou.

**Juro lançado direto numa obra não passa por aqui**: já é despesa dela, e
mexer nisso seria tirar de quem o assumiu. O que se realoca é só o que está
consolidado nos departamentos administrativos — que é onde ele está, como o
dono disse.

**Na divisão entre sócios o juro entra na base de TODOS**, inclusive do sócio
externo — diferente do rateio da estrutura, que volta só para os internos. A
razão: estrutura é overhead da construtora, e por ela se cobra a taxa de
administração; juro é o preço do dinheiro que financiou **aquela obra**, e quem
participa do resultado dela participa do custo de bancá-la. A soma das quotas
continua fechando com o resultado do projeto — há teste para isso.

### Três chaves novas (migração 015)

- `categoria_juros` — o nome exato da categoria no OMIE (padrão *Juros sobre
  Empréstimos*). É por ele que o juro é separado do resto do custo da matriz;
- `juros_por_deficit` — `1` liga a régua nova, `0` volta ao comportamento
  antigo. Sai ligada;
- `juros_sem_deficit` — o que fazer com o juro de um mês em que **ninguém**
  estava no vermelho: `sobra` (fica visível como custo sem dono, o padrão) ou
  `estrutura` (segue a régua da estrutura).

Todas editáveis em Prestação de Contas › Parâmetros › Geral.

### O que a tela mostra agora

Uma coluna **Juros de empréstimo** separada do **Rateio recebido** no resultado
por projeto — juntá-las esconderia justamente a diferença que interessa. Abaixo,
um bloco com o juro que coube a cada obra e uma **memória mês a mês** (juro
pago, quantas obras no vermelho, o buraco somado, a maior devedora e qual régua
valeu) para a conta poder ser conferida em vez de acreditada.

### O que ficou de fora, e por quê

- **Os percentuais de cada sócio e parceiro** continuam vazios. O cadastro
  existe e a conta existe; falta o dado, que só o dono tem. Sem ele não há
  visão global.
- **A divisão do custo das obras do Ceará com as demais** não foi feita: a
  frase dele admite duas leituras muito diferentes — o **prejuízo** do Ceará
  espalhado nas outras obras, ou apenas o **custo do caixa** que elas
  demandaram, que a régua nova já resolve sozinha. Perguntado, não chutado.
- **Aporte recebido pela obra não abate a necessidade de caixa dela.** Hoje o
  déficit é o da operação, tenha ele sido tapado pelo banco ou pelo sócio. Se o
  dono quiser que aporte abata, é uma linha de configuração a mais — está na
  pergunta que foi devolvida a ele.

## O login trancou o dono para fora — 22/09/2026

**O relato:** *"Com a criação de usuários não tô mais conseguindo acessar o
painel."*

**A causa, e ela é traiçoeira o bastante para ficar escrita:** quando a tela de
login ganhou o campo *Usuário*, o caminho da entrada passou a ser escolhido por
esse campo estar **vazio**. Só que o gerenciador de senhas do navegador tinha a
senha do dono guardada de quando a tela tinha **um campo só** — e, ao ganhar o
segundo, passou a preenchê-lo sozinho. O pedido saía com usuário preenchido,
caía no caminho da pessoa presa a obra, e a resposta era "usuário ou senha
incorretos" **com a senha certa digitada**. Do lado de fora não havia como
adivinhar.

**O conserto:** quem decide é a **senha**. Se ela for a senha mestre, entra como
administrador, tenha o campo de usuário o que tiver. Isso não afrouxa nada —
quem conhece a senha mestre já é o administrador.

A única brecha que isso abriria: uma pessoa presa a obra com a **mesma senha do
dono** passaria a entrar como administrador. Fechada onde custa nada — o
cadastro recusa a senha mestre como senha de alguém.

**A lição geral:** *decidir um caminho de autenticação por um campo estar vazio
é frágil*, porque quem preenche o campo nem sempre é a pessoa. Decida pelo que
confere, não pelo que falta.

## O download passava por fora da proteção das telas — 22/09/2026

Achado ao acrescentar o arquivo do cenário: a rota `/painel/baixar/<assunto>`
**não conferia assunto nenhum**. Cada tela era protegida uma a uma e o download
passava por fora.

A maioria dos arquivos se monta a partir de `_filtros_do_pedido`, que já prende
a pessoa às obras dela — esses estavam certos **por acidente**. Mas `quotas`,
`posicao`, `rateio_admin` e o `cenario` não passam por ali: leem a empresa
inteira, porque a pergunta que respondem é sobre a empresa inteira. **Quem
tinha acesso a uma obra podia baixar a divisão de lucro entre os sócios.**

Agora o arquivo segue a tela (`TELA_DO_DOWNLOAD`, em `auth.py`), e os quatro
que abrem a empresa inteira são só do dono. Assunto novo que ninguém mapear
nasce **fechado**. Há teste para cada um dos seis.

## O cenário da prestação de contas — 22/09/2026

O dono, depois de olhar o que existia espalhado em quatro telas:

> *"Todas as outras telas juntas, elas têm sido uma tentativa ainda frustrada de
> realizar a prestação de contas da empresa (…) eu queria trazer para uma tela
> de prestação de conta, onde dentro dela eu vou nomear os parceiros, os sócios,
> os percentuais, vou definir se vai ser baseado na mão de obra ou no
> faturamento, quais contas da matriz eu vou dividir, em quais percentuais (…)
> e importantíssimo, auditável."*

E, sobre a operação — a parte em que a tela antiga falhou:

> *"Não adianta uma coisa que eu tenho que digitar coisa por coisa para sair
> colocando. Tem que ser um negócio realmente fácil de fazer."*

### O CENÁRIO é o objeto

Tudo o que muda o resultado vive dentro dele: a régua do rateio, os percentuais
de cada conta da matriz, a régua dos juros, a taxa de administração e quem
divide. Trocar de cenário troca o resultado inteiro — e o anterior fica
intacto. **Duplicar** um cenário é o que torna barato perguntar "e se fosse
faturamento?": parte-se do pronto e troca-se uma coisa só.

### Por que os percentuais são uma HIERARQUIA, e não uma lista de regras

A tela antiga pedia um cadastro por regra, com nome, escopo e vigência. Aqui a
conta da matriz herda o percentual padrão do cenário; marcar o **grupo**
sobrepõe; marcar a **categoria** sobrepõe o grupo; marcar um **lançamento**
sobrepõe a categoria. Configurar é tocar em poucas linhas, não em todas.

Duas sutilezas que têm teste porque custariam caro:

- **vazio e zero são coisas diferentes.** Zero é "esta conta não se divide";
  vazio é "segue o nível de cima". Por isso apagar o campo APAGA a marcação em
  vez de gravar zero;
- **lançamento marcado sai do balde da categoria.** Se entrasse com percentual
  próprio sem sair do agregado, o mesmo dinheiro contaria duas vezes.

### A régua: mão de obra ou faturamento

O padrão é mão de obra, e o motivo é do dono:

> *"O custo de despesas com o pessoal é um indicador da quantidade de energia
> que aquela obra requer (…) o DP vai ter mais trabalho, a engenharia vai ter
> mais trabalho. É uma obra normalmente mais complexa do que uma que usa pouca
> mão de obra e muito maquinário."*

Trocar para faturamento vira a conta, e é justamente a comparação que ele quer
poder fazer. Há uma **janela** (1, 3, 12 meses ou acumulado) para escolher entre
reagir rápido e ser estável.

### Quem divide: por OBRA, não por projeto

Diferente da prestação antiga. A razão: ele fala de "as obras do Ceará", e
parceiro entra em **obra**, não na construtora. Quem participa de tudo entra uma
vez só, com a obra em branco; quem for nomeado numa obra específica
**substitui** a lista geral naquela obra. É assim que entra o parceiro de uma
obra só sem refazer o resto.

Na obra com parceiro, a conta é a mesma de antes — taxa de administração sobre
a receita bruta, crédito da taxa e da estrutura só para os internos — com uma
diferença: **o juro entra na base de todos**, inclusive do parceiro, porque não
é estrutura da construtora, é o preço do dinheiro que financiou aquela obra. A
soma das quotas continua fechando com o resultado da obra, e há teste.

### Auditável, que era a outra palavra sublinhada

A tela de resultado abre a conta: estrutura por obra e mês a mês (com o bolo do
mês, quantas obras entraram e quem levou mais), juros por obra e mês a mês (com
quantas estavam no vermelho e o buraco somado), a quota de cada um obra a obra,
e o que ficou **sem dono**. O mesmo sai em planilha e em PDF, com os
**parâmetros do cenário na primeira aba** — memória de cálculo sem as escolhas
que a geraram não se confere seis meses depois.

E o gráfico que ele pediu: **a vida de uma obra**, mês a mês — o caixa
acumulado (já com a estrutura que ela recebeu), onde ele fica abaixo de zero, e
a mesma linha depois do juro absorvido. Uma obra por vez, porque 174 linhas
sobrepostas não se leem.

### O que NÃO mudou

A Prestação de Contas antiga continua funcionando, com as regras antigas, e é
ela que roda hoje. O cenário é uma porta nova, com um aviso na tela velha. Nada
foi trocado sem o dono ver — e aposentar a tela antiga é decisão dele, depois de
conferir que a nova bate.

### O que ainda falta

- **Comparar dois cenários lado a lado** numa tela só. Hoje compara-se trocando
  o cenário no seletor. A tela antiga de cenários faz isso para as regras
  antigas e serve de modelo;
- **A divisão do custo das obras do Ceará com as demais.** Continua
  perguntada, não chutada — a frase dele admite duas leituras (o prejuízo
  espalhado nas outras obras, ou só o custo do caixa que elas demandaram, que a
  régua dos juros já resolve sozinha);
- **Os percentuais de cada sócio e parceiro.** O cadastro existe e a conta
  existe; falta o dado, que só ele tem.

## A conta da baixa continuava errada — 22/09/2026

O dono: *"refiz os números do painel, mas o problema das contas permaneceu"*.

**O que tinha acontecido.** O conserto de 21/09 trocou a **fonte** da conta —
do título (`id_conta_corrente`) para o movimento de baixa (`ncodcc`). Só que o
OMIE guarda cada baixa em **duas pernas**, e o código estava lendo a errada:

- a **consolidada** (`cLiquidado = 'S'`) é o *resumo do título* — e carrega a
  conta **do título**, a da previsão. Era dela que a conta saía. Trocar de
  tabela e continuar lendo essa perna deu exatamente o mesmo número de antes;
- a **bancária** (`cLiquidado` vazio, `nValLiquido = 0`, uma por conta que o
  dinheiro tocou) é a única que sabe **por onde o dinheiro saiu**.

O `_escolher_recebimentos` já sabia dessa armadilha — é ele quem desmonta as
duas pernas para não dobrar o caixa — e o título pago em parcelas já usava a
conta de cada perna bancária. Só o título pago **de uma vez** (a esmagadora
maioria) caía no atalho da consolidada.

**O conserto:** `_conta_de_onde_saiu`, no `fato.py`. Entre as pernas que o
`_escolher_recebimentos` escolhe, vale a de maior valor; empate, a mais
recente. Sem perna com conta, fica a consolidada, e depois a previsão — a
linha nunca perde a informação. Um teste reproduz a cena: previsto na 7,
resumo na 7, dinheiro saiu da 9, o relatório tem de dizer 9.

**E uma conferência nova, "Conta de onde o dinheiro saiu"**, em Configurações,
porque desta vez eu não tenho como olhar a base real: ela conta, no espelho,
quantas pernas bancárias existem e em quantas a conta é diferente da do
título — e quantas consolidadas diferem (a aposta é: quase nenhuma). Se a
base **não tiver** perna bancária nenhuma, a caixa fica vermelha e diz o que
isso significa: a conta real não está no espelho, refazer os números não muda
nada, e ela teria de vir de outra listagem do OMIE com uma carga nova.

**O que o dono precisa fazer:** depois de publicado, **"Só refazer os
números"** de novo — o espelho já tem as duas pernas, nada precisa ser baixado
do OMIE. E rodar as conferências para ler a caixa nova.

**Lição:** um conserto "confirmado por teste" pode confirmar só a leitura do
código, não a do mundo. O teste de 21/09 modelava a conta na perna consolidada
porque eu assumi que era lá que ela estava. Quando o dono diz que não mudou,
a primeira pergunta é *de qual pedaço do dado* o número está vindo.

## Analítico: filtro pela conta de pagamento e o relatório com abas — 22/09/2026

O dono: *"coloque no despesas analítico filtro pra conta de pagamento. Gostaria
ainda que você melhorasse o relatório, tá muito pobre. Acho que pode ter outras
abas."*

**O filtro já existia — e sumia.** A barra lateral tem a lista de contas desde
o Extrato (21/09), e o Analítico já a respeitava. Mas o formulário da própria
tela não levava a conta adiante: quem escolhia uma conta na barra lateral e
clicava em **Aplicar** via o filtro desaparecer, sem aviso. O DRE tinha o mesmo
defeito. Os dois foram consertados, e o Analítico ganhou uma caixa própria,
**"Conta de pagamento"** — uma conta só; várias marcadas na barra lateral
viajam escondidas e a caixa avisa quantas são, em vez de mostrar só a primeira.

A conta é **a de onde o pagamento saiu**, não a prevista no título — vale a
correção da perna bancária, mais acima.

**O relatório passou a ter oito abas**, todas somadas **a partir das mesmas
linhas da aba de lançamentos** — e não por consultas próprias. É de propósito:
assim cada resumo fecha com a lista, com os mesmos filtros (inclusive os desta
tela: grupo, credor, busca, faixa de data), e ninguém precisa explicar por que
a soma por conta deu diferente da lista.

1. **Resumo** — os filtros aplicados (para o arquivo dizer o que ele é) e os
   totais;
2. **Por grupo**, 3. **Por categoria**, 4. **Por credor**,
5. **Por conta de pagamento**, 6. **Por obra** — cada um com lançamentos,
   pago, a pagar, juros e multa, total e % do total, do maior para o menor;
7. **Por mês** — em ordem cronológica, o sem-data por último;
8. **Lançamentos** — a lista de sempre.

E o botão de **PDF** ao lado do Excel, que já existia para o resto e não para
esta tela.

Sete testes com banco de verdade: o recorte, a caixa, as duas contas que não
se perdem, o DRE, as abas fechando com a lista, o filtro escrito no arquivo e
o PDF.

## "Falha gravíssima": a medição que valia oito vezes o título — 22/09/2026

O dono:

> *"CREPEBELEM | Medição 1, doc. PM1339984827 — R$ 664.875,43 bruto. No OMIE
> tenho para o mesmo título o valor bruto de 86.828,71. Vi outra obra
> (MERCADOBARBALHA) e estava correto. Outra tem valores corretos e errados."*

**O que a linha é.** A Receita de Obra junta numa linha todos os títulos cuja
**observação** diz a mesma medição da mesma obra (`OBRA|Medição No: N`) — de
propósito, desde a conversão: uma medição é faturada em várias notas (principal
e reajuste, fontes de recurso diferentes), e a linha mostra a medição inteira.
MERCADOBARBALHA bate porque lá cada medição é uma nota só.

**O que estava errado na tela.** Ela mostrava **um** documento para o grupo
inteiro (`MAX(numero_documento)`) — e não dizia que era um grupo. O dono pegou
esse documento, conferiu no OMIE, achou R$ 86 mil, e concluiu, com razão, que
o número não tinha pé nem cabeça. **Um número que não dá para conferir é um
número errado**, mesmo quando a soma está certa.

**A decisão do dono, ao ver a composição:** *"não fica legal agrupado,
confunde, tem que separar mesmo os títulos."* Então a linha da Receita de Obra
passou a ser **o título**, não a medição:

- **um título por linha**, com o documento dele, o valor dele e o nº no OMIE.
  A medição continua escrita ao lado, como rótulo — mas o número é o do
  título, que é o que se confere no OMIE;
- o detalhe (`/receita/titulo/<nº>`) mostra o título e os recebimentos dele,
  e recusa com "não encontrado" quem está preso a outra obra;
- o rodapé conta títulos. O agrupamento por medição sobrevive só no rótulo e
  na Receita Analítico (que já abria por recebimento);
- a lista passou a sair **por data**, mais recente primeiro. Era por valor, e o
  dono viu "aleatório";
- **as outras receitas saíram da lista de medições.** Rendimento, estorno e
  devolução apareciam misturados com as medições (a lista era "toda receita do
  DRE"); agora a lista é só receita de obra e o imposto retido dela
  (`RECEITA_DE_OBRA`), e o resto fica só no bloco "Outras receitas", embaixo.
  O total do rodapé segue a mesma régua. *"Não misturar com a receita de
  obra"* — o dono, três vezes na mesma hora.

**O que NÃO dá para saber daqui:** se os outros títulos de "CREPEBELEM |
Medição 1" são mesmo dessa medição (notas da mesma medição) ou títulos com a
observação errada. O detalhe novo é o que responde — e a resposta é dele.

**Se ele confirmar que o OMIE está certo e o painel errado** mesmo depois de
ver a composição, a suspeita seguinte é duplicação de movimentos na retomada
por página (`espelho.py`): a página gravada sem a marca salva seria regravada.
Não foi investigado porque a composição explica o caso relatado; fica anotado.

## O bloco de Aportes levava dois minutos para abrir — 22/09/2026

O dono, com o número na mão: *"Tela montada em 126753 ms — 15 consultas ao
banco, 126743 ms delas."*

**A causa.** Cada consulta do bloco decidia, **linha a linha**, se o lançamento
era aporte e de que tipo: tirar acento da categoria, testar dez expressões
regulares e procurar "bws" na contraparte. Para 185 mil linhas, quinze vezes
por tela. A decisão não muda entre uma tela e outra — muda quando a base é
refeita.

**O conserto: decidir uma vez, na montagem do fato** (`tipo_aporte`, migração
017). `''` quer dizer "não é aporte"; `NULL` quer dizer "linha anterior à
migração, ainda não recalculada" — e para essas a consulta cai na expressão
antiga, lenta mas certa. Depois do primeiro **"Só refazer os números"** não
sobra NULL nenhum e cada consulta lê um texto pronto.

**O que precisa acontecer para valer:** apertar "Aplicar atualizações do
banco" (017) **e** rodar "Só refazer os números". Só a migração não acelera
nada.

**Medido pelo dono, em produção, no mesmo dia:** *"Tela montada em 252 ms —
12 consultas ao banco, 241 ms delas."* De 126.753 ms para 252 ms: **500 vezes
mais rápido**. Fica o número aqui para a próxima vez que alguém pensar em
decidir coisa cara linha a linha dentro de uma consulta.

**E três pedidos do dono na mesma hora, no mesmo bloco:**

- *"Essa informação não deveria aparecer aqui, tem que colocar em
  Configurações."* A comparação com a base inteira ("na base inteira são X
  aportados… estão fora do que você está vendo") e a cascata "de onde vem
  cada número" **saíram do DRE**. Estão em Configurações › Conferências, num
  bloco próprio ("Aportes na base inteira — e onde está o resto"). O DRE
  perdeu quatro consultas pesadas com isso;
- *"Essa tela por tipo tá errada, acho que nem precisa dela — tô achando os
  dados em duplicidade."* A tabela **Por tipo** saiu do bloco e da planilha.
  Era a mesma soma do "Por obra" aberta por outro eixo;
- *"Na por obra poderia ter só o somatório embaixo."* Tem: linha de total com
  aportado, devolvido e saldo.

**De passagem, um erro meu:** ao remover o bloco do DRE cortei junto a tabela
"Por sócio ou parceiro" — um teste do dublê pegou antes de sair daqui.

## Alterar um título logo depois de ensaiá-lo NUNCA funcionava — 22/09/2026

O dono, ao alterar **um** título pelo Explorador:

> *"0 título(s) alterado(s). A Omie bloqueou as chamadas por consumo excessivo
> e pediu 59 segundos."*

**A causa.** O fluxo da tela é ensaio → executar. Cada um **consultava o
título no OMIE** — a mesma chamada, com os mesmos parâmetros, com segundos de
diferença. É exatamente o que a Omie chama de *consumo redundante*, e ela
bloqueia por um minuto. Como o teto de espera na tela é 30 s, o lote parava
antes de enviar. Um título só, e nunca dava certo. **É provavelmente por isso
que "a primeira escrita no OMIE nunca aconteceu"** — estava anotado como
pendência desde 13/09.

**O conserto.** O cadastro lido no ensaio fica guardado por cinco minutos e o
envio usa o que o ensaio leu: a única chamada nova é a alteração, que é
outra chamada. Depois de alterado, o cadastro guardado é esquecido.

**O preço, dito:** se alguém mexer no título no OMIE nesses cinco minutos, o
envio parte do cadastro de antes. Curto o bastante para ser raro; longo o
bastante para ler o ensaio e clicar.

**O que o dono precisa fazer agora:** esperar o minuto que a Omie pediu, e
mandar de novo — ensaio e executar. Se ainda bloquear, é outra coisa
consumindo o OMIE ao mesmo tempo (uma atualização da base, ou o Análise de
SPs), e aí é esperar ela acabar.

## O Explorador levava seis minutos quando se procurava por valor — 22/09/2026

O dono: *"Tela montada em 373542 ms — 5 consultas ao banco, 373530 ms delas."*

**A causa** é da mesma família do bloco de Aportes: uma decisão cara refeita
linha a linha. A busca por valor precisa achar o título cuja **soma** (todas
as obras juntas) bate com o número digitado — e fazia isso com uma subconsulta
dentro do `OR` do WHERE (`codigo_lancamento IN (SELECT … GROUP BY … HAVING …)`).
Com 120 mil títulos o resultado não cabe na memória de trabalho do banco, e o
Postgres deixa de guardá-lo numa tabela de hash: passa a **refazer a soma da
base inteira para cada uma das 185 mil linhas**. Três consultas montam o mesmo
WHERE (a lista, os totais e o resumo) — seis minutos.

**O conserto:** a soma por título vira **uma consulta só, antes**
(`_titulos_com_o_valor`), com o resultado guardado no próprio pedido, e os
títulos entram na condição como lista pronta. A busca por texto e por número
do título continuam iguais.

**Não medido em produção ainda** — o dono vai mandar o "Tela montada em" da
mesma busca.

**Ainda pesa, e fica anotado:** a busca por texto varre a base inteira com
`ILIKE` em três colunas, sem índice. Numa base de 185 mil linhas são alguns
segundos, não minutos; se incomodar, o caminho é um índice de trigramas
(`pg_trgm`), que depende de a extensão existir no Postgres do Render.

## O PDF do DRE dava "página não encontrada" para o usuário — 22/09/2026

O dono, no acesso de um usuário preso a obra: clicou em PDF no DRE e caiu em
"Página não encontrada", com o texto genérico mandando ir a Configurações —
que ele nem pode abrir.

**A causa:** os dois botões do DRE (Excel e PDF) baixavam o relatório
**completo** — que cruza todas as telas e, desde a manhã do mesmo dia, é só do
dono (`SO_DO_DONO_PARA_BAIXAR`). O bloqueio estava certo; o botão, não.

**O conserto:** o contexto das telas passou a dizer quem é o dono
(`administrador`), e para quem está preso a obra os botões do DRE baixam o
**DRE** — o mesmo recorte da tela. Dois testes com banco: o parceiro baixa o
PDF dele, o dono continua com o completo.

## Por que a mesma tela é rápida numa hora e lenta na outra — 22/09/2026

O dono: *"queria entender onde está a inconstância na velocidade. Uma hora é
rápido e outras nem vai."* Três causas, todas conhecidas, nenhuma aleatória:

1. **O serviço reinicia a cada ~150 acessos** (`--max-requests 150`, com
   `--workers 1`). Quem cai na reinicialização espera a partida inteira do
   monorepo (18 módulos), e as primeiras telas depois disso pagam de novo as
   listas guardadas em memória (`_lembrando`), que morrem com o processo. É a
   causa mais provável do "nem vai". Afrouxar para 1000 é decisão do dono e
   exige mudar **em dois lugares** — o `Procfile` e o *Start Command* do
   Render, que o sobrescreve (ver `CLAUDE.md` › Gunicorn). **Decidido e aplicado em
   22/09: 1000**, no `Procfile` e no Start Command do Render, os dois no mesmo
   dia. A memória fica em observação nos próximos dias.
2. **Estatísticas velhas depois de refazer os números.** "Só refazer os
   números" esvazia e regrava o `fato` inteiro; até o autovacuum passar, o
   planejador do Postgres escolhe os caminhos das consultas com os números de
   antes — e escolhe errado. Consertado: `ANALYZE` logo depois de regravar,
   no `fato` e nos recebimentos.
3. **Uma carga ou atualização rodando ao mesmo tempo.** O serviço tem 4
   threads; uma carga toma uma delas por horas e disputa o banco com as telas.

## Cenário: obras fora da análise, e onde estão os juros — 22/09/2026

Três pedidos do dono, juntos, com a frase que dói: *"tá muito foda isso. Não
consigo ter confiança no painel. Pra todo lado que olho tem erro."*

1. **Tirar obras ou projetos da análise.** Cada cenário tem a sua lista
   ("obra:NOME" / "projeto:NOME", migração 018). O que sai não recebe
   estrutura nem juros, não entra na quota de ninguém e não pesa na régua. A
   estrutura (matriz e filial) nunca entra nessa lista, mesmo que esteja no
   projeto tirado — o teste pegou isso. Duplicar leva a lista junto.

2. **"Na controladoria tem 1,6 milhão de juros de empréstimo, no painel só
   vejo 191 mil."** Não chutei: uma conferência nova, "Onde estão os juros de
   empréstimo na base" (na montagem do cenário e em Configurações), lista
   toda categoria cujo nome fala em juro, empréstimo, financiamento, IOF,
   encargo ou amortização, com a análise em que o OMIE a põe (DRE ou Fluxo de
   Caixa), quanto foi pago, e marca a que a prestação conta. As hipóteses,
   nesta ordem:
   - a configuração alcançava **um nome só** ("Juros sobre Empréstimos");
     agora aceita vários, separados por ponto-e-vírgula;
   - **parcela de empréstimo lançada inteira** (principal e juro juntos)
     fica no Fluxo de Caixa — não é despesa para o painel, e o juro que
     está dentro dela **não aparece em lugar nenhum**. Se for isso, o
     conserto é no OMIE (separar o juro) ou é uma tabela de amortização —
     decisão do dono;
   - filtro de ano na tela.
   A conferência diz qual das três é.

   **Confirmado pelo dono em 22/09/2026:** "Juros sobre Empréstimos" **é** a
   categoria certa — a de resultado (DRE). A outra, de amortização, é o
   pagamento do **principal** e fica no Fluxo de Caixa, fora do resultado.
   Ou seja: a configuração está certa, e o que falta entender é por que a
   controladoria enxerga 1,6 milhão. A conferência publicada mostra quanto a
   base inteira tem nessa categoria; se for 191 mil, a diferença está fora
   do painel (outro período, principal somado ao juro, ou lançamento que a
   carga não traz) — o próximo passo é o dono comparar os dois números na
   conferência, não mudar a categoria.

3. **"(sem projeto)" com 90 mil ao lado de "(não apropriado)" com 7 mil —
   "sem projeto só pode ser coisa não apropriada."** São duas coisas, e a
   tela agora explica e nomeia: **(não apropriado)** é lançamento sem obra
   nenhuma; **(sem projeto)** é obra que existe mas não está ligada a projeto
   no cadastro do OMIE — dado para corrigir lá, e a tela lista as obras pelo
   nome. Na prestação antiga e no cenário.

## A planilha de projetos passa a ser lida todo dia — 23/09/2026

O dono: *"os projetos são puxados da planilha C. Diários? Em qual momento?
Toda vez que o painel abre?"*

Não é ao abrir: o painel lê só o banco. O de-para obra → projeto vem da
aba "C. Diários" da planilha "Bases de Dados Pipefy" (colunas AJ e AK: o
código do departamento no OMIE e o projeto), gravado na tabela
`depto_projeto` — e **só era lido na primeira carga**. Obra nova no OMIE
chegava "(sem projeto)" até alguém rodar a carga inicial de novo. Com o
acesso por projeto isso ficaria pior: a obra nova ficaria fora do acesso de
quem tem o projeto.

Agora **toda atualização (a do dia e a completa) lê a planilha** antes de
refazer os números. É uma faixa de duas colunas, custa segundos. Se a
planilha falhar (credencial, planilha fora do ar), a atualização segue e a
tela de Configurações diz "ATENÇÃO: a planilha de projetos não foi lida".
"Só refazer os números" continua sem ler nada de fora.

## As medições por trás do número do DRE, num clique — 23/09/2026

O dono: *"num clique, visualizar a receita executada; num clique, a receita
em aberto — no modal, as medições. Não vai sobrecarregar?"*

Não: nada é lido ao abrir o DRE. Nas linhas **Receita Bruta** e **Receita
Líquida**, os três números (executado, em aberto, comprometido) viraram
botões; clicar busca, na hora, a mesma lista da Receita de Obra (um título
por linha, os 300 mais recentes, com os filtros da barra lateral) e abre a
janela: medição, cliente, obra, documento (link do Pipefy), data, recebido,
retido, a receber, bruto e situação, com os totais no alto e o botão "Ver
na Receita de Obra" com os mesmos filtros. Quem não tem a tela Receita de
Obra abre a janela (é parte do DRE), mas sem os links para lá. O endereço
novo é da tela DRE para a autorização, e passa pelo mesmo filtro de obra.

## A retenção do título em aberto aparecia como executada — 23/09/2026

O dono, nos KPIs do DRE: *"tem o executado e tem as retenções; no que está
em aberto não aparecem as retenções. No comprometido aparece só o que já
foi executado. Esses tributos estão lançados — você tem essa informação."*

Tinha. **A carga gravava a linha de retenção sempre como realizado**, mesmo
com o título ainda a receber. Efeito: retenção de medição não recebida
entrava em "Executado", "Em aberto" ficava sem retenção nenhuma, e
"Comprometido" parecia mostrar só as executadas.

**Agora a linha de retenção segue o estado do título**: quitado, realizado;
em aberto, em aberto — junto com o líquido do mesmo título. O bruto continua
líquido + retido, nas três leituras. Nas telas de receita (Receita de Obra,
medição, título, receita por obra), **"Retido"** passou a ser o
comprometido da linha (o já retido e o que ainda vai ser retido) e **"A
receber"** é só o líquido — o retido nunca vai entrar na conta.

**Para valer na base é preciso refazer os números**: a mudança está na
montagem do fato. Depois de publicar, Configurações › **"Só refazer os
números"** (não baixa nada do OMIE). Até lá as retenções continuam como
antes. O que muda nos números: parte do "Executado" da receita bruta
migra para "Em aberto"; o comprometido não muda; o resultado não muda.

## O filtro de conta sumia ao mudar de mês no Calendário — 23/09/2026

O dono: *"aplico um filtro de conta corrente, mudo o mês, ele perde o
filtro"*. Os botões de mês passam pelo mesmo `com_filtros` que as abas do
topo usam — e ele levava ano, projeto, obra e transferências, **mas não a
conta**, que entrou na barra lateral depois dele (21/09). Agora leva. Vale
para toda troca de tela: quem está numa conta no Extrato e vai ao Calendário
chega na mesma conta. Teste cobre o botão de mês e a aba do topo.

## Acesso por projeto, além de por obra — 23/09/2026 (migração 019)

O dono: *"tanto define por obra como por projeto, porque pode ser que eu
queira dar acesso ao projeto como um todo"*.

- **No cadastro de acesso** (Configurações › Acesso por pessoa) há agora
  "Projetos inteiros" ao lado de "Obras, uma a uma". Marcar um projeto libera
  **todas as obras dele, inclusive as que ainda vão entrar na base** — o
  projeto se abre em obras na hora de entrar, não na hora de marcar. Dá para
  combinar: o projeto BETA mais a obra avulsa X.
- **A tranca é a mesma**: toda tela continua lendo a lista de obras da pessoa
  (`_filtros_do_pedido`); o que mudou é que essa lista passou a ser as obras
  marcadas **mais** as obras dos projetos liberados, calculada a cada
  pedido. Sem obra nenhuma (marcada ou vinda de projeto), não entra — um
  projeto ainda sem obra na base não abre nada.
- **A barra lateral** de quem está preso passou a mostrar só os projetos
  dele (os liberados, ou os das obras dele). Antes listava todos os projetos
  da empresa para qualquer pessoa — o nome vazava.
- **Editar a pessoa não congela o projeto**: regravar o cadastro guarda as
  obras marcadas uma a uma, nunca as efetivas — senão a obra futura do
  projeto deixaria de entrar.
- **Custo**: a lista obra → projeto (uma varredura do fato) passou a ser
  lembrada até a próxima carga; sem isso, quem tem projeto pagaria essa
  varredura a cada clique.
- **Migração 019** cria `usuario_projetos`. Ao publicar, apertar "Aplicar
  atualizações do banco" no mesmo momento: o cadastro de acesso lê a tabela.

## O Calendário ganhou o terceiro número: a pagar, em laranja — 23/09/2026

O dono: *"gostaria de visualizar o que está a pagar de cada dia também, um
terceiro número, em outra cor — laranja. Se eu vejo laranja numa data que já
passou, venceu; num dia que não aconteceu ainda, é a vencer. E entra no
KPI."*

- **O número**: o título **a pagar em aberto**, no dia do **vencimento**
  (a coluna de vencimento; quando o OMIE não a trouxe, a data de sempre, que
  no título em aberto já é o vencimento). Só contas a pagar — o a receber em
  aberto não entra, e o caixa (verde e vermelho) não muda.
- **Dia passado com laranja = vencido**: o quadradinho ganha borda laranja e
  o texto diz "venc."; dia futuro diz "a pagar". O KPI "A pagar no mês" abre
  em vencido e a vencer, com a contagem de títulos.
- **Na janela do dia** os títulos em aberto aparecem em laranja, como
  "Vencido" ou "A pagar", e ficam **fora do líquido** — líquido é caixa.
- **"Mostrar"** ganhou "Só a pagar"; "Só pagamentos" e "Só recebimentos"
  escondem o laranja. A planilha do mês leva as duas colunas.
- Teste com banco: o título 3 do cenário (250, vence 30/06) aparece no dia
  30, vencido visto de julho, a vencer visto de junho; o a receber de
  setembro não entra.

## Dividendos: o critério, o negativo de Barbalha, e o dinheiro da obra com os sócios — 23/09/2026

O dono: *"como é que você está fazendo aquela distribuição de dividendos,
qual é o critério? Na obra Mercado Barbalha aparece uma distribuição de
dividendos NEGATIVA e eu nem compreendi. Só deve ser informado o que
efetivamente foi distribuído. E seria legal visualizar ali: positivo são
receitas e aportes; negativo, despesas, devolução de aportes e distribuição
de lucros."*

**O critério** (inalterado, agora escrito na tela): é dividendo todo
lançamento **pago ou recebido** cuja categoria tenha "dividendo" ou
"distribuição de lucro(s)" no nome — qualquer conta com esse nome, inclusive
as marcadas como transferência. Nada além do nome da categoria.

**O negativo de Barbalha**: o quadro somava "pago − recebido" por sócio. Se
entra dinheiro com categoria de dividendo (a matriz recebendo da parceria,
um estorno, ou categoria trocada no OMIE), o "recebido" passa do "pago" e o
líquido fica negativo — sem a tela explicar. **Agora o quadro mostra só o
distribuído** (o que saiu), e, quando há entrada com esse nome, uma coluna
à parte e um aviso: não foi somado nem abatido, confira e corrija no OMIE
se for o caso. O quadro "Resultado × dividendos" já usava só o que saiu.
(Eu não vi o lançamento de Barbalha — a base não é alcançável daqui. A
hipótese acima é a única que produz um negativo; o aviso na tela mostra a
linha.)

**"O dinheiro da obra, com os sócios"** — quadro novo no bloco de Aportes e
Dividendos do DRE, obra por obra e no total: receitas recebidas (sem
retenções) + despesas pagas = resultado; + aportes que entraram − devoluções
que saíram − dividendos pagos = **saldo com os sócios**. Tudo em caixa. Vai
na planilha do bloco como aba "Caixa com Socios". Teste com banco cobre a
soma, o dividendo que entrou (fica fora do saldo) e a igualdade com o
"Resultado" do quadro ao lado.

**Uma correção de passagem**: o "Resultado realizado" do quadro "Resultado ×
dividendos" somava as retenções de receita (dinheiro que nunca entrou). Agora
segue a mesma régua do Fluxo de Caixa. Obra com retenção vai mostrar
resultado um pouco menor do que antes — é o certo.

## O rateio da matriz: afunilar até o lançamento, e ver o que entra — 23/09/2026

O dono, explicando a dificuldade que sobrou na prestação de contas: *"dentro
dos custos da matriz não é tudo que entra (...) só informar um grupo fica
muito complicado para quem quiser analisar depois — o que é que está dentro
daquele grupo? De repente eu quero eliminar parcial, olhar de forma analítica,
um lançamento específico com 0% (...) preciso poder afunilar isso em tela e
não pode ser difícil."*

**O afunilar já existia** desde 22/09 (grupo › categoria › lançamento, com
percentual em cada nível e marcação em lote), mas a tela não dizia **quanto
dinheiro** cada escolha punha ou tirava do bolo, e não havia um lugar que
listasse tudo que foi marcado. Agora:

- **"Entra" e "Fica de fora" em dinheiro**, em cada grupo e categoria, já
  descontando as exceções marcadas lá embaixo — o mesmo cálculo da conta
  (`_contas_da_matriz` e `calcular_rateio_do_cenario` têm de concordar, e há
  teste provando). Rodapé com o total da matriz, e quatro números no alto:
  gastou, entra, fica de fora, juros (régua própria).
- **As categorias de juros aparecem marcadas** como "não entram aqui: seguem
  a régua do déficit", em vez de parecerem coisa que se divide por aqui.
- **Na lista de lançamentos**: busca (fornecedor, documento, observação),
  marcar todos os visíveis, botões **"Não entra (0%)"** e **"Entra inteiro
  (100%)"** para os marcados, "Voltar a herdar", e cada linha diz o que
  divide de fato — **próprio** ou **herdado** da categoria — e quanto entra.
  O documento abre no Pipefy quando há cartão.
- **"O que você marcou"**: a lista de toda exceção do cenário (grupo,
  categoria, lançamento com fornecedor e documento), com valor e percentual,
  cada uma com link para o lugar onde se muda. É a trilha para quem for
  auditar.
- Selo "N exceção(ões)" no grupo e na categoria que têm lançamento marcado
  por dentro, para não passar despercebido.

Sem migração: é a mesma gravação de antes, só a leitura ficou mais rica.

## O Calendário, e o menu que quebrava na tela pequena — 23/09/2026

Ideia do dono em 22/09/2026, à noite: *"um calendário grande na tela (...)
cada dia tem um resuminho dentro do dia de valores pagos ou recebidos (...)
você bate o olho e já vê toda a evolução dia após dia. Quando clicar no dia,
ele expande com o detalhamento: fornecedor, categoria, valor — e dali abrir
o Pipefy. Dois botões para o mês anterior e o seguinte, e KPIs."*

**Como ficou** — a tela "Calendário", no menu, depois do Fluxo de Caixa:

- **A régua é a do Fluxo de Caixa**, de propósito: só o que foi pago ou
  recebido de fato, pela data em que aconteceu, com juros e multa pagos e
  sem as retenções de receita. O total do mês do calendário **fecha com a
  linha do mesmo mês no Fluxo de Caixa** — há teste com banco de verdade
  provando isso. Título em aberto não aparece: calendário é caixa, não
  compromisso. Se um dia ele quiser "o que vence", é outra visão, não esta.
- **Filtros**: os da barra lateral (projeto, obra, conta, transferências) e
  os próprios da tela, como no Analítico: busca, grupo, categoria e "mostrar"
  (tudo, só recebimentos, só pagamentos). **O ano da barra lateral não vale
  aqui** — o mês manda; senão "mês seguinte" atravessaria a virada do ano e
  acharia um calendário vazio sem explicação.
- **Cada dia é um botão** com o que entrou (verde), o que saiu (vermelho) e
  quantos lançamentos. Dia sem nada fica apagado. Ao clicar, abre uma janela
  com a lista (tipo, quem, categoria, obra, documento, conta, valor) e o
  documento vira link para o Pipefy quando há cartão. A janela pede o dia ao
  servidor **com os mesmos filtros da tela** — a soma dela fecha com o
  número do quadradinho.
- **KPIs**: recebido no mês (e o maior dia), pago no mês (e o maior dia),
  líquido, dias com movimento e quantidade de lançamentos.
- **Planilha do mês**: duas abas, o dia a dia e os lançamentos do mês.
- **Escopo**: é tela de dado como as outras — só abre para quem tem
  "Calendário" marcado no cadastro, e o detalhe do dia (outro endereço) passa
  pelo mesmo filtro de obra. Testado com usuário preso a uma obra.
- **Na tela do celular** os valores viram "7 mil" / "−1,2 mil": o valor
  inteiro não cabe no quadradinho.

**Custo**: uma consulta agrupada por dia (no máximo 31 linhas) para montar o
mês; o detalhe só é lido quando alguém clica.

**O menu de cima na tela pequena.** O dono: *"quando a tela encolhe fica
uma barra de rolagem, fica ruim de encontrar as telas"*. Agora as abas
**quebram linha** em vez de rolar, e o topo deixa de ser fixo abaixo de 900
px de largura — fixo, com três linhas de abas, comeria a tela do celular.
A data da base some do topo nessa largura (continua no rodapé das telas).

**Uma armadilha que custou uma hora**: a classe `entrada` já existia no
CSS, para a **tela de login** (ocupa a tela inteira, fundo azul-escuro).
Usada num valor do calendário, o quadradinho virou um bloco de 900 px de
altura. As classes do calendário levam o prefixo `cal-` por isso.

## Fora da análise em Parâmetros, e a prestação por obra — 22/09/2026

Duas perguntas do dono, no mesmo dia, depois de ver o cenário: *"tem a visão
de projetos, mas dá para ver por obra também?"* e *"na parte de configurações
da prestação de conta, pra eu poder eliminar projetos e/ou obras dessa
análise"*. Ele esperava a exclusão nos Parâmetros — e ela estava só dentro
de cada cenário.

1. **A lista "Fora da análise" agora vive em Parâmetros** (aba própria), na
   configuração da prestação (`fora_da_analise`, itens "obra:NOME" e
   "projeto:NOME" separados por ponto-e-vírgula — **sem migração**: é uma
   chave a mais na tabela `config`). É aplicada na base, antes de qualquer
   conta, e por isso vale para **tudo**: a Prestação de Contas, os cenários
   de rateio e todo cenário. A lista de cada cenário **soma-se** a ela — nunca
   a substitui. "Tirar o Ceará de tudo" se faz uma vez; "e se sem a obra X?"
   continua sendo coisa de um cenário só. A montagem do cenário mostra o que
   já está fora por Parâmetros, e avisa que só volta por lá.

2. **A Prestação de Contas ganhou "Resultado por obra"**, logo abaixo do
   resultado por projeto: a mesma conta obra a obra, com o projeto ao lado,
   pior primeiro, com linha de total. A planilha da prestação (botão "Baixar
   planilha") leva as duas abas — por projeto e por obra.

O que ficou de fora, de propósito: a lista geral não entra no Rateio
administrativo (a simulação antiga, tela própria) — ele tem os filtros dele.

## O Explorador ganhou o link do Pipefy — 22/09/2026

Pedido do dono: *"quero que seja adicionado o link pra acessar o Pipefy quando
pertinente"*. O link já existia na base (é o mesmo do Analítico e do Extrato,
montado a partir do número do documento); o Explorador não o mostrava. Agora o
documento vira link quando há cartão no Pipefy, com "↗", e a planilha do
Explorador ganhou a coluna.

**E os relatórios também** (*"aproveita e coloca nos relatórios os links para
acessar o Pipefy quando houver"*): na planilha, o endereço vira uma célula
**clicável** ("Abrir no Pipefy") — Analítico, Extrato, Receita de Obra e
Explorador; no PDF, a célula diz "Pipefy" em azul e o clique abre o cartão.

## O que falta

Atualizado em **14/09/2026**, no fim da sessão que caçou uma devolução de aporte
a tarde inteira.

### Esperando decisão do dono

1. **As duas regras de "foi pago" divergem** — a carga considera pago o título
   cujo status diga *pago/recebido/conciliado* **ou** cuja baixa do OMIE diga
   liquidado; as telas olham só o texto do status. **Medido na base real:
   R$ 96.750,00 em 2 títulos** estão marcados como pagos e não são contados por
   tela nenhuma. A conferência vive em Configurações e não altera nada.
   O conserto é as telas lerem `situacao_vencimento = 'Quitado'`, que é a
   decisão que a própria carga já grava — e a regra passa a existir num lugar
   só. **Falta o dono dizer se pode**, porque números do DRE e da Visão Geral
   vão subir (no máximo esse valor).

   Para orientar o conserto, falta ler na conferência **quais palavras de
   situação** estão escapando e **em quais categorias** — está tudo na tela.

2. **Os valores do bloco Aportes e Dividendos do DRE parecem errados**, disse o
   dono em 13/09. Não foi atacado: ele não chegou a dizer **quais** números
   estão errados nem o que esperava ver. Sem isso só dá para levantar hipótese,
   e a tarde de 13/09 mostrou o custo disso. **É o item mais importante da
   lista** — este bloco nunca foi conferido contra dado real.

### Buracos conhecidos, sem conserto ainda

3. **Lançamento de conta corrente não entra no painel.** A carga lê Contas a
   Pagar e a Receber; movimento sem título é descartado na gravação
   (`gravar_movimentos` conta os ignorados e segue). Na conferência do dono, os
   lançamentos de "Débito em Conta Corrente" de R$ 3,00 e R$ 1,00 não aparecem
   por isso — e não vão aparecer por recálculo nenhum. Trazê-los exige decidir
   antes se entram nas contas: somá-los junto com o título que eles quitam
   contaria o mesmo dinheiro duas vezes. Provavelmente devem aparecer no
   Explorador e ficar **fora** dos totais.

4. **As colunas de dinheiro do espelho são `REAL` e perdem centavos acima de
   R$ 131.072.** Ver a seção própria acima. Consertar é migração mais carga
   completa (horas). O dono decidiu em 14/09 **não fazer agora** — o dano
   prático era a busca, e ele está resolvido.

5. **O ensaio da alteração no OMIE depende de a API estar no ar.** A recusa por
   rateio sai do espelho do próprio painel e não precisaria de rede, mas hoje só
   aparece depois que o painel fala com o OMIE.

### Conferência com dado real (só o dono consegue)

6. **A primeira escrita no OMIE nunca aconteceu.** Protocolo: ensaio → **um**
   título conferido dentro do OMIE com os olhos → só então lote.
7. **O PDF contra a planilha** do mesmo recorte.
8. **Os cenários de rateio**, contra a intuição de quem conhece as obras.

### Fora desta área

9. **O mesmo defeito da senha com acento existe no Análise de SPs**
   (`analisesps/auth.py` 120 e 271, `analisesps/web.py` 628). Avisado em 04/09.

### Melhorias possíveis, nenhuma urgente

10. **O que sobrou de lentidão está no banco, não no código.** Medido pelo dono
    em 04/09: 478 ms de tela, 443 deles no banco — 93%.

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
