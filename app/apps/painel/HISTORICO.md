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
