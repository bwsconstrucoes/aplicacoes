# HISTÓRICO — ERP · Pessoal e DP

Memória **desta área**. O chat é descartável; isto não.

⚑ **ESTE ARQUIVO NASCEU EM 18/11/2026**, quando o dono dividiu o chat do ERP em
quatro. O pedido dele, com as palavras dele:

> *"Talvez assim separar mais: a parte de pessoal eu trato no chat, a parte de
> suprimento eu trato no outro chat, porque assim uma coisa que eu vejo numa
> área eu jogo lá, e a outra coisa que eu vejo na outra área eu jogo no outro
> chat. Eu acho que tem como evoluir mais rápido dessa forma."*

⚠️ **O PASSADO DESTA ÁREA NÃO ESTÁ AQUI — está em `HISTORICO.md`**, que até
18/11/2026 guardava o ERP inteiro (12 mil linhas). Ele **não foi quebrado**, de
propósito: repartir doze mil linhas de memória por adivinhação perderia
contexto, e perder memória é o pior estrago possível neste repositório. Então:

- **procurar o passado** → `HISTORICO.md`, buscando por "colaborador", "DP", "etapa", "contabilidade", "portal", "agente"
- **escrever o novo** → aqui

**Leitura desta área, além do `HISTORICO.md`:** `CADASTRO_DE_PESSOAS.md`,
`MANUAL_DP.md`, `AGENTE_DP.md`, `COMO_O_AGENTE_TRABALHA.md`,
`PLACAR_DO_DP.md`, `DE_PARA_PIPEFY_DP.md`, `DESLIGAR_A_PLANILHA.md`,
`PERGUNTAS.md`.

---

## Onde o trabalho está

Cadastro de pessoas, as etapas do DP (a fila de trabalho dele),
afastamentos e retorno, solicitações que a obra faz ao DP, pedidos à
contabilidade, o agente de DP, o portal da obra, cargos e convenções.

**É a área onde esteve quase todo o trabalho de outubro e novembro de
2026**, então o `HISTORICO.md` antigo é, na prática, o histórico dela.

---

## ⚑ PENDENTE AGORA — leia isto antes de qualquer coisa

**Falta publicar (18/11/2026).** Está tudo no ramo `claude/oi-vjvrn8`, testado,
e **espera o "pode" dele**:

- migração **112** (as listas de conferência do DP, configuráveis pela tela);
- migração **113** (o primeiro acesso pelo portal: códigos e a marca de
  identidade confirmada);
- a liberação de acesso que também configura perfil e obras (sem migração).

⚠️ **São duas atualizações de banco.** Ao juntar, ele tem de apertar "Aplicar
atualizações do banco" em Configurações **no mesmo momento**.

**Ainda sem resposta dele:**

- o endereço de produção, para entrar no lugar de `SEU-ENDERECO` no PDF de
  orientação e na mensagem de WhatsApp (os dois estão prontos);
- se o diário de obra deve esconder também obra cuja **fase** está encerrada
  (hoje ele filtra só por `status <> 'ENCERRADA'` e **ignora a fase**, então
  obra marcada "Concluída" continua aparecendo na lista do diário).

**Aprovado por ele e ainda não construído:**

- caixa de entrada de e-mail do Suprimento (a mesma conta que envia, uma por
  empresa);
- campo do resultado do ASO ("apto" / "apto com restrição");
- a ponte ponto → **Solicitações DP** no afastamento. Correção dele, que
  manda: aprovar afastamento no ponto **não** leva o cadastro para Afastado;
  tem de abrir uma solicitação ao DP, porque a informação vai para a
  contabilidade.

O que estava pendente antes da divisão dos chats está em `HISTORICO.md`, na
seção "PENDENTE AGORA".

⚠️ **Perguntar ao dono** se o que está escrito lá já aconteceu — o estado do
mundo muda entre uma sessão e outra, e ele é a única fonte disso.

---

## Decisões desta área

### 18/11/2026 — Liberar o acesso leva direto para configurar o acesso

**O pedido, com as palavras dele:** *"no ato da liberação já abrir a tela de
configurações do cadastro da pessoa. No caso é o administrador que vai ter
poderes para isso. Aí lá será designado o perfil dela, obras de acesso e etc.
E pronto."*

**O que acontece agora:** quem administra operadores confirma a liberação e
**cai direto em Configurações › Usuários, com o cadastro da pessoa aberto** —
perfil de acesso, perfis acrescentados, obras, cargo, fundo fixo e situação.
O resultado da liberação (inclusive o link do convite, quando o e-mail não
sai) aparece **dentro** dessa janela. Vale para as duas portas: a fila de
pedidos do portal e a ficha da pessoa.

Quem resolve a fila mas **não** administra operadores (o DP) não sai da tela
de Pessoal: libera, vê o aviso ali mesmo, e o acesso nasce com o padrão
seguro — administrativo de obra, preso à obra do cadastro.

**⚠️ O CAMINHO ERRADO QUE FOI DESFEITO NO MESMO DIA, e o motivo vale guardar:**
a primeira entrega foi uma **janelinha** na tela de Pessoal perguntando perfil
e obras. Estava certa no pedido anterior dele (*"tem que definir o acesso dele,
o perfil, as obras"*) e errada no desenho: virou **uma segunda tela com metade
dos campos** da tela de operadores que já existe. É exatamente a terceira porta
de que ele reclamou em 17/11 (*"era só para equalizar e complementar os
dados"*) — e um dia as duas divergiriam.

**A regra que isso confirma:** quando a resposta a "onde isso se configura?" já
existe numa tela, **leve a pessoa até ela**. Não recrie um pedaço dela onde a
ação começou.

**O que veio junto, e fica:**

1. **A resposta da fila emagreceu.** Ela mandava a lista de perfis e de cargos
   para desenhar aquela janelinha — uma consulta por abertura da tela de
   Pessoal, para uma lista que acabou morando em outro lugar. Agora manda só
   se a pessoa administra o acesso ou não.
2. **A trava do DP continua no servidor.** Nenhuma tela manda perfil ou obra,
   mas a rota segue recusando esses campos de quem não administra operadores —
   quem souber montar o pedido na mão não passa. Há teste.
3. **O recado não viaja escrito na URL.** Só uma marca conhecida e o link; o
   texto é montado na tela de destino, porque o aviso escreve HTML na página
   de quem administra o sistema.

**Mexeu em arquivo compartilhado** (importa para quem roda outro chat do ERP):
`routes.py`, `templates/erp_base.html` (ganhou um tipo de campo novo no
diálogo, uma lista de caixas de marcar — nasceu para a janelinha desfeita, mas
é genérico e qualquer tela pode usar) e `templates/erp_config.html`.

---

## Incidentes

(vazio)
