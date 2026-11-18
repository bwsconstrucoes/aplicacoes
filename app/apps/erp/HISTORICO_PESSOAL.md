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

### 18/11/2026 — Liberar o acesso passou a CONFIGURAR o acesso

**O pedido, com as palavras dele:** *"o administrador vai liberar a pessoa, mas
tem que definir o acesso dele, o perfil, as obras, etc e tal. Eu acho que assim
fecha essa parte de liberação e forma um ciclo perfeito."*

**O que mudou para quem usa:** ao liberar alguém — tanto pela fila de pedidos
quanto pela ficha da pessoa — o sistema pergunta **que acesso ela terá** e
**quais obras ela enxerga** (várias, ou todas, inclusive as futuras). Antes a
fila criava sempre "administrativo de obra" preso à obra do cadastro, e a ficha
criava "consulta"; quem precisasse de outra coisa tinha de ir à tela de
operadores logo em seguida.

**Continua sendo um clique no caso comum:** os campos já vêm preenchidos com o
que o sistema faria sozinho. Quem não quer mudar nada só confirma.

**O que ficou travado, de propósito:** quem resolve a fila mas **não** gere
operadores (o DP) não vê esses campos, e o servidor nem manda as opções. Sem
isso, o DP poderia mandar "perfil: ADMIN" na mão e criar um administrador do
sistema por uma fila de acesso de obra.

**Duas decisões de tela que vieram junto:**

1. **Uma lista só de acesso.** Os perfis cadastrados repetem, com os mesmos
   nomes, os cargos antigos — oferecer os dois dava 23 opções com
   "Administrador" aparecendo duas vezes. Vale o perfil cadastrado; o cargo
   antigo só aparece se ainda não houver perfil nenhum. É a regra "um
   cadastro, uma porta" aplicada aqui.
2. **Lista de obras vazia é escolha, não esquecimento.** Quem desmarcar tudo
   libera um acesso sem obra (o caso do escritório). O servidor distingue
   "ninguém disse nada" de "disseram nenhuma".

**Mexeu em arquivo compartilhado** (importa para quem roda outro chat do ERP):
`routes.py` e `templates/erp_base.html` — este último ganhou um tipo de campo
novo no diálogo, uma lista de caixas de marcar, que qualquer tela pode usar.

---

## Incidentes

(vazio)
