# Mensageria — decisões, estado e o que falta

Este arquivo existe para uma sessão nova pegar o trabalho sem repetir o que já
foi lido e discutido. O `README.md` ao lado explica **como** o chatbot, o bot
do Telegram, o gateway de WhatsApp e o notificador funcionam. Este aqui explica
**em que pé estão**, o que o dono quer deles e o que ainda depende dele.

---

## Como esta memória nasceu — 05/10/2026

O dono abriu um chat para estas aplicações e pediu duas coisas:

1. **Trazê-las para o mesmo jeito de trabalhar das outras áreas** — um chat
   por área, memória no repositório, evolução pelo Claude Code.
2. **Uma análise, com cautela, sobre juntar as três numa só** ("dentro do
   chatbot ter o gateway do Telegram e o gateway do WhatsApp"), sem quebrar
   nada.

E deixou claro o que quer no fim: **usar o WhatsApp para algumas coisas e
manter no Telegram o chatbot e os envios que já existem lá.**

Os dois arquivos foram escritos **a partir do código, lido de ponta a ponta**
(cerca de 3.300 linhas nas três pastas, mais o notificador e os seis módulos
que falam com o WhatsApp). O que está no mundo fora do repositório — o que há
nas variáveis do Render, se a Evolution foi instalada, se o Z-API continua
contratado, para onde o Make aponta — **não foi possível conferir daqui**, e
virou pergunta (abaixo).

**Antes desta sessão a área não tinha memória nenhuma:** o `CONTEXTO.md` dizia
"ainda não documentado aqui" para o `telegram` desde julho, e "detalhar quando
precisar mexer" para o `chatbot`. O histórico do git também não ajuda: as três
pastas entraram no repositório num commit só (29/09/2026), então **não há
registro de quando cada coisa foi escrita nem por quê** além dos comentários
no próprio código.

---

## Onde o trabalho está

**Nada de código mudou nesta sessão.** O que existe foi lido, posto para subir
(o monorepo sobe com os 19 blueprints e as rotas de saúde respondem) e
escrito aqui. O que o dono acredita sobre as aplicações e o que o código diz
diverge em três pontos, e vale corrigir a imagem antes de decidir:

| O dono lembra | O que o código mostra |
|---|---|
| "O chatbot usa o Telegram" | O **chatbot é de WhatsApp** (Z-API). O Telegram tem uma **cópia** do fluxo de contracheque dentro do `telegram_bot.py`, que importa os módulos do chatbot e troca só o envio |
| "Tem algo em algum canto que desativa o WhatsApp, e acho que está desativado" | O botão é a variável **`NOTIFICAR_WHATSAPP=0`** no Render. Ela desliga o WhatsApp no notificador, no ValidaSP, no BaixaBradesco, na emissão de NFS-e e no ponto. **Não** desliga as respostas do chatbot nem o aviso "cadastre-se no Telegram" — esses dois não olham a variável. Se está em `0` hoje, só o Render diz |
| "São três aplicações separadas que trabalham juntas" | São **quatro peças**, e a quarta é a mais usada: o `notificador.py`, importado por seis módulos. E o gateway **não trabalha com ninguém** por enquanto: nenhum módulo Python o chama, e ele só tem efeito se a Evolution estiver no ar e o Make apontar para ele |

### O que está pendente AGORA

**Decisões e dados que só o dono tem** — enquanto não vierem, o trabalho de
código desta área está parado por escolha, não por falta do que fazer:

1. **O Z-API continua contratado e ativo?** Todo envio de WhatsApp do
   repositório passa por ele. Se foi cancelado, "usar o WhatsApp" começa por
   escolher outro provedor (ver análise).
2. **Qual destas duas variáveis existe no Render: `ZAPI_API_TOKEN`,
   `ZAPI_INSTANCE_TOKEN`, ou as duas?** São dois nomes para o mesmo token.
   O chatbot, o ValidaSP, o BaixaBradesco e a emissão de NFS-e leem a
   primeira; o notificador, o ponto e o aviso do Telegram leem a segunda.
   **Se só uma existir, metade dos envios de WhatsApp falha em silêncio** —
   e o ponto (QR Code, PIN) está nessa metade. A resposta é olhar a tela de
   variáveis do Render; o nome basta, **o valor não entra no chat**.
3. **`NOTIFICAR_WHATSAPP` está em `0` hoje?** Mesmo lugar.
4. **A Evolution API foi instalada como segundo serviço no Render?**
   `WHATSAPP_GATEWAY_INSTANCES` foi preenchida? Algum cenário do Make já
   aponta para `aplicacoes.bwsconstrucoes.com.br/instances/...`? Em julho isso
   estava "pendente"; não há registro posterior.
5. **O webhook do Z-API ainda aponta para `/api/chatbot/webhook`?** Ou seja:
   alguém ainda pede contracheque pelo WhatsApp, ou isso já migrou de vez para
   o Telegram? Decide se o chatbot de WhatsApp é algo a manter ou a aposentar.
6. **Quais coisas vão para o WhatsApp?** Hoje a escolha é por módulo, no
   código (tabela do `README.md`), e o botão `NOTIFICAR_WHATSAPP` é **um só
   para tudo**. "Algumas coisas pelo WhatsApp" exige dizer quais: QR e PIN do
   ponto (já são só WhatsApp), aviso de título pago (hoje só Telegram),
   falhas da baixa, nota emitida, validação de SP…

---

## A análise: juntar as três numa só?

Pedida pelo dono em 05/10/2026. Resposta curta: **juntar as pastas é possível
e não exige mudar nenhum endereço externo — mas é a mudança que menos resolve
o que ele quer.** O que desorganiza a área não é haver três pastas; é haver
**seis códigos de envio de WhatsApp**, **dois nomes para o mesmo token**, **um
botão só** para ligar e desligar tudo, e **um gateway sem ninguém do lado de
dentro usando**.

### O que um "juntar" físico mudaria, e o que não mudaria

- **Endereços externos não precisam mudar.** O Z-API, o BotFather, o Make e a
  Evolution apontam para rotas (`/api/chatbot/webhook`, `/telegram/webhook`,
  `/telegram/enviar`, `/instances/...`). Um blueprint pode mudar de pasta e
  manter as rotas iguais. Então mover código **não** obriga a reconfigurar
  nada fora — a preocupação do dono com "alterar redirecionamentos" não se
  confirma, desde que as rotas sejam preservadas à risca.
- **O que quebra ao mover são as importações internas.** O notificador importa
  funções privadas do `telegram_bot` (com underline, isto é, não feitas para
  serem importadas); o Telegram importa cinco módulos do chatbot; seis
  módulos importam o notificador; e a emissão de NFS-e chama `/telegram/enviar`
  por HTTP com a URL **gravada no código**. É tudo rastreável e a suíte não
  cobre nada disso — um nome errado só aparece em produção, na hora de
  enviar.
- **O ganho é só de arrumação.** Uma pasta em vez de três. Nenhum
  comportamento muda, nenhuma pergunta do dono é respondida.

### O que realmente organiza (recomendação)

A separação que faz sentido não é por aplicativo, é por **papel**:

| Papel | Hoje | Como deveria ser |
|---|---|---|
| **Canal** — como se fala com o WhatsApp e com o Telegram | WhatsApp em 6 lugares; Telegram em 1 (bem feito) | **Um** lugar para cada canal. Todo mundo chama "mande isto para este telefone" e não sabe se por trás é Z-API, Evolution ou outro |
| **Conversa** — o que o bot diz e pergunta | O "cérebro" no `chatbot/`, uma cópia do fluxo dentro do `telegram_bot.py` | Um fluxo só, que recebe "chegou esta mensagem de fulano por este canal" e responde pelo mesmo canal |
| **Avisos** — quem recebe o quê e por onde | Decidido no código de cada módulo + um botão global | Uma regra por **tipo de aviso** (QR do ponto, título pago, nota emitida…) dizendo o canal e se está ligado — e aí sim "WhatsApp para algumas coisas" vira configuração, não publicação |

Isso se faz **em fases, cada uma segura sozinha, sem mexer em pasta**:

1. **Primeiro, as respostas do dono** (lista acima). Sem saber o que está no
   Render e se a Evolution existe, qualquer mudança no envio é chute.
2. **Um envio de WhatsApp só**, dentro do `notificador.py`, aceitando os dois
   nomes de token e respeitando o botão; os seis módulos passam a chamar ele.
   É mudança de código em seis áreas — pequena em cada uma, mas **atravessa
   áreas** e precisa do teste de cada módulo. Ganha teste próprio (hoje não há
   nenhum). ⚠️ Efeito colateral a avisar: se hoje só um dos nomes de token
   existe no Render, metade dos envios está falhando em silêncio; aceitar os
   dois nomes **faz esses envios passarem a sair**. É bom — mas é mensagem
   chegando no celular de colaborador que hoje não chega, e o dono precisa
   saber antes.
3. **Botão por tipo de aviso** em vez de um global — o que o dono pediu de
   fato.
4. **Decidir o provedor de WhatsApp.** Se a Evolution está no ar, o gateway
   ganha um segundo cliente (o próprio notificador, além do Make) e o Z-API
   pode ser aposentado como estava planejado. Se não está, o gateway é código
   parado e a decisão é instalá-la ou removê-lo.
5. **Só no fim, e só se o dono quiser a arrumação:** mover as três pastas para
   uma (`app/apps/mensageria/`, esta mesma, que por enquanto só guarda a
   memória), mantendo todas as rotas. A essa altura as importações já
   estariam concentradas, e mover seria barato.

### O que ficou de fora de propósito

- Não se mexeu em código, nem nos consertos pequenos e seguros (aceitar os
  dois nomes de token, fazer o chatbot respeitar o botão): cada um deles **muda
  o que chega no celular de alguém**, e isso depende das respostas acima.
- Não se tentou falar com Z-API, Telegram, Evolution, Google ou Dropbox: a
  sessão não tem credenciais, e não deve ter.

---

## Decisões registradas

- **05/10/2026 — A área ganhou memória antes de ganhar código.** Mesma regra
  da emissão de NFS-e (21/09/2026): área sem `README.md`/`HISTORICO.md` não
  entra na tabela do `CLAUDE.md` apontando para arquivos que não existem. A
  memória vive numa pasta própria, `app/apps/mensageria/`, porque cobre
  quatro peças em quatro lugares — e porque, se um dia as pastas forem
  unidas, é para cá que vêm.
- **05/10/2026 — Recomendação: não juntar as pastas agora.** Motivos na
  análise acima. A decisão é do dono.

---

## Incidentes conhecidos (do próprio código — sem data no `CONTEXTO.md`)

- **16/07/2026 — preview de link do Telegram disparava ações.** O robô do
  Telegram fazia um GET na URL para montar o preview e isso **executava links
  de ação de um clique** (validação/anuência de SP) antes de a pessoa clicar.
  Conserto: todo envio vai com preview desligado (`_tg_enviar`). Está só no
  comentário do código; registrado aqui para não se perder.
- **Loop de eco no bloqueio do chatbot.** Telefone bloqueado por CPF errado
  recebia resposta a cada nova mensagem, inclusive a callbacks de status do
  Z-API reentrando no webhook — gastava cota e girava em círculo. Conserto:
  bloqueado não recebe resposta nenhuma. Sem data.
- **Token do Dropbox guardado para sempre.** O chatbot usava um cache sem
  validade e, depois de ~4 h de processo de pé, todo download falhava com
  `401 expired_access_token`. Conserto: cache com expiração. Sem data.
- **Make quebrava JSON com aspas** na mensagem do `/telegram/enviar`.
  Conserto: a rota aceita formulário e upload, além de JSON, e destrava `\n`
  literal. Sem data.
