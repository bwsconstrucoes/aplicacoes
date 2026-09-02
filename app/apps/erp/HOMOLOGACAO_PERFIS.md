# Homologação dos perfis — o que testar navegando

Este roteiro fecha o único item de segurança que **teste automatizado não
cobre**: o caminho de quem TEM acesso. A suíte prova bem o lado da recusa;
provar que a pessoa certa consegue trabalhar exige banco de verdade e olho na
tela.

Ele foi derivado da tabela de permissões do código (`core/auth/permissoes.py`),
não da memória de ninguém. Se o código mudar e este documento não, ele mente —
gere de novo pelo mesmo caminho.

---

## Antes de começar

1. Aplicar as migrações pendentes: **Configurações → "Aplicar atualizações do
   banco"**, logado como ADMIN. A migração `029` cria o campo de alcance do
   operador.
2. Criar os operadores de teste, presos a **uma obra só**:

   ```
   python app/apps/erp/scripts/seed_usuarios_teste.py CODIGO_DA_OBRA
   ```

   O script sorteia as senhas e as imprime **uma vez**. Anote.
3. Ter à mão o número de um título de **outra** obra — é com ele que se testa o
   escopo. Sem um segundo registro fora do alcance, o teste não prova nada.

Use uma janela anônima por perfil, ou saia entre um e outro: a sessão fica
guardada no navegador.

---

## Como ler o resultado

- **Abre** — a tela carrega e mostra dados.
- **Recusa** — a tela **não** abre. Quem não tem a ação leva "sem permissão".
- **Não encontrado** — o registro existe, mas está fora do alcance da pessoa.
  A resposta é **"não encontrado"**, igual a um número que não existe. **Isso é
  o certo, não é bug.** Dizer "sem permissão" confirmaria que o registro
  existe, e varrer os números mapearia o sistema inteiro sem abrir nada.

Se em algum ponto aparecer **erro 500, tela branca ou "sem permissão" no lugar
de "não encontrado"**, anote a tela e o que você fez. É defeito.

---

## 1. Administrativo de obra

`teste.administrativo@bws.local` — preso a uma obra, começa vendo **só o que
ele mesmo lançou**.

**Tem de abrir:** Títulos · Início · Lançar · Confirmar · Empreitas · Locações ·
Obras · Prestação · Despesas com colaborador · Colaboradores

**Tem de recusar:** Pagamentos · Conciliação · Receber · Relatórios · Importar ·
Configurações

**Escopo — o teste que importa:**

| O que fazer | O que tem de acontecer |
|---|---|
| Abrir a lista de Títulos | Só aparecem títulos que **ele** lançou |
| Lançar um título e voltar à lista | O novo aparece |
| Digitar na barra do navegador o endereço de um título de outra obra | **"Não encontrado"** |
| Abrir um título dele e olhar os dados de pagamento | Chave Pix, banco e código de barras **não aparecem** |
| Tentar baixar um anexo de título alheio pelo endereço direto | **"Não encontrado"** |

**Depois, ampliar o alcance** — é a novidade desta entrega:

1. Como ADMIN: Configurações → Operadores → *TESTE — Administrativo de obra* →
   **"O que esta pessoa enxerga"** → **"Tudo das obras designadas"** → Salvar.
2. Entrar de novo como ele.

| O que fazer | O que tem de acontecer |
|---|---|
| Abrir a lista de Títulos | Agora aparecem também os títulos **de outras pessoas** rateados na obra dele |
| Abrir um título da obra dele lançado por outro | Abre |
| Abrir um título de **outra** obra | Continua **"não encontrado"** |
| Olhar os dados de pagamento | Continuam ocultos — alcance é uma coisa, alçada é outra |

> Se a lista mudar mas o detalhe não (ou o contrário), pare e avise: listagem e
> detalhe passam pelo mesmo filtro, e divergir seria defeito grave.

---

## 2. Supervisor de obra

`teste.supervisor@bws.local` — preso à mesma obra.

**Tem de abrir:** Títulos · Início · Lançar · Confirmar · Empreitas · Locações ·
Obras · Prestação · Despesas com colaborador · Colaboradores · **Relatórios**

**Tem de recusar:** Pagamentos · Conciliação · Receber · Importar ·
Configurações

| O que fazer | O que tem de acontecer |
|---|---|
| Abrir a lista de Títulos | Aparece o que **ele** lançou **e** o que está rateado na obra dele, de quem quer que seja |
| Abrir um título de outra obra pelo endereço | **"Não encontrado"** |
| Abrir um título da obra dele | Abre, **com** os dados de pagamento — o supervisor os enxerga |
| Avalizar um título aguardando aval | Consegue |
| Tentar aprovar (etapa seguinte) | Recusa — avalizar não é aprovar |
| Abrir a tela da obra dele | Abre |
| Abrir a tela de **outra** obra pelo endereço | **"Não encontrado"** |

---

## 3. Administrativo financeiro

`teste.financeiro@bws.local` — sem obra: opera o sistema inteiro.

**Tem de abrir:** tudo — Títulos, Início, Lançar, Confirmar, **Pagamentos**,
Empreitas, Locações, Obras, Prestação, **Conciliação**, **Receber**,
**Relatórios**, **Importar**, Despesas com colaborador, Colaboradores

**Tem de recusar:** **Configurações** — é a única. Configurar é só do
administrador.

| O que fazer | O que tem de acontecer |
|---|---|
| Abrir a lista de Títulos | Aparece **tudo**, de todas as obras |
| Abrir qualquer título | Abre, com dados de pagamento |
| Registrar um pagamento | Consegue |
| Abrir a Conciliação | Abre |
| Tentar **avalizar** um título | **Recusa** — o aval é de quem responde pela obra, não de quem paga. É a separação que evita a mesma pessoa pedir e liberar |
| Tentar editar um colaborador | **Recusa** — cadastro de pessoal é do DP |
| Abrir Configurações | **Recusa** |

---

## 4. Departamento pessoal

`teste.dp@bws.local` — sem obra.

**Tem de abrir:** Títulos · Início · Lançar · Confirmar · Empreitas · Locações ·
Obras · Prestação · **Despesas com colaborador** · **Colaboradores**

**Tem de recusar:** Pagamentos · Conciliação · Receber · Relatórios · Importar ·
Configurações

| O que fazer | O que tem de acontecer |
|---|---|
| Abrir Despesas com colaborador | Abre e mostra **todas** — é o trabalho dele |
| Aprovar uma despesa que está "aguardando DP" | Consegue |
| Aprovar uma que ainda está "aguardando supervisor" | Recusa — a ordem da cadeia é obrigatória |
| Editar o cadastro de um colaborador | Consegue — só ele, o diretor e o administrador podem |
| Abrir a lista de Títulos | Mostra **só o que ele mesmo lançou** — ver a observação abaixo |
| Olhar dados de pagamento | Ocultos |

> **Ponto a decidir depois do teste.** Hoje a lista de Títulos do DP mostra só
> os lançamentos dele. A tela de Despesas com colaborador — que é onde ele
> realmente trabalha — mostra tudo. Se na prática ele precisar enxergar os
> títulos das despesas que revisa, isso é decisão de negócio, não defeito.
> Anote e me diga.

---

## O que fazer com o resultado

- **Tudo conforme:** marque o item de homologação no `ROTEIRO.md` e apague os
  operadores de teste (Configurações → Operadores → Inativo).
- **Algo abriu que não devia:** é o caso grave. Anote a tela, o perfil e o
  endereço, e trate como urgente.
- **Algo recusou que devia abrir:** atrapalha o trabalho, mas não vaza nada.
  Anote e ajuste a tabela de permissões.

Os operadores de teste **não devem sobreviver à homologação**. Senha provisória
que fica viva é senha esquecida.
